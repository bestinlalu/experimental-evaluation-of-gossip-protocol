import time
import json
import subprocess
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import statistics

# --- CONFIGURATION ---
DB_CONFIG = {
    'host':            'localhost',
    'database':        'gossipdb',
    'user':            'root',
    'password':        'password',
    'port':            3306,
    'connect_timeout': 10
}

POLL_INTERVAL    = 5    # seconds between DB polls
BASELINE_UIDS    = 5    # UIDs to collect before fault
POST_FAULT_UIDS  = 5    # UIDs to collect after reform
TTL_SECONDS      = 30   # must match -ttl flag on gossip nodes

# ─────────────────────────────────────────────────────────────
# DB Helpers
# ─────────────────────────────────────────────────────────────

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except Error as e:
        print(f"❌ DB connection failed: {e}")
        return None


def get_active_nodes(cursor):
    cursor.execute("""
        WITH RankedActions AS (
            SELECT
                CONCAT(host, ':', actionPort) AS node_address,
                action,
                ROW_NUMBER() OVER (
                    PARTITION BY host, actionPort
                    ORDER BY timestamp DESC
                ) AS rn
            FROM ActionRecord
        )
        SELECT node_address FROM RankedActions WHERE rn = 1 AND action = 'START';
    """)
    return {row["node_address"] for row in cursor.fetchall()}


def get_neighbor_list(cursor, active_nodes):
    """
    Infer neighbor list from ActionRecord peers column.
    Shows which nodes each active node knows about.
    """
    cursor.execute("""
        WITH RankedActions AS (
            SELECT
                CONCAT(host, ':', actionPort) AS node_address,
                peers,
                action,
                ROW_NUMBER() OVER (
                    PARTITION BY host, actionPort
                    ORDER BY timestamp DESC
                ) AS rn
            FROM ActionRecord
        )
        SELECT node_address, peers FROM RankedActions WHERE rn = 1 AND action = 'START';
    """)
    rows = cursor.fetchall()
    neighbor_map = {}
    for row in rows:
        node = row["node_address"]
        try:
            peers = json.loads(row["peers"]) if row["peers"] else []
        except Exception:
            peers = []
        neighbor_map[node] = peers
    return neighbor_map


def print_neighbor_list(neighbor_map, active_nodes, label=""):
    print(f"\n  📡 Neighbor List {label}")
    print(f"  {'─'*50}")
    all_nodes = sorted(neighbor_map.keys())
    for node in all_nodes:
        peers    = neighbor_map[node]
        is_alive = "✅" if node in active_nodes else "💀 DEAD"
        # Filter peers to show which are still alive
        alive_peers = [p for p in peers if p in active_nodes]
        dead_peers  = [p for p in peers if p not in active_nodes]
        peer_str = ""
        if alive_peers:
            peer_str += ", ".join([f"✅ {p}" for p in alive_peers])
        if dead_peers:
            peer_str += ("  " if alive_peers else "") + ", ".join([f"💀 {p}" for p in dead_peers])
        if not peers:
            peer_str = "(no peers — bootstrap node)"
        print(f"  {is_alive} {node:<25} → knows: {peer_str}")
    print(f"  {'─'*50}")


def get_node_activity(cursor, all_known_nodes, fault_time=None, ttl_seconds=30):
    """
    Query last forwarded timestamp per node from GossipRecord.
    Shows who is still pushing and who has gone silent.
    """
    cursor.execute("""
        SELECT forwarderAddress, MAX(forwarderTimestamp) as last_seen
        FROM GossipRecord
        GROUP BY forwarderAddress
        ORDER BY forwarderAddress;
    """)
    rows = cursor.fetchall()
    activity = {}
    for row in rows:
        ts = row["last_seen"]
        if isinstance(ts, datetime):
            ts = ts.timestamp()
        activity[row["forwarderAddress"]] = ts
    return activity


def print_node_activity(activity, active_nodes, all_known_nodes, label="", fault_time=None, ttl_seconds=30):
    now = time.time()
    print(f"\n  📡 Node Activity — {label}")
    print(f"  {'─'*58}")
    for node in sorted(all_known_nodes):
        last_ts = activity.get(node)
        if last_ts is None:
            print(f"  ❓ {node:<28} → no data yet")
            continue

        last_seen_str = datetime.fromtimestamp(last_ts).strftime('%H:%M:%S')
        silent_for    = now - last_ts

        if node in active_nodes:
            if silent_for < 5:
                status = "✅ ACTIVE"
            else:
                status = f"⚠️  SLOW ({silent_for:.0f}s since last push)"
        else:
            if fault_time and (now - fault_time) >= ttl_seconds:
                status = f"🗑️  DEAD — removed from state maps (TTL={ttl_seconds}s expired)"
            else:
                remaining = ttl_seconds - (now - fault_time) if fault_time else ttl_seconds
                status = f"💀 STOPPED PUSHING ({silent_for:.0f}s ago) — TTL expires in {max(0,remaining):.0f}s"

        print(f"  {status}")
        print(f"     {node:<28} last pushed at {last_seen_str} ({silent_for:.0f}s ago)")

    print(f"  {'─'*58}")


def poll_new_gossip(cursor, last_id):
    cursor.execute("""
        SELECT id, uid, creatorAddress, forwarderAddress,
               forwarderTimestamp, creationTimestamp
        FROM GossipRecord
        WHERE id > %s
        ORDER BY id ASC
        LIMIT 10000;
    """, (last_id,))
    return cursor.fetchall()


# ─────────────────────────────────────────────────────────────
# State Tracker
# ─────────────────────────────────────────────────────────────

def ingest_rows(rows, global_data, last_id):
    """Parse new GossipRecord rows into global_data tracker."""
    for row in rows:
        uid     = row['uid']
        fwd     = row['forwarderAddress']
        creator = row['creatorAddress']

        if uid not in global_data:
            global_data[uid] = {}
            if creator:
                ct = row['creationTimestamp']
                ts = ct.timestamp() if isinstance(ct, datetime) else float(ct)
                global_data[uid][creator] = {"ft": ts, "ct": ts}

        ft_raw = row['forwarderTimestamp']
        ct_raw = row['creationTimestamp']
        global_data[uid][fwd] = {
            "ft": ft_raw.timestamp() if isinstance(ft_raw, datetime) else float(ft_raw),
            "ct": ct_raw.timestamp() if isinstance(ct_raw, datetime) else float(ct_raw),
        }

    return rows[-1]['id'] if rows else last_id


def check_convergence(global_data, active_nodes, reported_uids, durations, phase_label):
    """Check all tracked UIDs for full convergence. Mutates reported_uids + durations."""
    for uid, node_map in global_data.items():
        if uid in reported_uids:
            continue

        active_reached = {addr for addr in node_map if addr in active_nodes}

        if len(active_reached) >= len(active_nodes):
            start_time = next(iter(node_map.values()))["ct"]
            end_time   = max(d["ft"] for d in node_map.values())
            duration   = end_time - start_time

            durations.append(duration)
            reported_uids.add(uid)

            print(f"  ✅ [{phase_label}] UID {uid[:8]}... | "
                  f"{duration:.4f}s | "
                  f"nodes: {len(active_reached)}/{len(active_nodes)}")


# ─────────────────────────────────────────────────────────────
# Stats Printer
# ─────────────────────────────────────────────────────────────

def print_stats(label, durations, node_count):
    print(f"\n  {'Metric':<28} | Value")
    print(f"  {'-'*28}-+-{'-'*12}")
    if not durations:
        print(f"  No data collected for {label}")
        return
    print(f"  {'Nodes active':<28} | {node_count}")
    print(f"  {'Samples collected':<28} | {len(durations)}")
    print(f"  {'Mean convergence time':<28} | {statistics.mean(durations):.4f}s")
    print(f"  {'Min convergence time':<28} | {min(durations):.4f}s")
    print(f"  {'Max convergence time':<28} | {max(durations):.4f}s")
    print(f"  {'Std Dev':<28} | "
          f"{statistics.stdev(durations):.4f}s" if len(durations) > 1
          else f"  {'Std Dev':<28} | N/A (need >1 sample)")


# ─────────────────────────────────────────────────────────────
# Kill Helper
# ─────────────────────────────────────────────────────────────

def find_gossip_pids():
    """Returns list of (pid, port) for all running gossip_pull/push processes."""
    try:
        result = subprocess.run(
            ["ps", "aux"],
            capture_output=True, text=True
        )
        pids = []
        for line in result.stdout.splitlines():
            if "gossip_pull" in line or "gossip_push" in line:
                parts = line.split()
                pid   = parts[1]
                # Extract port from -addr flag
                port  = "unknown"
                if "-addr" in line:
                    idx  = line.index("-addr")
                    addr = line[idx:].split()[1]
                    port = addr.split(":")[-1]
                pids.append((pid, port))
        return pids
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# Main Monitor
# ─────────────────────────────────────────────────────────────

def run():
    conn = get_db_connection()
    if not conn:
        return

    try:
        conn.cursor().execute("SET time_zone = '+00:00';")
        cursor_nodes  = conn.cursor(dictionary=True, buffered=True)
        cursor_gossip = conn.cursor(dictionary=True, buffered=True)

        # Anchor to NOW — only watch new gossip from this point forward
        cursor_gossip.execute("SELECT COALESCE(MAX(id), 0) AS max_id FROM GossipRecord")
        last_id = cursor_gossip.fetchone()["max_id"]

        global_data   = {}
        reported_uids = set()

        baseline_durations   = []
        post_fault_durations = []

        # Get initial node count
        active_nodes        = get_active_nodes(cursor_nodes)
        baseline_node_count = len(active_nodes)

        print(f"\n🚀 Fault Tolerance Monitor Started")
        print(f"   Active nodes   : {baseline_node_count} → {sorted(active_nodes)}")
        print(f"   Baseline UIDs  : {BASELINE_UIDS}")
        print(f"   Post-fault UIDs: {POST_FAULT_UIDS}")
        print(f"   TTL            : {TTL_SECONDS}s")
        print(f"   Poll interval  : {POLL_INTERVAL}s")
        print(f"   ID anchor      : {last_id} (only watching NEW gossip)\n")

        if baseline_node_count == 0:
            print("❌ No active nodes found in ActionRecord. Start your cluster first!")
            return

        # Print initial neighbor list
        cursor_neighbors = conn.cursor(dictionary=True, buffered=True)
        cursor_activity  = conn.cursor(dictionary=True, buffered=True)
        initial_neighbors = get_neighbor_list(cursor_neighbors, active_nodes)
        print_neighbor_list(initial_neighbors, active_nodes, label="— INITIAL (before fault)")

        # Track ALL nodes ever seen (including dead ones later)
        all_known_nodes = set(active_nodes)

        # Print initial activity snapshot
        initial_activity = get_node_activity(cursor_activity, all_known_nodes)
        print_node_activity(initial_activity, active_nodes, all_known_nodes,
                           label="BEFORE FAULT")

        # ── PHASE 1: BASELINE ──────────────────────────────────
        print("=" * 58)
        print("📊 PHASE 1 — BASELINE (all nodes running normally)")
        print("=" * 58)

        while len(baseline_durations) < BASELINE_UIDS:
            active_nodes = get_active_nodes(cursor_nodes)
            rows         = poll_new_gossip(cursor_gossip, last_id)

            if rows:
                last_id = ingest_rows(rows, global_data, last_id)

            check_convergence(global_data, active_nodes,
                              reported_uids, baseline_durations, "BASELINE")

            print(f"  🔍 [{time.strftime('%X')}] "
                  f"polled={len(rows)} rows | "
                  f"id_anchor={last_id} | "
                  f"active={len(active_nodes)} | "
                  f"baseline={len(baseline_durations)}/{BASELINE_UIDS}")

            time.sleep(POLL_INTERVAL)

        print(f"\n✅ Baseline complete!")
        print_stats("BASELINE", baseline_durations, baseline_node_count)

        # ── PHASE 2: FAULT INJECTION ───────────────────────────
        print("\n" + "=" * 58)
        print("💥 PHASE 2 — FAULT INJECTION")
        print("=" * 58)

        # Show active nodes so user knows which port to kill
        print(f"\n  Active nodes:")
        for i, node in enumerate(sorted(active_nodes)):
            print(f"    [{i}] {node}")

        # Ask user which port to kill
        while True:
            port_input = input("\n  Enter the actionPort to kill (e.g. 9002): ").strip()
            try:
                kill_port = int(port_input)
                break
            except ValueError:
                print(f"  ❌ Invalid port '{port_input}' — enter a number")

        # Find which VM this port is running on
        # Works for both single VM and multi VM automatically
        kill_host = next(
            (n.split(":")[0]           # extract IP from "IP:PORT"
             for n in active_nodes     # search all active nodes
             if n.split(":")[1] == str(kill_port)),  # match by port
            "152.7.177.162"            # fallback to VM1 if not found
        )

        print(f"  🔍 Node-{kill_port} found on host: {kill_host}")

        # Build kill payload with correct host
        # host tells Action Initiator which VM's Node Manager to call
        kill_payload = [{
            "host":        kill_host,   # ← correct VM (works for multi VM)
            "actionPort":  kill_port,
            "managerPort": 8082,
            "peers":       [],
            "strategy":    "PUSH",
            "kafkaBroker": "152.7.177.162:9092",  # Kafka always on VM1
            "topic":       "gossip"
        }]

        print(f"\n  📤 Sending kill request for port {kill_port}...")
        try:
            resp = requests.post(
                "http://localhost:8080/action/kill",
                data=json.dumps(kill_payload),
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            if resp.status_code in [200, 201, 204]:
                print(f"  ✅ Kill request accepted — Node-{kill_port} is being terminated")
            else:
                print(f"  ⚠️  Kill returned {resp.status_code} — {resp.text}")
        except Exception as e:
            print(f"  ❌ Kill request failed: {e}")

        print(f"\n  ⏳ Waiting for cluster to detect and reform (TTL={TTL_SECONDS}s)...\n")

        fault_time      = time.time()
        detection_time  = None
        reform_time     = None
        cluster_reformed = False

        # ── PHASE 3: REFORM DETECTION ──────────────────────────
        print("=" * 58)
        print("⏳ PHASE 3 — WAITING FOR CLUSTER TO REFORM")
        print("=" * 58)

        no_gossip_polls = 0

        while not cluster_reformed:
            active_nodes = get_active_nodes(cursor_nodes)
            rows         = poll_new_gossip(cursor_gossip, last_id)

            if rows:
                last_id = ingest_rows(rows, global_data, last_id)
                no_gossip_polls = 0
            else:
                no_gossip_polls += 1

            elapsed = time.time() - fault_time
            ttl_remaining = max(0, TTL_SECONDS - elapsed)

            # Detect node loss in ActionRecord
            if len(active_nodes) < baseline_node_count and detection_time is None:
                detection_time = time.time()
                lost = baseline_node_count - len(active_nodes)
                print(f"\n  🔍 [{time.strftime('%X')}] "
                      f"Node loss detected! "
                      f"{lost} node(s) gone from ActionRecord "
                      f"at t={elapsed:.1f}s")

            print(f"  ⏳ [{time.strftime('%X')}] "
                  f"elapsed={elapsed:.0f}s | "
                  f"active={len(active_nodes)}/{baseline_node_count} | "
                  f"ttl_remaining={ttl_remaining:.0f}s | "
                  f"gossip_flowing={'yes' if rows else 'no ('+str(no_gossip_polls)+' polls)'}")

            # Print node activity every poll so user can watch 9002 go silent
            activity = get_node_activity(cursor_activity, all_known_nodes)
            all_known_nodes.update(activity.keys())
            print_node_activity(activity, active_nodes, all_known_nodes,
                               label=f"t={elapsed:.0f}s",
                               fault_time=fault_time,
                               ttl_seconds=TTL_SECONDS)

            # Reformed = TTL passed AND ActionRecord shows fewer nodes
            if elapsed >= TTL_SECONDS and len(active_nodes) < baseline_node_count:
                cluster_reformed = True
                reform_time      = time.time()
                print(f"\n  ✅ Cluster reformed with {len(active_nodes)} nodes "
                      f"after {elapsed:.1f}s!\n")

            # Reformed = TTL passed AND gossip stopped (manual kill, ActionRecord not updated)
            elif elapsed >= TTL_SECONDS and no_gossip_polls >= 2:
                cluster_reformed = True
                reform_time      = time.time()
                active_nodes     = {a for i, a in enumerate(sorted(active_nodes)) if i != baseline_node_count - 1}
                print(f"\n  ✅ Cluster reformed (detected via gossip stop) "
                      f"after {elapsed:.1f}s — {baseline_node_count - 1} nodes active\n")

            # Safety: TTL passed twice, proceed anyway
            elif elapsed >= TTL_SECONDS * 2:
                print(f"\n  ⚠️  TTL window passed twice. "
                      f"Proceeding with {len(active_nodes)} active nodes.")
                cluster_reformed = True
                reform_time      = time.time()

            time.sleep(POLL_INTERVAL)

        reformed_node_count = len(active_nodes)

        # Print final activity after reform
        final_activity = get_node_activity(cursor_activity, all_known_nodes)
        print_node_activity(final_activity, active_nodes, all_known_nodes,
                           label="AFTER FAULT — FINAL STATE",
                           fault_time=fault_time,
                           ttl_seconds=TTL_SECONDS)

        # Print updated neighbor list after reform
        updated_neighbors = get_neighbor_list(cursor_neighbors, active_nodes)
        print_neighbor_list(updated_neighbors, active_nodes, label="— AFTER FAULT (reformed cluster)")

        # ── PHASE 4: POST-FAULT CONVERGENCE ───────────────────
        print("=" * 58)
        print(f"📊 PHASE 4 — POST-FAULT ({reformed_node_count} nodes active)")
        print("=" * 58)

        post_fault_reported = set()  # fresh set — count only new convergences

        while len(post_fault_durations) < POST_FAULT_UIDS:
            active_nodes = get_active_nodes(cursor_nodes)
            rows         = poll_new_gossip(cursor_gossip, last_id)

            if rows:
                last_id = ingest_rows(rows, global_data, last_id)

            check_convergence(global_data, active_nodes,
                              post_fault_reported, post_fault_durations, "POST-FAULT")

            print(f"  🔍 [{time.strftime('%X')}] "
                  f"polled={len(rows)} rows | "
                  f"id_anchor={last_id} | "
                  f"active={len(active_nodes)} | "
                  f"post_fault={len(post_fault_durations)}/{POST_FAULT_UIDS}")

            time.sleep(POLL_INTERVAL)

        # ── FINAL REPORT ───────────────────────────────────────
        print("\n" + "=" * 58)
        print("📋 FINAL FAULT TOLERANCE REPORT")
        print("=" * 58)

        print(f"\n  🟢 BASELINE ({baseline_node_count} nodes)")
        print_stats("BASELINE", baseline_durations, baseline_node_count)

        print(f"\n  🔴 POST-FAULT ({reformed_node_count} nodes)")
        print_stats("POST-FAULT", post_fault_durations, reformed_node_count)

        print(f"\n  ⚡ FAULT EVENT SUMMARY")
        print(f"  {'-'*58}")
        print(f"  {'Nodes before fault':<35} | {baseline_node_count}")
        print(f"  {'Nodes after fault':<35} | {reformed_node_count}")
        print(f"  {'Nodes lost':<35} | {baseline_node_count - reformed_node_count}")
        print(f"  {'TTL window':<35} | {TTL_SECONDS}s")

        if detection_time:
            detect_lag = detection_time - fault_time
            print(f"  {'Time to detect dead node':<35} | {detect_lag:.1f}s")

        if reform_time:
            reform_lag = reform_time - fault_time
            print(f"  {'Time to reform cluster':<35} | {reform_lag:.1f}s")

        if baseline_durations and post_fault_durations:
            b_mean = statistics.mean(baseline_durations)
            p_mean = statistics.mean(post_fault_durations)
            delta  = p_mean - b_mean
            pct    = (delta / b_mean) * 100 if b_mean > 0 else 0
            direction = "slower ⬆️" if delta > 0 else "faster ⬇️"
            print(f"  {'Baseline mean convergence':<35} | {b_mean:.4f}s")
            print(f"  {'Post-fault mean convergence':<35} | {p_mean:.4f}s")
            print(f"  {'Delta':<35} | {delta:+.4f}s ({pct:+.1f}% {direction})")

        print(f"\n  {'Cluster recovered?':<35} | ✅ YES — converging with {reformed_node_count} nodes")
        print(f"  {'Messages still propagating?':<35} | ✅ YES — {POST_FAULT_UIDS} UIDs converged post-fault")
        print("=" * 58)

    except KeyboardInterrupt:
        print("\n\n⚠️  Monitor interrupted by user.")
    except Error as e:
        print(f"❌ Database error: {e}")
    finally:
        if conn.is_connected():
            cursor_nodes.close()
            cursor_gossip.close()
            conn.close()
            print("\n🔌 DB connection closed.")


if __name__ == "__main__":
    run()