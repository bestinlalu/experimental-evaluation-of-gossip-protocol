import time, json, statistics
from inject_fault_tolerance import kill_node, find_gossip_pids
import mysql.connector
from mysql.connector import Error
from datetime import datetime
 
# ── CONFIG ─────────────────────────────────────────────────────
DB_CONFIG = {
    'host':            'localhost',
    'database':        'gossipdb',
    'user':            'root',
    'password':        'password',
    'port':            3306,
    'connect_timeout': 10
}

POLL_INTERVAL   = 5
BASELINE_UIDS   = 5
POST_FAULT_UIDS = 5
TTL_SECONDS     = 30
 
# ── DB HELPERS ─────────────────────────────────────────────────
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

def get_neighbors_from_gossip(cursor):
    """
    Gets latest neighbor list per node from GossipRecord neighbors column.
    Returns dict: { forwarderAddress → [neighbor1, neighbor2, ...] }
    """
    cursor.execute("""
        SELECT forwarderAddress, neighbors
        FROM GossipRecord
        WHERE neighbors IS NOT NULL AND neighbors != ''
        ORDER BY forwarderTimestamp DESC
        LIMIT 1000
    """)
    rows = cursor.fetchall()
    seen = {}
    for row in rows:
        fwd = row['forwarderAddress']
        if fwd not in seen and row['neighbors']:
            seen[fwd] = [n.strip() for n in row['neighbors'].split(',') if n.strip()]
    return seen
 
# ── STATE TRACKER ──────────────────────────────────────────────
def ingest_rows(rows, global_data, last_id, uid_first=None):
    for row in rows:
        uid, fwd, creator = row['uid'], row['forwarderAddress'], row['creatorAddress']
        if uid not in global_data:
            global_data[uid] = {}
            if uid_first is not None:
                uid_first.setdefault(uid, row['id'])  # first GossipRecord ID for this UID
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
 
def check_convergence(global_data, active_nodes, reported_uids, durations, phase_label,
                      anchor=None, uid_first=None):
    for uid, node_map in global_data.items():
        if uid in reported_uids:
            continue
        # Post-fault filter: skip UIDs that existed before the kill
        if anchor is not None and uid_first is not None:
            if uid_first.get(uid, 0) <= anchor:
                continue
        active_reached = {addr for addr in node_map if addr in active_nodes}
        if len(active_reached) >= len(active_nodes):
            start_time = min(d["ct"] for d in node_map.values())
            end_time   = max(d["ft"] for d in node_map.values())
            duration   = end_time - start_time
            durations.append(duration)
            reported_uids.add(uid)
            print(f"  ✅ [{phase_label}] UID {uid[:8]}... | "
                  f"{duration:.4f}s | "
                  f"nodes: {len(active_reached)}/{len(active_nodes)}")
 
# ── STATS PRINTER ──────────────────────────────────────────────
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
 
def print_neighbors(neighbor_map, active_nodes, label=""):
    print(f"\n  📡 Neighbor List — {label}")
    print(f"  {'─'*55}")
    for node in sorted(neighbor_map.keys()):
        peers    = neighbor_map[node]
        status   = "✅" if node in active_nodes else "💀 DEAD"
        alive    = [f"✅ {p}" for p in peers if p in active_nodes]
        dead     = [f"💀 {p}" for p in peers if p not in active_nodes]
        all_peers = ", ".join(alive + dead) or "(none)"
        print(f"  {status} {node:<28} → {all_peers}")
    print(f"  {'─'*55}")
    
# ── MAIN ───────────────────────────────────────────────────────
def run():
    conn = get_db_connection()
    if not conn:
        return
 
    try:
        conn.cursor().execute("SET time_zone='+00:00';")
        cursor_nodes  = conn.cursor(dictionary=True, buffered=True)
        cursor_gossip = conn.cursor(dictionary=True, buffered=True)
 
        cursor_gossip.execute("SELECT COALESCE(MAX(id),0) AS max_id FROM GossipRecord")
        last_id     = cursor_gossip.fetchone()["max_id"]
        global_data = {}
        uid_first   = {}   # uid → first GossipRecord ID it appeared in
        done        = set()
        b_dur, p_dur = [], []
        nodes       = get_active_nodes(cursor_nodes)
        b_count     = len(nodes)
 
        print(f"\n🚀 Monitor started | nodes={sorted(nodes)} | anchor={last_id}\n")
        if b_count == 0:
            print("❌ No active nodes found. Start your cluster first!")
            return
 
        # ── PHASE 1: BASELINE ──────────────────────────────────
        print("="*50 + "\n📊 PHASE 1 — BASELINE\n" + "="*50)
        while len(b_dur) < BASELINE_UIDS:
            nodes = get_active_nodes(cursor_nodes)
            rows  = poll_new_gossip(cursor_gossip, last_id)
            if rows:
                last_id = ingest_rows(rows, global_data, last_id, uid_first)
            check_convergence(global_data, nodes, done, b_dur, "BASELINE")
            print(f"  🔍 [{time.strftime('%X')}] polled={len(rows)} | "
                  f"anchor={last_id} | baseline={len(b_dur)}/{BASELINE_UIDS}")
            time.sleep(POLL_INTERVAL)
        print("\n✅ Baseline complete!")
        print_stats("BASELINE", b_dur, b_count)
        
        cursor_nb = conn.cursor(dictionary=True, buffered=True)
        nb_before = get_neighbors_from_gossip(cursor_nb)
        print_neighbors(nb_before, nodes, label="BEFORE FAULT")
 
        # ── PHASE 2: KILL NODE ─────────────────────────────────
        print("\n" + "="*50 + "\n💥 PHASE 2 — FAULT INJECTION\n" + "="*50)
        kill_port, _ = kill_node(active_nodes=nodes, strategy="PUSH")
        anchor     = last_id   # snapshot at moment of kill
        fault_time = time.time()
        detect_t   = reform_t = None
        print(f"  📌 Post-fault anchor = {anchor}\n")
 
        # ── PHASE 3: WAIT FOR REFORM ───────────────────────────
        print("="*50 + "\n⏳ PHASE 3 — WAITING FOR REFORM\n" + "="*50)
        no_rows = 0
        while True:
            nodes   = get_active_nodes(cursor_nodes)
            rows    = poll_new_gossip(cursor_gossip, last_id)
            if rows:
                last_id = ingest_rows(rows, global_data, last_id, uid_first)
                no_rows = 0
            else:
                no_rows += 1
            elapsed = time.time() - fault_time
            if len(nodes) < b_count and not detect_t:
                detect_t = time.time()
                print(f"\n  🔍 Node loss detected at t={elapsed:.1f}s\n")
            print(f"  ⏳ [{time.strftime('%X')}] elapsed={elapsed:.0f}s | "
                  f"active={len(nodes)}/{b_count} | "
                  f"ttl_left={max(0,TTL_SECONDS-elapsed):.0f}s | "
                  f"gossip={'flowing' if rows else 'stopped'}")
            if elapsed >= TTL_SECONDS and len(nodes) < b_count:
                reform_t = time.time()
                print(f"\n  ✅ Cluster reformed with {len(nodes)} nodes!\n")
                break
            if elapsed >= TTL_SECONDS and no_rows >= 2:
                reform_t = time.time()
                print(f"\n  ✅ Reform detected via gossip stop!\n")
                break
            if elapsed >= TTL_SECONDS * 2:
                reform_t = time.time()
                print(f"\n  ⚠️  Safety timeout — proceeding\n")
                break
            time.sleep(POLL_INTERVAL)
        reformed = len(nodes)
        
        nb_after = get_neighbors_from_gossip(cursor_nb)
        print_neighbors(nb_after, nodes, label="AFTER FAULT")
 
        # ── PHASE 4: POST FAULT ────────────────────────────────
        print("="*50 + f"\n📊 PHASE 4 — POST-FAULT ({reformed} nodes)\n" + "="*50)
        print(f"  ℹ️  Only counting UIDs with first GossipRecord ID > {anchor}\n")
        p_done = set()
        while len(p_dur) < POST_FAULT_UIDS:
            nodes = get_active_nodes(cursor_nodes)
            rows  = poll_new_gossip(cursor_gossip, last_id)
            if rows:
                last_id = ingest_rows(rows, global_data, last_id, uid_first)
            check_convergence(global_data, nodes, p_done, p_dur, "POST-FAULT",
                              anchor=anchor, uid_first=uid_first)
            print(f"  🔍 [{time.strftime('%X')}] polled={len(rows)} | "
                  f"anchor={last_id} | post_fault={len(p_dur)}/{POST_FAULT_UIDS}")
            time.sleep(POLL_INTERVAL)
 
        # ── FINAL REPORT ───────────────────────────────────────
        print("\n" + "="*50 + "\n📋 FINAL FAULT TOLERANCE REPORT\n" + "="*50)
        print(f"\n  🟢 BASELINE ({b_count} nodes)")
        print_stats("BASELINE", b_dur, b_count)
        print(f"\n  🔴 POST-FAULT ({reformed} nodes)")
        print_stats("POST-FAULT", p_dur, reformed)
        print(f"\n  ⚡ FAULT SUMMARY")
        print(f"  {'-'*50}")
        print(f"  {'Nodes before fault':<30} | {b_count}")
        print(f"  {'Nodes after fault':<30} | {reformed}")
        print(f"  {'Nodes lost':<30} | {b_count - reformed}")
        print(f"  {'TTL window':<30} | {TTL_SECONDS}s")
        if detect_t:
            print(f"  {'Detection time':<30} | {detect_t-fault_time:.1f}s")
        if reform_t:
            print(f"  {'Reform time':<30} | {reform_t-fault_time:.1f}s")
        if b_dur and p_dur:
            delta = statistics.mean(p_dur) - statistics.mean(b_dur)
            pct   = (delta / statistics.mean(b_dur)) * 100
            print(f"  {'Baseline mean':<30} | {statistics.mean(b_dur):.4f}s")
            print(f"  {'Post-fault mean':<30} | {statistics.mean(p_dur):.4f}s")
            print(f"  {'Convergence delta':<30} | {delta:+.4f}s ({pct:+.1f}% {'slower' if delta>0 else 'faster'})")
        print(f"  {'Cluster recovered':<30} | ✅ YES — {reformed} nodes converging")
        print(f"  {'Messages propagating':<30} | ✅ YES — {POST_FAULT_UIDS} UIDs converged")
        print("="*50)
 
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