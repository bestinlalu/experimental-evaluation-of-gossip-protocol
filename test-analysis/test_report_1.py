import time
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import statistics

# --- CONFIGURATION ---
DB_CONFIG = {
    'host': '152.7.179.141',
    'database': 'gossipdb',
    'user': 'root',
    'password': 'password',
    'port': 3306,
    'connect_timeout': 10
}
POLL_INTERVAL = 5 
TARGET_UID_COUNT = 10

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn
    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
        return None

def monitor_convergence():
    conn = get_db_connection()
    if not conn: return

    print(f"🚀 Starting Seek-Based Optimized Monitor...")
    
    try:
        # Set session to UTC to match VM/DB timestamps
        cursor_sync = conn.cursor()
        cursor_sync.execute("SET time_zone = '+00:00';")
        cursor_sync.close()

        # Dual cursors prevent "Commands out of sync"
        cursor_nodes = conn.cursor(dictionary=True, buffered=True)
        cursor_gossip = conn.cursor(dictionary=True, buffered=True)
        
        # --- INITIAL ANCHOR SETUP ---
        cursor_gossip.execute("SELECT MAX(id) as max_id FROM GossipRecord")
        initial = cursor_gossip.fetchone()
        # Start from the current max ID to only monitor NEW gossip, 
        # or 0 if starting a fresh experiment
        last_id = 0
        
        reported_uids = set()
        convergence_durations = []
        # Persistent store for UID progress across polling intervals
        global_data = {}

        time.sleep(5)  # Initial wait to allow nodes to start and generate some gossip

        while len(reported_uids) < TARGET_UID_COUNT:
            
            # 1. Update Active Nodes
            cursor_nodes.execute("""
                WITH RankedActions AS (
                    SELECT 
                        CONCAT(host, ':', actionPort) AS node_address, 
                        action,
                        ROW_NUMBER() OVER(PARTITION BY host, actionPort ORDER BY timestamp DESC) as rn
                    FROM ActionRecord
                )
                SELECT node_address FROM RankedActions WHERE rn = 1 AND action = 'START';
            """)
            active_nodes = {row["node_address"] for row in cursor_nodes.fetchall()}
            total_active = len(active_nodes)
            
            if total_active == 0:
                print(f"[{time.strftime('%X')}] ⚠️ No active nodes detected. Waiting...")
                time.sleep(POLL_INTERVAL)
                continue

            # 2. Poll new records since last_id
            query = """
                SELECT id, uid, creatorAddress, forwarderAddress, forwarderTimestamp, creationTimestamp 
                FROM GossipRecord 
                WHERE id > %s ORDER BY id ASC LIMIT 10000;
            """
            cursor_gossip.execute(query, (last_id,))
            rows = cursor_gossip.fetchall()

            print(f"🔍 [{time.strftime('%X')}] Polled {len(rows)} new gossip records since ID {last_id}. Active Nodes: {total_active}")

            if rows:
                for row in rows:
                    uid = row['uid']
                    node_who_has_it = row['forwarderAddress']
                    creator_node = row['creatorAddress']
                    
                    if uid not in global_data:
                        global_data[uid] = {}
                        # OPTIONAL: Explicitly mark the creator as having the message at CT
                        if creator_node:
                            global_data[uid][creator_node] = {
                                "ft": row['creationTimestamp'].timestamp(),
                                "ct": row['creationTimestamp'].timestamp()
                            }
                    
                    # Attribute this arrival to the forwarder
                    global_data[uid][node_who_has_it] = {
                        "ft": row['forwarderTimestamp'].timestamp() if isinstance(row['forwarderTimestamp'], datetime) else float(row['forwarderTimestamp']),
                        "ct": row['creationTimestamp'].timestamp() if isinstance(row['creationTimestamp'], datetime) else float(row['creationTimestamp'])
                    }
                last_id = rows[-1]['id']

                # 3. Check Convergence
                for uid in list(global_data.keys()):
                    if uid in reported_uids: continue
                    
                    nodes_with_this_uid = global_data[uid]
                    active_reached = {addr for addr in nodes_with_this_uid if addr in active_nodes}
                    
                    if len(active_reached) >= total_active:
                        # Calculation: Latest Arrival - Original Creation
                        start_time = next(iter(nodes_with_this_uid.values()))["ct"]
                        end_time = max(d["ft"] for d in nodes_with_this_uid.values())
                        
                        duration = end_time - start_time
                        convergence_durations.append(duration)
                        reported_uids.add(uid)
                        
                        print(f"✅ [CONVERGED] {uid} | {duration:.4f}s | Total: {len(reported_uids)}/{TARGET_UID_COUNT}")
                    else:
                        # Optional: Log progress for stuck UIDs
                        pass 
                    if len(reported_uids) >= TARGET_UID_COUNT:
                        break

            print(f"🔍 [{time.strftime('%X')}] ID Anchor: {last_id} | Active: {total_active} | Converged: {len(reported_uids)}")
            time.sleep(POLL_INTERVAL)

        print_final_stats(convergence_durations, total_active)

    except Error as e:
        print(f"❌ Database error: {e}")
    finally:
        if conn.is_connected():
            cursor_nodes.close()
            cursor_gossip.close()
            conn.close()

def print_final_stats(durations, node_count):
    if not durations: return
    print("\n" + "="*40)
    print(f"📊 FINAL GOSSIP REPORT ({node_count} Nodes)")
    print("="*40)
    print(f"{'Mean Duration':<20} | {statistics.mean(durations):.4f}s")
    print(f"{'Min Duration':<20} | {min(durations):.4f}s")
    print(f"{'Max Duration':<20} | {max(durations):.4f}s")
    print(f"{'Std Dev':<20} | {statistics.stdev(durations) if len(durations) > 1 else 0:.4f}s")
    print("="*40)

if __name__ == "__main__":
    monitor_convergence()