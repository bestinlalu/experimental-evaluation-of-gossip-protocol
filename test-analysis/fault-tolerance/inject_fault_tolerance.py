import subprocess, requests, json

# ── CONFIG ─────────────────────────────────────────────────────
KILL_URL     = "http://localhost:9090/action/kill"
KAFKA_BROKER = "152.7.179.8:9092"
TOPIC        = "gossip"
MANAGER_PORT = 8082

def find_gossip_pids():
    """Returns list of (pid, port) for all running gossip processes."""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True)
        pids = []
        for line in result.stdout.splitlines():
            if "gossip_pull" in line or "gossip_push" in line:
                parts = line.split()
                pid   = parts[1]
                port  = "unknown"
                if "-addr" in line:
                    idx  = line.index("-addr")
                    addr = line[idx:].split()[1]
                    port = addr.split(":")[-1]
                pids.append((pid, port))
        return pids
    except Exception:
        return []

def kill_node(active_nodes, strategy="PUSH"):
    """
    Prompts user for port, finds correct host, calls kill API.
    Returns (kill_port, success: bool)
    """
    print(f"\n  Active nodes:")
    for node in sorted(active_nodes):
        print(f"    {node}")

    pids = find_gossip_pids()
    if pids:
        print(f"\n  Running gossip processes:")
        print(f"  {'PID':<10} {'Port'}")
        print(f"  {'-'*20}")
        for pid, port in pids:
            print(f"  {pid:<10}   {port}")

    kill_port = int(input("\n  Enter port to kill: ").strip())
    kill_host = next(
        (n.split(":")[0] for n in active_nodes if n.split(":")[1] == str(kill_port)),
        "152.7.177.162"
    )
    print(f"  🔍 Node-{kill_port} found on host: {kill_host}")

    try:
        resp = requests.post(KILL_URL, json=[{
            "host": kill_host, "actionPort": kill_port,
            "managerPort": MANAGER_PORT, "peers": [],
            "strategy": strategy, "kafkaBroker": KAFKA_BROKER, "topic": TOPIC
        }])
        success = resp.status_code in [200, 201, 204]
        print(f"  {'✅' if success else '❌'} Kill response: {resp.status_code}")
        return kill_port, success
    except Exception as e:
        print(f"  ❌ Kill request failed: {e}")
        return kill_port, False


# Run standalone if needed
if __name__ == "__main__":
    port, ok = kill_node(active_nodes=set(), strategy="PUSH")
    print(f"Killed port {port} — success={ok}")