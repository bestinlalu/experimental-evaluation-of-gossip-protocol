import requests
import json
import random

# --- CONFIGURATION ---
HOSTS = ["152.7.178.142", "152.7.178.134", "152.7.178.151", "152.7.178.90"]        # Single VM — use localhost
                                       # Add "152.7.179.79" for 2-VM test
START_PORT      = 6000
END_PORT        = 8000
NUM_NODES       = 100                    # Start with 4, scale to 100 later
PEERS_PER_NODE  = max(1, int(NUM_NODES * 0.3))  # 30% of previous nodes
MANAGER_PORT    = 8082
STRATEGY        = "PUSH"              # PUSH for this test
KAFKA_BROKER    = "152.7.178.142:9092"
TOPIC           = "gossip"
URL             = "http://localhost:9090/action/start"


def generate_ordered_payload():
    """
    Builds N node configs where each node only knows peers
    that were created before it — simulating realistic bootstrap.

    Node 0  → peers: []
    Node 1  → peers: [Node 0]
    Node 2  → peers: [Node 0 or Node 1]
    Node 10 → peers: [3 random from Node 0-9]
    """
    payload           = []
    created_addresses = []   # grows as nodes are added
    used_combinations = set()

    for i in range(NUM_NODES):
        # Pick a unique host:port
        while True:
            h    = random.choice(HOSTS)
            p    = random.randint(START_PORT, END_PORT)
            addr = f"{h}:{p}"
            if addr not in used_combinations:
                used_combinations.add(addr)
                current_host = h
                current_port = p
                break

        # Select peers only from already-created nodes
        selected_peers = []
        if created_addresses:
            num_to_sample  = min(len(created_addresses), PEERS_PER_NODE)
            selected_peers = random.sample(created_addresses, num_to_sample)

        node_entry = {
            "host":        current_host,
            "actionPort":  current_port,
            "managerPort": MANAGER_PORT,
            "peers":       selected_peers,
            "strategy":    STRATEGY,
            "kafkaBroker": KAFKA_BROKER,
            "topic":       TOPIC
        }

        payload.append(node_entry)
        created_addresses.append(f"{current_host}:{current_port}")

    return payload, created_addresses


def start_cluster():
    payload, all_addresses = generate_ordered_payload()

    # Print all nodes so you know which ports to kill later
    print("\nCluster Topology:")
    print(f"{'Index':<8} {'Address':<25} {'Peers'}")
    print("-" * 70)
    for i, node in enumerate(payload):
        addr  = f"{node['host']}:{node['actionPort']}"
        peers = node['peers'] if node['peers'] else ["(none — bootstrap node)"]
        print(f"  [{i:<4}] {addr:<25} → {peers}")

    print(f"\nSummary:")
    print(f"   Total nodes   : {NUM_NODES}")
    print(f"   Strategy      : {STRATEGY}")
    print(f"   Peers/node    : ~{PEERS_PER_NODE} (30% of predecessors)")
    print(f"   Kafka broker  : {KAFKA_BROKER}")
    print(f"   Topic         : {TOPIC}")

    # Save addresses to file so fault_tolerance_monitor knows which ports to watch
    with open("./ft_cluster_addresses.json", "w") as f:
        json.dump(all_addresses, f)
    print(f"\nNode addresses saved to ./ft_cluster_addresses.json")

    # Fire the request
    print(f"\nSending START request to {URL}...")
    try:
        response = requests.post(
            URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        if response.status_code in [200, 201]:
            print(f"✅ Success! {NUM_NODES} nodes started.")
            print(f"\n⚡ NEXT STEP:")
            print(f"   Wait 5-10 seconds for nodes to boot, then run:")
            print(f"   python3 fault_tolerance_monitor.py")
        else:
            print(f"❌ Error {response.status_code}: {response.text}")

    except requests.exceptions.ConnectionError:
        print("❌ Could not connect. Is the action initiator running on port 9090?")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")


if __name__ == "__main__":
    start_cluster()
