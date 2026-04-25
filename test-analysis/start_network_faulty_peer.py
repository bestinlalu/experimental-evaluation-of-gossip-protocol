import argparse

import requests
import json
import random

# --- CONFIGURATION ---
HOSTS = ["152.7.179.141", "152.7.179.79"]
START_PORT = 6000
END_PORT = 8000
NUM_NODES = 10
PEERS_PER_NODE = int(NUM_NODES * 0.3)  # Each node knows 30% of the previous nodes, rounded down

MANAGER_PORT = 8082
STRATEGY = "PUSH"
KAFKA_BROKER = "152.7.179.141:9092"
TOPIC = "gossip"

URL = "http://localhost:9090/action/start"

def generate_ordered_payload():
    payload = []
    created_addresses = [] # Pool of nodes created "before" the current one
    used_combinations = set()
    inject_faulty_peer = False
    faulty_peer = ""

    for i in range(NUM_NODES):
        # 1. Generate unique host/port for current node
        while True:
            h = random.choice(HOSTS)
            p = random.randint(START_PORT, END_PORT)
            addr = f"{h}:{p}"
            if addr not in used_combinations:
                used_combinations.add(addr)
                current_host = h
                current_port = p
                break

        # 2. Select peers ONLY from nodes already in the created_addresses list
        # This enforces: JsonObject[i] only knows peers from JsonObject[0...i-1]
        selected_peers = []
        if created_addresses:
            # We can only sample as many peers as actually exist in the "before" pool
            num_to_sample = len(created_addresses) if len(created_addresses) < PEERS_PER_NODE else PEERS_PER_NODE
            selected_peers = random.sample(created_addresses, num_to_sample)

        if not inject_faulty_peer and (5 == random.randint(1, 10) or i == NUM_NODES - 1):  # Randomly decide to inject a faulty peer, but only once and not on the last node
            faulty_peer = f"192.0.2.{random.randint(1, 254)}:{random.randint(5000, 6000)}"  # Using TEST-NET-1
            selected_peers.remove(random.choice(selected_peers))  # Remove one valid peer to keep the count consistent
            selected_peers.append(faulty_peer)  # Add the faulty peer
            inject_faulty_peer = True

        # 3. Create the object
        node_entry = {
            "host": current_host,
            "actionPort": current_port,
            "managerPort": MANAGER_PORT,
            "peers": selected_peers,
            "strategy": STRATEGY,
            "kafkaBroker": KAFKA_BROKER,
            "topic": TOPIC
        }

        payload.append(node_entry)
        
        # 4. Add current node to the "available pool" for the NEXT nodes in the array
        created_addresses.append(f"{current_host}:{current_port}")
    # Store the created addresses for verification
    with open("robustness.json", "w") as f:
        json.dump({"created_addresses": created_addresses, "injected_faulty_peer": faulty_peer, "payload": payload}, f, indent=4)

    return payload

def start_ordered_cluster():
    payload = generate_ordered_payload()
    
    # Print the first 3 nodes to verify the "Order" logic
    print("📜 Verification of Peer Order:")
    for i, node in enumerate(payload):
        print(f"Node {i} ({node['host']}:{node['actionPort']}) -> Peers: {node['peers']}")

    try:
        response = requests.post(
            URL, 
            data=json.dumps(payload), 
            headers={'Content-Type': 'application/json'}
        )

        if response.status_code in [200, 201]:
            print(f"\n🚀 Success! Started {len(payload)} nodes with strict ordered peers.")
        else:
            print(f"\n❌ Error: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"\n❌ Connection failed: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-strategy", required=True, default="PUSH")
    parser.add_argument("-nodes", type=int, required=True, default=10)
    STRATEGY = parser.parse_args().strategy
    NUM_NODES = parser.parse_args().nodes
    start_ordered_cluster()