import psutil
import time
import json
import argparse
import threading
from datetime import datetime
from kafka import KafkaProducer

def get_node_metrics():
    """Captures CPU, RAM, and Disk I/O."""
    cpu = psutil.cpu_percent(interval=1.0)
    ram = psutil.virtual_memory().percent
    io = psutil.disk_io_counters()
    return {
        "cpu": cpu,
        "ram": ram,
        "io_read": io.read_bytes,
        "io_write": io.write_bytes
    }

def start_telemetry(node_addr, broker):
    """Initializes Kafka producer and starts a 1-second collection loop."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # Ensure high reliability for metrics
            acks=1 
        )
        print(f"🚀 Telemetry started for {node_addr}. Sending to Broker: {broker}")
    except Exception as e:
        print(f"❌ Failed to connect to Kafka Broker: {e}")
        return

    def run():
        while True:
            metrics = get_node_metrics()
            payload = {
                "node_address": node_addr,
                "cpu": metrics["cpu"],
                "ram": metrics["ram"],
                "io_read": metrics["io_read"],
                "io_write": metrics["io_write"],
                "ts": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
            }
            
            # Send to the dedicated telemetry topic
            producer.send('metrics', payload)
            time.sleep(1)

    # Run in a background thread so the node can keep gossiping
    thread = threading.Thread(target=run, daemon=True)
    thread.start()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Node Metrics Producer")
    parser.add_argument("-addr", help="This Node's Address (e.g. 152.7.179.79:6981)")
    parser.add_argument("-kafka-broker", help="Remote Kafka Broker IP (e.g. 152.7.179.141:9092)")
    
    args = parser.parse_args()
    
    # This keeps the script alive for testing; 
    # In your real node code, this would be part of your main startup logic
    start_telemetry(args.addr, args.kafkabroker)
    
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("🛑 Telemetry stopped.")