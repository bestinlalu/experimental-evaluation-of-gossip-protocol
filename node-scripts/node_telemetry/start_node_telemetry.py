import psutil
import time
import argparse
import json
import os
from datetime import datetime
from kafka import KafkaProducer

def get_pid_by_port(port):
    """Finds the process listening on the specified port."""
    for proc in psutil.process_iter(['pid']):
        try:
            for conn in proc.connections(kind='inet'):
                if conn.laddr.port == port:
                    return proc.pid
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            continue
    return None

def get_io_bytes(pid):
    """Reads raw rchar (read) and wchar (write) bytes from the proc filesystem."""
    try:
        # rchar and wchar represent the total bytes passed through read() and write() syscalls
        # For a UDP app with no disk activity, this is 100% network traffic.
        with open(f"/proc/{pid}/io", "r") as f:
            lines = f.readlines()
            stats = {line.split(':')[0]: int(line.split(':')[1].strip()) for line in lines}
            return stats['rchar'], stats['wchar']
    except (FileNotFoundError, PermissionError):
        return 0, 0

def start_telemetry(node_addr, broker):
    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    port = int(node_addr.split(':')[-1])
    pid = get_pid_by_port(port)

    if not pid:
        print(f"❌ No node found on port {port}")
        return

    print(f"✅ Monitoring PID {pid} for Node {node_addr}")
    
    # Initial "Odometer" readings
    last_read, last_write = get_io_bytes(pid)
    proc = psutil.Process(pid)

    try:
        while True:
            # 1. CPU and RAM
            cpu = proc.cpu_percent(interval=None)
            ram = proc.memory_info().rss / (1024 * 1024)

            # 2. Per-PID Network Bytes (Deltas)
            current_read, current_write = get_io_bytes(pid)
            delta_read = current_read - last_read
            delta_write = current_write - last_write
            
            # Update anchors
            last_read, last_write = current_read, current_write

            payload = {
                "nodeAddress": node_addr,
                "cpuPercentage": cpu,
                "memoryPercentage": round(ram, 2),
                "io_read_mbytes": delta_read,   # Now sending raw BYTES
                "io_write_mbytes": delta_write, # Now sending raw BYTES
                "timestamp": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
            }

            producer.send('metrics', payload)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")

if __name__ == "__main__":
    time.sleep(5)  # Wait for the node to start and bind to the port
    parser = argparse.ArgumentParser()
    parser.add_argument("-addr", required=True)
    parser.add_argument("-kafka-broker", required=True)
    args = parser.parse_args()
    start_telemetry(args.addr, args.kafka_broker)