import subprocess
import time
import os

# Adjust Kafka directory if needed
KAFKA_DIR = r"C:\kafka"

ZK_START = os.path.join(KAFKA_DIR, "bin", "windows", "zookeeper-server-start.bat")
ZK_CONFIG = os.path.join(KAFKA_DIR, "config", "zookeeper.properties")

KAFKA_START = os.path.join(KAFKA_DIR, "bin", "windows", "kafka-server-start.bat")
KAFKA_CONFIG = os.path.join(KAFKA_DIR, "config", "server.properties")

def start_process(cmd):
    return subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)

def main():
    print("Starting ZooKeeper...")
    zk = start_process([ZK_START, ZK_CONFIG])
    time.sleep(5)

    print("Starting Kafka Broker...")
    kafka = start_process([KAFKA_START, KAFKA_CONFIG])
    time.sleep(10)

    print("Starting Producer...")
    producer = start_process(["python", "producer.py"])

    print("Starting Consumer...")
    consumer = start_process(["python", "consumer.py"])

    print("\nAll services and scripts are now running.")
    print("Close this window to stop everything.\n")

    try:
        producer.wait()
        consumer.wait()
    except KeyboardInterrupt:
        print("Shutting down...")

if __name__ == "__main__":
    main()
