import subprocess
import time
import os

KAFKA_DIR = r"C:\kafka"

ZK_START = os.path.join(KAFKA_DIR, "bin", "windows", "zookeeper-server-start.bat")
ZK_CONFIG = os.path.join(KAFKA_DIR, "config", "zookeeper.properties")

KAFKA_START = os.path.join(KAFKA_DIR, "bin", "windows", "kafka-server-start.bat")
KAFKA_CONFIG = os.path.join(KAFKA_DIR, "config", "server.properties")

# Your Python script names
PRODUCER_BEFORE = "producer_before_nifi.py"    # raw CSV → Kafka topic A
CONSUMER_BEFORE = "consumer_before_nifi.py"    # read events before NiFi
PRODUCER_AFTER = "producer_after_nifi.py"      # NiFi output → Kafka topic B


def start_process(cmd):
    return subprocess.Popen(cmd, creationflags=subprocess.CREATE_NEW_CONSOLE)


def main():
    print("Starting ZooKeeper...")
    zk = start_process([ZK_START, ZK_CONFIG])
    time.sleep(5)

    print("Starting Kafka Broker...")
    kafka = start_process([KAFKA_START, KAFKA_CONFIG])
    time.sleep(10)

    print("Starting Producer BEFORE NiFi...")
    producer_before = start_process(["python", PRODUCER_BEFORE])
    time.sleep(2)

    print("Starting Consumer BEFORE NiFi...")
    consumer_before = start_process(["python", CONSUMER_BEFORE])
    time.sleep(2)

    print("Starting Producer AFTER NiFi...")
    producer_after = start_process(["python", PRODUCER_AFTER])
    time.sleep(2)

    print("\nAll services & scripts are now running.\n")
    print("✔ ZooKeeper")
    print("✔ Kafka Broker")
    print("✔ Producer BEFORE NiFi")
    print("✔ Consumer BEFORE NiFi")
    print("✔ Producer AFTER NiFi")
    print("\nClose this window to stop everything.\n")

    try:
        producer_before.wait()
        consumer_before.wait()
        producer_after.wait()
    except KeyboardInterrupt:
        print("Shutting down...")


if __name__ == "__main__":
    main()
