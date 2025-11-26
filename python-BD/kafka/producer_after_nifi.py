import json
from kafka import KafkaProducer
import time

# New topic after NiFi
OUTPUT_TOPIC = "processed_football_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

# This file is created by NiFi (PutFile, or any output processor)
NIFI_OUTPUT_FILE = "kafka/nifi_output.json"   # must contain a JSON array


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    return producer


def produce_after_nifi():
    producer = create_producer()
    print(f"Sending NiFi-processed events to Kafka topic '{OUTPUT_TOPIC}'...")

    # NiFi output must be JSON array
    with open(NIFI_OUTPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    total = len(data)

    for idx, event in enumerate(data, start=1):
        producer.send(OUTPUT_TOPIC, value=event)
        print(f"[{idx}/{total}] Sent event: {event}")
        time.sleep(0.05)

    producer.flush()
    producer.close()
    print("Finished sending all NiFi-processed events.")


if __name__ == "__main__":
    produce_after_nifi()