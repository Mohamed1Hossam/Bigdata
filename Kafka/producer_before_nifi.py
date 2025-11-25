import pandas as pd
from kafka import KafkaProducer
import json
import time
import sys

# Force UTF-8 output in Windows terminal to avoid UnicodeEncodeError
sys.stdout.reconfigure(encoding='utf-8')

TOPIC_NAME = "football_dataset_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CSV_PATH = "./Dataset/full_data.csv"
SLEEP_SECONDS = 0.1   # delay between rows to simulate live stream


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )


def stream_csv_to_kafka(csv_path, producer, topic, sleep_seconds=0.1):
    df = pd.read_csv(csv_path)

    total_rows = len(df)
    print(f"Starting to stream {total_rows} rows from CSV to Kafka topic '{topic}'...")

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        event = row.to_dict()
        producer.send(topic, value=event)

        # Safe print (no crash on special characters)
        safe_json = json.dumps(event, ensure_ascii=False)
        print(f"[{idx}/{total_rows}] Sent event: {safe_json}")

        if sleep_seconds:
            time.sleep(sleep_seconds)

    producer.flush()
    print("All events sent and flushed.")


def main():
    producer = create_producer()
    try:
        stream_csv_to_kafka(CSV_PATH, producer, TOPIC_NAME, SLEEP_SECONDS)
    finally:
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
