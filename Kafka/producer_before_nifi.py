import pandas as pd
from kafka import KafkaProducer
import json
import time
import sys
import math
import re
import os

sys.stdout.reconfigure(encoding='utf-8')

TOPIC_NAME = "football_dataset_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CSV_FOLDER = "./Dataset"
CSV_FILES = ["dataset_part_1.csv", "dataset_part_2.csv", "dataset_part_3.csv"]
SLEEP_SECONDS = 0.1


def replace_nan_values(obj, replacement=None):
    if isinstance(obj, dict):
        return {key: replace_nan_values(value, replacement) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_values(item, replacement) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return replacement
    else:
        return obj


def parse_inc_field(inc_str):
    events = []
    if not inc_str or inc_str == "[]" or pd.isna(inc_str):
        return events

    inc_str = str(inc_str)
    inc_str = inc_str.strip("[]")
    raw_events = re.split(r"',\s*'", inc_str.strip("'"))

    for raw in raw_events:
        raw = raw.strip().strip("'")
        m = re.match(r"(\d+\+?\d*)\s+([A-Za-z_]+)-?\s*(.*?)\((.*?)\)", raw)
        if m:
            minute, typeteam, player, reason = m.groups()
            if "_" in typeteam:
                typ, team = typeteam.split("_", 1)
            else:
                typ, team = typeteam, ""
            events.append({
                "minute": minute,
                "team": team,
                "type": typ,
                "player": player.strip(),
                "reason": reason.strip()
            })
        else:
            if raw:
                events.append({"raw": raw})
    return events


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: json.dumps(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )


def process_event(event):
    event = replace_nan_values(event, replacement=None)
    if "INC" in event:
        event["INC"] = parse_inc_field(event["INC"])
    return event


def stream_csv_to_kafka(csv_path, producer, topic, sleep_seconds=0.1, start_idx=1):
    df = pd.read_csv(csv_path)
    total_rows = len(df)
    print(f"Streaming {total_rows} rows from {os.path.basename(csv_path)} to topic '{topic}'...")

    for idx, (_, row) in enumerate(df.iterrows(), start=start_idx):
        event = row.to_dict()
        event = process_event(event)

        key = {"row_id": idx}

        producer.send(topic, key=key, value=event)

        safe_json = json.dumps(event, ensure_ascii=False)
        print(f"[{idx}] Sent event with key={key}: {safe_json}")

        if sleep_seconds:
            time.sleep(sleep_seconds)

    return start_idx + total_rows  # return next row index


def main():
    producer = create_producer()
    try:
        current_idx = 1
        for csv_file in CSV_FILES:
            csv_path = os.path.join(CSV_FOLDER, csv_file)
            if os.path.exists(csv_path):
                current_idx = stream_csv_to_kafka(csv_path, producer, TOPIC_NAME, SLEEP_SECONDS, start_idx=current_idx)
            else:
                print(f"File not found: {csv_path}")
    finally:
        producer.close()
        print("Producer closed.")


if __name__ == "__main__":
    main()
