import pandas as pd
from kafka import KafkaProducer
import json
import time
import sys
import math
import re

# Force UTF-8 output in Windows terminal to avoid UnicodeEncodeError
sys.stdout.reconfigure(encoding='utf-8')

TOPIC_NAME = "football_dataset_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]
CSV_PATH = "./Dataset/full_data.csv"
SLEEP_SECONDS = 0.1   # delay between rows to simulate live stream


def replace_nan_values(obj, replacement=None):
    """Recursively replace NaN values in a dict/list with replacement."""
    if isinstance(obj, dict):
        return {key: replace_nan_values(value, replacement) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_values(item, replacement) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return replacement
    else:
        return obj


def parse_inc_field(inc_str):
    """
    Convert INC string to structured list of events:
    Example:
      "['13' Yellow_Home - Corredera A.(Podcięcie)', ...]"
    → [{"minute": 13, "team": "Home", "type": "Yellow", "player": "Corredera A.", "reason": "Podcięcie"}, ...]
    """
    events = []
    if not inc_str or inc_str == "[]" or pd.isna(inc_str):
        return events

    # Convert to string if needed
    inc_str = str(inc_str)
    
    # Remove surrounding brackets and split by comma that separates events
    inc_str = inc_str.strip("[]")
    raw_events = re.split(r"',\s*'", inc_str.strip("'"))

    for raw in raw_events:
        raw = raw.strip().strip("'")
        # Match: "13 Yellow_Home - Corredera A.(Podcięcie)"
        m = re.match(r"(\d+\+?\d*)\s+([A-Za-z_]+)-?\s*(.*?)\((.*?)\)", raw)
        if m:
            minute, typeteam, player, reason = m.groups()
            # Split type and team
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
            # fallback: keep as raw string if it doesn't match pattern
            if raw:  # only add if not empty
                events.append({"raw": raw})
    return events


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
    )


def process_event(event):
    """Process a single event: replace NaN values and parse INC field."""
    # Replace NaN values
    event = replace_nan_values(event, replacement=None)
    
    # Parse INC field to structured array
    if "INC" in event:
        event["INC"] = parse_inc_field(event["INC"])
    
    return event


def stream_csv_to_kafka(csv_path, producer, topic, sleep_seconds=0.1):
    df = pd.read_csv(csv_path)

    total_rows = len(df)
    print(f"Starting to stream {total_rows} rows from CSV to Kafka topic '{topic}'...")

    for idx, (_, row) in enumerate(df.iterrows(), start=1):
        # Convert row to dict
        event = row.to_dict()
        
        # Process the event (NaN replacement + INC parsing)
        event = process_event(event)
        
        # Send processed event to Kafka
        producer.send(topic, value=event)

        # Safe print (no crash on special characters)
        safe_json = json.dumps(event, ensure_ascii=False)
        print(f"[{idx}/{total_rows}] Sent processed event: {safe_json}")

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