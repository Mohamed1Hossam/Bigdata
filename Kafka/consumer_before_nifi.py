from kafka import KafkaConsumer
import json
import math
import re

TOPIC_NAME = "football_dataset_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

GROUP_ID = "football-consumer-group"
OUTPUT_FILE = "consumed_events.json"  # standard JSON (array of objects)


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
    if not inc_str or inc_str == "[]":
        return events

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
            events.append({"raw": raw})
    return events


def create_consumer():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    return consumer


def consume_to_json_file():
    consumer = create_consumer()
    print(f"Consuming from topic '{TOPIC_NAME}'... (Ctrl+C to stop)")
    first_record = True

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("[\n")  # start JSON array
        try:
            for message in consumer:
                event = message.value

                # Replace NaN values
                event = replace_nan_values(event, replacement=None)

                # Parse INC field to structured array
                if "INC" in event:
                    event["INC"] = parse_inc_field(event["INC"])

                line = json.dumps(event, ensure_ascii=False)

                print(f"Offset {message.offset}: {line}")

                if first_record:
                    f.write(line)
                    first_record = False
                else:
                    f.write(",\n" + line)

                f.flush()

        except KeyboardInterrupt:
            print("Stopping consumer...")

        finally:
            f.write("\n]\n")  # close JSON array
            consumer.close()
            print("Consumer closed. JSON written to", OUTPUT_FILE)


if __name__ == "__main__":
    consume_to_json_file()