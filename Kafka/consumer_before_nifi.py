from kafka import KafkaConsumer
import json
import math

TOPIC_NAME = "football_dataset_events"
BOOTSTRAP_SERVERS = ["localhost:9092"]

GROUP_ID = "football-consumer-group"
OUTPUT_FILE = "consumed_events.json"  # standard JSON (array of objects)


def replace_nan_values(obj, replacement=None):
    """
    Recursively replace NaN values in a dictionary or list.
    
    Args:
        obj: The object to process (dict, list, or primitive)
        replacement: Value to replace NaN with (None, 0, "", or "NaN")
    
    Returns:
        The object with NaN values replaced
    """
    if isinstance(obj, dict):
        return {key: replace_nan_values(value, replacement) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [replace_nan_values(item, replacement) for item in obj]
    elif isinstance(obj, float) and math.isnan(obj):
        return replacement
    else:
        return obj


def create_consumer():
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",     # read from beginning if no committed offset
        enable_auto_commit=True,
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )
    return consumer


def consume_to_json_file():
    consumer = create_consumer()
    print(f"Consuming from topic '{TOPIC_NAME}'... (Ctrl+C to stop)")
    first_record = True

    # "w" to overwrite file and produce a valid JSON array
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("[\n")  # start JSON array
        try:
            for message in consumer:
                event = message.value
                
                # Replace NaN values with null (or choose: 0, "", "NaN")
                event = replace_nan_values(event, replacement=None)
                
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