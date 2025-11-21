from kafka import KafkaConsumer
import json

def main():
    topic_name = '../Dataset/full_data.csv'

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='football-consumer-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("Waiting for messages... (press Ctrl+C to exit)")

    with open('consumed_events.json', 'a', encoding='utf-8') as f:
        try:
            for message in consumer:
                event_str = str(message.value).encode('utf-8', errors='ignore').decode('utf-8')
                print(f"Received event: {event_str}")
                f.write(event_str + '\n')
        except KeyboardInterrupt:
            print("Consumer stopped.")

if __name__ == "__main__":
    main()
