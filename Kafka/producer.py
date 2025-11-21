import pandas as pd
from kafka import KafkaProducer
import json
import time

def main():
    # Load your CSV
    df = pd.read_csv(r"../Dataset/full_data.csv")

    # Configure Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    topic_name = 'football_dataset_events'

    # Send each row as a Kafka message
    for _, row in df.iterrows():
        event = row.to_dict()
        producer.send(topic_name, value=event)
        producer.flush()

        print(f"Sent event: {str(event).encode('utf-8', errors='ignore').decode('utf-8')}")
        time.sleep(0.1)

    print("All events sent!")

if __name__ == "__main__":
    main()
