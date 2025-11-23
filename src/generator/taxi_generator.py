import json
import time
import pandas as pd
from kafka import KafkaProducer

# Kafka broker (adjust if needed)
KAFKA_BROKER = "localhost:9092"
TOPIC = "taxi_trips"

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Load sample data CSV
df = pd.read_csv("../../data/samples/sample_1k.csv")   # Put your sample file here

print("Starting event production...")
for _, row in df.iterrows():
    event = row.to_dict()
    producer.send(TOPIC, event)
    print("Sent:", event)
    time.sleep(0.5)  # 2 events per second (adjust as needed)

producer.flush()
print("Finished sending events!")

