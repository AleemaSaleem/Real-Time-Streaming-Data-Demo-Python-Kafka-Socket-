from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='sensor-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening for messages...")

for msg in consumer:
    print(f"Received: {msg.value}")
