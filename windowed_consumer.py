from kafka import KafkaConsumer
import json, time

consumer = KafkaConsumer(
    'sensor_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='sensor-group',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Counting messages in 10-second windows...")

start_time = time.time()
count = 0

for msg in consumer:
    count += 1
    now = time.time()
    if now - start_time >= 10:
        print(f"Messages in last 10 seconds: {count}")
        start_time = now
        count = 0
