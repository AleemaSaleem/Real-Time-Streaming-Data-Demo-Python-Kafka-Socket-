# 🌊 Real-Time Streaming Data Demo (Python + Kafka / Socket)

This project demonstrates **basic streaming concepts** and **simple data flows** using two different approaches:

1. **Apache Kafka** – a distributed streaming platform.
2. **Python Sockets** – a lightweight alternative for quick testing.

The project simulates **real-time sensor readings** (like temperature) and shows:
- Live data production and consumption
- Continuous message printing
- Windowed counting (messages every 10 seconds)

---

## 🧭 Project Goals

- Understand fundamental **streaming data** concepts.
- Learn to **produce** and **consume** real-time messages.
- Implement **windowed counting** logic (e.g., messages per 10 seconds).
- Compare a **Kafka-based** solution vs. a **simple socket-based** one.

---

## ⚙️ Requirements

- **Ubuntu (Linux)**
- **Python 3.8+**
- **pip**
- **Java (for Kafka only)**

Install common dependencies:
```bash
sudo apt update
sudo apt install python3 python3-pip -y

# 🅰️ Method 1: Apache Kafka
1️⃣ Install Kafka
```bash

You said:
Goal: Understand basic streaming concepts and simple data flows.   Tasks   Set up a local streaming tool    Install Apache Kafka (or use a lightweight Python socket stream).   Produce and consume data    Write a small Python program that sends random sensor readings (e.g., temperature).   Write another program that prints every message as it arrives.   Windowed Counting    Count how many messages arrive every 10 seconds and display the count.  using ubuntu
ChatGPT said:
Perfect 👍 — this is a great beginner project to understand streaming data using Python and Kafka (or sockets) on Ubuntu.
Let’s go step-by-step so you can easily set up and run everything locally.

🧭 Overview
You’ll do three main things:

Set up a local streaming tool — Apache Kafka or simple Python sockets.

Produce and consume data — a sensor simulator sends random readings; a listener prints them.

Windowed counting — count how many messages arrive every 10 seconds.

We’ll do it in two variants:

(A) Kafka-based (real streaming framework)

(B) Python socket-based (lightweight and simple)

🧩 A. Kafka-Based Streaming Setup
1️⃣ Install Kafka on Ubuntu
Run these commands in your terminal:

sudo apt update
sudo apt install openjdk-11-jre -y
Download and extract Kafka:

wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0
Start Zookeeper (Kafka needs it):

bin/zookeeper-server-start.sh config/zookeeper.properties
Then open a new terminal tab and start Kafka broker:

bin/kafka-server-start.sh config/server.properties
2️⃣ Create a Kafka Topic
In a new terminal tab:

cd kafka_2.13-3.8.0
bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
3️⃣ Install Kafka Python client
pip install kafka-python
4️⃣ Python Producer (sends random sensor data)
Save as producer.py:

from kafka import KafkaProducer
import json, random, time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "timestamp": time.time()
    }
    producer.send('sensor_data', data)
    print(f"Sent: {data}")
    time.sleep(1)
Run:

python3 producer.py
5️⃣ Python Consumer (prints every message)
Save as consumer.py:

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
Run:

python3 consumer.py
You’ll see sensor readings being printed live 🎯

6️⃣ Windowed Counting (every 10 seconds)
Modify your consumer to count messages in 10-second windows.

Save as windowed_consumer.py:

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
Run:

python3 windowed_consumer.py
🪶 B. Lightweight Alternative: Python Socket Stream (no Kafka)
If you don’t want to install Kafka, use this simple socket method.

1️⃣ Producer (server)
Save as socket_server.py:

import socket, time, random, json

HOST = 'localhost'
PORT = 9999

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen(1)
print("Server started, waiting for connection...")

conn, addr = server.accept()
print(f"Connected by {addr}")

while True:
    data = {
        "sensor_id": random.randint(1, 5),
        "temperature": round(random.uniform(20.0, 35.0), 2),
        "timestamp": time.time()
    }
    conn.sendall((json.dumps(data) + "\n").encode('utf-8'))
    print(f"Sent: {data}")
    time.sleep(1)
2️⃣ Consumer (client)
Save as socket_client.py:

import socket, json, time

HOST = 'localhost'
PORT = 9999

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((HOST, PORT))

print("Connected to stream. Receiving data...")
start_time = time.time()
count = 0

buffer = ""
while True:
    chunk = client.recv(1024).decode('utf-8')
    buffer += chunk
    while "\n" in buffer:
        line, buffer = buffer.split("\n", 1)
        data = json.loads(line)
        print(f"Received: {data}")
        count += 1
        if time.time() - start_time >= 10:
            print(f"Messages in last 10 seconds: {count}")
            count = 0
            start_time = time.time()
Run in two terminals:

python3 socket_server.py
python3 socket_client.py
✅ Summary
Task	Kafka Method	Socket Method
Setup	Install Kafka	Built-in Python
Producer	producer.py	socket_server.py
Consumer	consumer.py	socket_client.py
Windowed Count	windowed_consumer.py	included in client
Would you like me to provide a single combined Python file (no Kafka, just sockets) that runs both producer and consumer in the same script for demo/testing?

You said:
create the read me file for this project using both methods in .md format
ChatGPT said:
Here’s a clean, professional README.md file you can include in your project folder.
It documents both the Kafka-based and Socket-based streaming setups in Markdown format.

# 🌊 Real-Time Streaming Data Demo (Python + Kafka / Socket)

This project demonstrates **basic streaming concepts** and **simple data flows** using two different approaches:

1. **Apache Kafka** – a distributed streaming platform.
2. **Python Sockets** – a lightweight alternative for quick testing.

The project simulates **real-time sensor readings** (like temperature) and shows:
- Live data production and consumption
- Continuous message printing
- Windowed counting (messages every 10 seconds)

---

## 🧭 Project Goals

- Understand fundamental **streaming data** concepts.
- Learn to **produce** and **consume** real-time messages.
- Implement **windowed counting** logic (e.g., messages per 10 seconds).
- Compare a **Kafka-based** solution vs. a **simple socket-based** one.

---

## ⚙️ Requirements

- **Ubuntu (Linux)**
- **Python 3.8+**
- **pip**
- **Java (for Kafka only)**

Install common dependencies:
```bash
sudo apt update
sudo apt install python3 python3-pip -y
🅰️ Method 1: Apache Kafka
1️⃣ Install Kafka
```bash
sudo apt install openjdk-11-jre -y
wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0

Start Zookeeper:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties

Start Kafka Broker (in a new terminal):
```bash
bin/kafka-server-start.sh config/server.properties

