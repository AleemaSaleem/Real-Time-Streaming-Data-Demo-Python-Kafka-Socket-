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
```

---

# 🅰️ Method 1: Apache Kafka

### 1️⃣ Install Kafka

```bash
sudo apt install openjdk-11-jre -y
wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
tar -xzf kafka_2.13-3.8.0.tgz
cd kafka_2.13-3.8.0
```

Start **Zookeeper**:
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Start **Kafka Broker** (in a new terminal):
```bash
bin/kafka-server-start.sh config/server.properties
```

---

### 2️⃣ Create Topic

```bash
cd kafka_2.13-3.8.0
bin/kafka-topics.sh --create --topic sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

### 3️⃣ Install Kafka Python Library

```bash
pip install kafka-python
```

---

### 4️⃣ Producer (Send Sensor Data)

**File:** `producer.py`

```python
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
```

Run:
```bash
python3 producer.py
```

---

### 5️⃣ Consumer (Receive and Print Messages)

**File:** `consumer.py`

```python
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
```

Run:
```bash
python3 consumer.py
```

---

### 6️⃣ Windowed Counting (Every 10 Seconds)

**File:** `windowed_consumer.py`

```python
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
```

Run:
```bash
python3 windowed_consumer.py
```

---

# 🅱️ Method 2: Python Socket Stream (Lightweight)

This version skips Kafka and uses a simple **TCP socket** for streaming data locally.

---

### 1️⃣ Socket Producer (Server)

**File:** `socket_server.py`

```python
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
```

Run in one terminal:
```bash
python3 socket_server.py
```

---

### 2️⃣ Socket Consumer (Client)

**File:** `socket_client.py`

```python
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
```

Run in another terminal:
```bash
python3 socket_client.py
```

---

# 🧪 Expected Output

```
Sent: {'sensor_id': 3, 'temperature': 31.24, 'timestamp': 1728388273.12}
Received: {'sensor_id': 3, 'temperature': 31.24, 'timestamp': 1728388273.12}
Messages in last 10 seconds: 10
```

---

# 🧱 Project Structure

```
streaming-demo/
│
├── README.md
├── producer.py
├── consumer.py
├── windowed_consumer.py
├── socket_server.py
└── socket_client.py
```

---

# 🧠 Concepts Covered

- **Streaming Data Pipelines**
- **Producer–Consumer Architecture**
- **Message Serialization (JSON)**
- **Windowed Computation**
- **Real-Time Analytics**

---

# 🏁 Summary

| Feature | Kafka Method | Socket Method |
|----------|---------------|---------------|
| Complexity | Moderate | Simple |
| Realism | ✅ Industry-standard | ⚡ Quick demo |
| Setup | Requires Kafka | Pure Python |
| Throughput | High | Limited |
| Ideal For | Learning Kafka & Streaming | Small demos or offline labs |

---

# 🧑‍💻 Author

**Aleema Saleem**  
MS Data Science — NED University  
Project: *Understanding Streaming Data Concepts in Python*
