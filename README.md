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
