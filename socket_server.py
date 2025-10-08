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
