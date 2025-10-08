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
