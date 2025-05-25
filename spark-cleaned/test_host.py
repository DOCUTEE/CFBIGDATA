import socket

host = "hive-service"
port = 9083

try:
    print(f"Resolving {host}...")
    ip = socket.gethostbyname(host)
    print(f"{host} resolved to {ip}")

    print(f"Connecting to {host}:{port}...")
    with socket.create_connection((host, port), timeout=5) as s:
        print(f"Successfully connected to {host}:{port}")
except Exception as e:
    print(f"Failed to connect to {host}:{port} - {e}")