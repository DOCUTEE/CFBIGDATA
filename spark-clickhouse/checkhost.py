import socket

def check_host(host: str, port: int, timeout: float = 3.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            print(f"✅ Host '{host}' is reachable on port {port}")
            return True
    except socket.gaierror:
        print(f"❌ Hostname '{host}' could not be resolved.")
    except socket.timeout:
        print(f"⏱️ Connection to '{host}:{port}' timed out.")
    except ConnectionRefusedError:
        print(f"🔌 Connection to '{host}:{port}' was refused.")
    except Exception as e:
        print(f"❗ Unexpected error: {e}")
    return False

# Example usage
check_host("cf-clickhouse", 8123)
