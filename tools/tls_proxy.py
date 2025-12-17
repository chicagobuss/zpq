import socket
import ssl
import threading
import sys

# Configuration
LISTEN_PORT = 9443
TARGET_HOST = '127.0.0.1'
TARGET_PORT = 9000
CERT_FILE = 'cert.pem'
KEY_FILE = 'key.pem'

def handle_client(client_sock):
    target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        target_sock.connect((TARGET_HOST, TARGET_PORT))
    except Exception as e:
        print(f"[Proxy] Failed to connect to target: {e}")
        client_sock.close()
        return

    def forward(src, dst, name):
        try:
            while True:
                data = src.recv(4096)
                if not data:
                    print(f"[{name}] Connection closed")
                    break
                print(f"[{name}] {len(data)} bytes")
                dst.sendall(data)
        except Exception as e:
            print(f"[{name}] Error: {e}")
        finally:
            src.close()
            dst.close()

    t1 = threading.Thread(target=forward, args=(client_sock, target_sock, "Client->Server"))
    t2 = threading.Thread(target=forward, args=(target_sock, client_sock, "Server->Client"))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

def main():
    # Create context
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=CERT_FILE, keyfile=KEY_FILE)
    
    bind_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bind_sock.bind(('0.0.0.0', LISTEN_PORT))
    bind_sock.listen(5)
    print(f"[Proxy] Listening on {LISTEN_PORT} (TLS) -> {TARGET_HOST}:{TARGET_PORT}")

    while True:
        try:
            newsock, fromaddr = bind_sock.accept()
            print(f"[Proxy] Accept from {fromaddr}")
            try:
                conn = context.wrap_socket(newsock, server_side=True)
                print(f"[Proxy] TLS Handshake Success: {conn.version()} {conn.cipher()}")
                t = threading.Thread(target=handle_client, args=(conn,))
                t.start()
            except ssl.SSLError as e:
                print(f"[Proxy] TLS Handshake Failed: {e}")
                newsock.close()
        except KeyboardInterrupt:
            break

if __name__ == '__main__':
    main()

