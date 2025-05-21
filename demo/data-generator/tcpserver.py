import socket
import time
import random
import datetime
import threading
import csv
import io

HOST = '0.0.0.0'  # Listen on all available network interfaces
PORT = 9999        # Port to listen on (non-privileged ports are > 1023)

def handle_client(client_socket, client_address):
    """Handles connection for a single client."""
    print(f"[+] Accepted connection from {client_address[0]}:{client_address[1]}")
    monotonic_counter = 0
    try:
        while True:
            monotonic_counter += 1
            timestamp = int(datetime.datetime.now().timestamp())
            random_val = random.uniform(0.0, 100.0)

            # Use io.StringIO and csv.writer for robust CSV formatting
            output = io.StringIO()
            writer = csv.writer(output, quoting=csv.QUOTE_NONE, escapechar='\\')
            writer.writerow([timestamp, f"{random_val:.4f}", monotonic_counter])
            csv_line = output.getvalue().strip() + '\n' # Get value and add newline

            client_socket.sendall(csv_line.encode('utf-8'))
            time.sleep(0.01)

    except (ConnectionResetError, BrokenPipeError):
        print(f"[-] Client {client_address[0]}:{client_address[1]} disconnected.")
    except Exception as e:
        print(f"[!] Error handling client {client_address[0]}:{client_address[1]}: {e}")
    finally:
        print(f"[*] Closing connection to {client_address[0]}:{client_address[1]}")
        client_socket.close()

def start_server():
    """Starts the TCP server."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Allow reusing the address immediately after the server stops
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5) # Max number of queued connections
        print(f"[*] Listening on {HOST}:{PORT}")

        while True:
            client_sock, address = server_socket.accept()
            # Create a new thread to handle the client connection
            client_handler = threading.Thread(
                target=handle_client,
                args=(client_sock, address)
            )
            client_handler.daemon = True # Allow program to exit even if threads are running
            client_handler.start()

    except KeyboardInterrupt:
        print("\n[*] Server shutting down...")
    except Exception as e:
        print(f"[!] Server error: {e}")
    finally:
        print("[*] Closing server socket.")
        server_socket.close()

if __name__ == "__main__":
    print("Starting")
    start_server()
