#! /usr/bin/python3
import socket
import threading
import time
import argparse

class ClientHandler:
    def __init__(self, client_socket, address, interval, count_limit):
        self.client_socket = client_socket
        self.address = address
        self.counter = 0
        self.running = True
        self.interval = interval
        self.count_limit = count_limit

    def handle(self):
        print(f"New connection from {self.address}")
        try:
            while self.running and (self.count_limit is None or self.counter < self.count_limit):
                message = f"{self.counter}\n"
                self.client_socket.send(message.encode('utf-8'))
                self.counter += 1
                if self.count_limit and self.counter >= self.count_limit:
                    print(f"Client {self.address} reached count limit of {self.count_limit}")
                    break
                time.sleep(self.interval)
        except (BrokenPipeError, ConnectionResetError):
            print(f"Client {self.address} disconnected")
        finally:
            self.cleanup()

    def cleanup(self):
        self.running = False
        try:
            self.client_socket.close()
        except:
            pass
        print(f"Cleaned up connection from {self.address}")

class CounterServer:
    def __init__(self, host='0.0.0.0', port=5000, interval=1.0, count_limit=None):
        self.host = host
        self.port = port
        self.interval = interval
        self.count_limit = count_limit
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients = []
        self.running = True

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}")
        print(f"Counter interval: {self.interval} seconds")
        print(f"Count limit: {self.count_limit if self.count_limit else 'unlimited'}")

        try:
            while self.running:
                client_socket, address = self.server_socket.accept()
                client = ClientHandler(client_socket, address, self.interval, self.count_limit)
                client_thread = threading.Thread(target=client.handle)
                client_thread.daemon = True
                client_thread.start()
                self.clients.append(client)
                # Clean up disconnected clients
                self.clients = [c for c in self.clients if c.running]
        except KeyboardInterrupt:
            print("\nShutting down server...")
        finally:
            self.cleanup()

    def cleanup(self):
        self.running = False
        # Clean up all client connections
        for client in self.clients:
            client.cleanup()
        # Close server socket
        try:
            self.server_socket.close()
        except:
            pass
        print("Server shutdown complete")

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='TCP server that emits an incrementing counter to each client.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-p', '--port',
        type=int,
        default=5000,
        help='Port number to listen on'
    )
    parser.add_argument(
        '-i', '--interval',
        type=float,
        default=1.0,
        help='Interval in seconds between counter updates'
    )
    parser.add_argument(
        '-n', '--count-limit',
        type=int,
        default=None,
        help='Number of values to emit before terminating connection (default: unlimited)'
    )
    parser.add_argument(
        '--host',
        default='0.0.0.0',
        help='Host address to bind to'
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_arguments()
    server = CounterServer(
        host=args.host,
        port=args.port,
        interval=args.interval,
        count_limit=args.count_limit
    )
    server.start()
