#! /usr/bin/python3
import socket
import threading
import time
import argparse
import random


class ClientHandler:
    def __init__(
        self,
        client_socket,
        address,
        count_limit,
        time_step,
    ):
        self.client_socket = client_socket
        self.address = address
        self.running = True
        self.count_limit = count_limit
        self.time_step = time_step

        # tuple attributes
        self.counter = 0
        self.value = 0
        self.payload = 0
        self.timestamp = 0

    def handle(self):
        print(f"New connection from {self.address}")
        try:
            while self.running and (
                self.count_limit is None or self.counter < self.count_limit
            ):
                self.value = random.randint(0, 10000)

                message = (
                    f"{self.counter},{self.value},{self.payload},{self.timestamp}\n"
                )
                self.client_socket.send(message.encode("utf-8"))

                self.counter += 1
                # increase global ts by time step
                self.timestamp += self.time_step

                if self.count_limit and self.counter >= self.count_limit:
                    print(
                        f"Client {self.address} reached count limit of {self.count_limit}"
                    )
                    break
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
    def __init__(
        self,
        host="0.0.0.0",
        port=5000,
        count_limit=None,
        time_step=1,
    ):
        self.host = host
        self.port = port
        self.count_limit = count_limit
        self.time_step = time_step
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients = []
        self.running = True

    def start(self):
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}")
        print(f"Count limit: {self.count_limit if self.count_limit else 'unlimited'}")

        try:
            while self.running:
                client_socket, address = self.server_socket.accept()
                client = ClientHandler(
                    client_socket,
                    address,
                    self.count_limit,
                    self.time_step,
                )
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
        description="TCP server that emits tuples to each client.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--host", default="0.0.0.0", help="Host address to bind to")
    parser.add_argument(
        "-p", "--port", type=int, default=5000, help="Port number to listen on"
    )
    parser.add_argument(
        "-n",
        "--count-limit",
        type=int,
        default=None,
        help="Number of values to emit before terminating connection (default: unlimited)",
    )
    parser.add_argument(
        "-t", "--time-step", type=int, default=1, help="Time step in ms"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    server = CounterServer(
        host=args.host,
        port=args.port,
        count_limit=args.count_limit,
        time_step=args.time_step,
    )
    server.start()
