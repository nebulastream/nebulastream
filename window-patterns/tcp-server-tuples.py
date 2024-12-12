#! /usr/bin/python3
import socket
import threading
import time
import argparse
import random
import logging


class ClientHandler:
    def __init__(
        self,
        client_socket,
        address,
        interval,
        count_limit,
        time_step,
        unorderedness,
        min_delay,
        max_delay,
    ):
        self.client_socket = client_socket
        self.address = address
        self.running = True
        self.interval = interval
        self.count_limit = count_limit
        self.time_step = time_step
        self.unorderedness = unorderedness
        self.min_delay = min_delay
        self.max_delay = max_delay

        # tuple attributes
        self.counter = 0
        self.value = 0
        self.payload = 0
        self.timestamp = 0
        import logging

        # configure logging
        logging.basicConfig(filename='tcp-server-tuples.log', level=logging.INFO,
                            format='%(message)s')

    def handle(self):
        print(f"New connection from {self.address}")
        try:
            while self.running and (
                self.count_limit is None or self.counter < self.count_limit
            ):
                start_time = time.time()
                self.value = random.randint(0, 10000)

                current_ts = self.timestamp
                # add random delay for some tuples with the percentage of unorderedness
                percentage = random.random()
                if percentage < self.unorderedness:
                    delay = random.randint(self.min_delay, self.max_delay)
                    random_bool = random.choice([True, False])
                    if current_ts - delay >= 0 and random_bool:
                        # add negative delay randomly, if possible
                        current_ts -= delay
                    else:
                        # or add positive delay
                        current_ts += delay

                message = f"{self.counter},{self.value},{self.payload},{current_ts}\n"
                self.client_socket.send(message.encode("utf-8"))

                self.counter += 1
                # increase global ts by time step
                self.timestamp += self.time_step

                if self.count_limit and self.counter >= self.count_limit:
                    print(
                        f"Client {self.address} reached count limit of {self.count_limit}"
                    )
                    break
                end_time = time.time()
                execution_time = end_time - start_time
                if (execution_time > self.interval):
                    logging.info(f"TCP server could not match given interval.")
                else:
                    pass
                    # time.sleep(self.interval - execution_time)
        except (BrokenPipeError, ConnectionResetError):
            print(f"Client {self.address} disconnected")
        finally:
            self.cleanup()

    def cleanup(self):
        self.running = False
        try:
            self.client_socket.close()
            logging.info("TCP source shut down.")
        except:
            pass
        print(f"Cleaned up connection from {self.address}")


class CounterServer:
    def __init__(
        self,
        host="0.0.0.0",
        port=5000,
        interval=1.0,
        count_limit=None,
        time_step=1,
        unorderedness=0,
        min_delay=0,
        max_delay=0,
    ):
        self.host = host
        self.port = port
        self.interval = interval
        self.count_limit = count_limit
        self.time_step = time_step
        self.unorderedness = unorderedness
        self.min_delay = min_delay
        self.max_delay = max_delay
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
        print(
            f"timeStep: {self.time_step}, unorderedness: {self.unorderedness}, minDelay: {self.min_delay}, maxDelay: {self.max_delay}"
        )

        try:
            while self.running:
                client_socket, address = self.server_socket.accept()
                client = ClientHandler(
                    client_socket,
                    address,
                    self.interval,
                    self.count_limit,
                    self.time_step,
                    self.unorderedness,
                    self.min_delay,
                    self.max_delay,
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
        "-i",
        "--interval",
        type=float,
        default=1.0,
        help="Interval in seconds between counter updates",
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
    parser.add_argument(
        "-u",
        "--unorderedness",
        type=float,
        default=0,
        help="Percentage of out-of-order tuples",
    )
    # parser.add_argument(
    #     '-d', '--delay-range',
    #     nargs=2,
    #     type=int,
    #     default=[0,0],
    #     help='Delay range in ms for an out-of-order tuple'
    # )
    parser.add_argument(
        "-d",
        "--min-delay",
        type=int,
        default=0,
        help="Minimum delay in ms for an out-of-order tuple",
    )
    parser.add_argument(
        "-m",
        "--max-delay",
        type=int,
        default=0,
        help="Maximum delay in ms for an out-of-order tuple",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()
    server = CounterServer(
        host=args.host,
        port=args.port,
        interval=args.interval,
        count_limit=args.count_limit,
        time_step=args.time_step,
        unorderedness=args.unorderedness,
        # min_delay=args.delay_range[0],
        # max_delay=args.delay_range[1]
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
    server.start()
