#!/usr/bin/env python3
"""
TCP Client to test the TCP Server
"""
import socket
import argparse
import sys

class TCPClient:
    def __init__(self, host='localhost', port=8080):
        self.host = host
        self.port = port
        self.socket = None
    
    def connect(self):
        """Connect to the TCP server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            print(f"Connected to {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Failed to connect to {self.host}:{self.port}: {e}")
            return False
    
    def listen(self):
        """Listen for messages from the server"""
        try:
            print("Listening for messages... (Press Ctrl+C to quit)")
            while True:
                data = self.socket.recv(1024)
                if not data:
                    print("Server closed the connection")
                    break
                
                message = data.decode().strip()
                print(f"Received: {message}")
                
        except KeyboardInterrupt:
            print("\nDisconnecting...")
        except Exception as e:
            print(f"Error receiving data: {e}")
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Disconnect from the server"""
        if self.socket:
            self.socket.close()
            print("Disconnected from server")

def main():
    parser = argparse.ArgumentParser(description='TCP Client to test the TCP Server')
    parser.add_argument('host', help='Server host address')
    parser.add_argument('port', type=int, help='Server port number')
    
    args = parser.parse_args()
    
    client = TCPClient(args.host, args.port)
    
    if client.connect():
        client.listen()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()