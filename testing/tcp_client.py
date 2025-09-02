#!/usr/bin/env python3
"""
Improved TCP Client to test the TCP Server
"""
import socket
import argparse
import sys
import time

class TCPClient:
    #def __init__(self, host='localhost', port=8080, buffer_size=65536):
    def __init__(self, host='localhost', port=8080, buffer_size=65536):
        self.host = host
        self.port = port
        self.socket = None
        self.buffer_size = buffer_size  # Larger buffer for better performance
    
    def connect(self):
        """Connect to the TCP server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # Set socket buffer sizes for better performance
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024*1024)  # 1MB receive buffer
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
            total_received = 0
            start_time = time.time()
            last_report_time = start_time
            
            while True:
                try:
                    # Use larger buffer and set a timeout
                    self.socket.settimeout(10.0)  # 10 second timeout
                    data = self.socket.recv(self.buffer_size)
                    
                    if not data:
                        print("Server closed the connection")
                        break
                    
                    total_received += len(data)
                    
                    # Report progress every second
                    current_time = time.time()
                    if current_time - last_report_time >= 1.0:
                        elapsed = current_time - start_time
                        rate_mbps = (total_received / (1024*1024)) / elapsed if elapsed > 0 else 0
                        print(f"Received: {total_received:,} bytes ({total_received/(1024*1024):.1f} MB) - Rate: {rate_mbps:.2f} MB/s")
                        last_report_time = current_time
                
                except socket.timeout:
                    print("Socket timeout - no data received for 10 seconds")
                    break
                except socket.error as e:
                    print(f"Socket error: {e}")
                    break
            
            end_time = time.time()
            elapsed = end_time - start_time
            final_rate = (total_received / (1024*1024)) / elapsed if elapsed > 0 else 0
            
            print(f"\nFinal stats:")
            print(f"Total received: {total_received:,} bytes ({total_received/(1024*1024):.1f} MB)")
            print(f"Time elapsed: {elapsed:.2f} seconds")
            print(f"Average rate: {final_rate:.2f} MB/s")
                
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
    parser.add_argument('--buffer-size', type=int, default=65536, 
                       help='Receive buffer size in bytes (default: 65536)')
    
    args = parser.parse_args()
    
    client = TCPClient(args.host, args.port, args.buffer_size)
    
    if client.connect():
        client.listen()
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()