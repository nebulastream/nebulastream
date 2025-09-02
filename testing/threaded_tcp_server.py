#!/usr/bin/env python3
"""
TCP Server that sends a configurable string at specified intervals
"""
import socket
import threading
import time
import argparse
import sys

class TCPServer:
    def __init__(self, host='localhost', port=0, message="Hello from TCP Server!", interval=1.0):
        self.host = host
        self.port = port
        self.message = message
        self.interval = interval
        self.clients = []
        self.running = False
        self.server_socket = None
        
    def start(self):
        """Start the TCP server"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            
            # Get the actual address and port
            actual_host, actual_port = self.server_socket.getsockname()
            print(f"TCP Server started on {actual_host}:{actual_port}")
            print(f"Message: '{self.message}'")
            print(f"Send interval: {self.interval} seconds")
            print("Waiting for connections...")
            
            self.server_socket.listen(5)
            self.running = True
            
            # Start the message broadcaster thread
            broadcast_thread = threading.Thread(target=self._broadcast_messages, daemon=True)
            broadcast_thread.start()
            
            # Accept connections
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    print(f"New connection from {client_address}")
                    
                    # Add client to list
                    self.clients.append(client_socket)
                    
                    # Handle client in separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client, 
                        args=(client_socket, client_address),
                        daemon=True
                    )
                    client_thread.start()
                    
                except socket.error:
                    if self.running:
                        print("Error accepting connection")
                    break
                    
        except Exception as e:
            print(f"Error starting server: {e}")
            sys.exit(1)
    
    def _handle_client(self, client_socket, client_address):
        """Handle individual client connection"""
        try:
            while self.running:
                # Keep connection alive - just wait for client to disconnect
                data = client_socket.recv(1024)
                if not data:
                    break
        except:
            pass
        finally:
            print(f"Client {client_address} disconnected")
            if client_socket in self.clients:
                self.clients.remove(client_socket)
            client_socket.close()
    
    def _broadcast_messages(self):
        """Broadcast messages to all connected clients at specified intervals"""
        sequence_number = 1
        while self.running:
            if self.clients:
                now = int(time.time_ns())
                sequence_number += 1
                disconnected_clients = []

                client_id = 1
                for client in self.clients:
                    try:
                        message_with_timestamp = f"{sequence_number},{now},{client_id},18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615\n"
                        client.send(message_with_timestamp.encode())
                        client_id += 1
                    except:
                        disconnected_clients.append(client)
                
                # Remove disconnected clients
                for client in disconnected_clients:
                    if client in self.clients:
                        self.clients.remove(client)
                    client.close()
            
            time.sleep(self.interval)
    
    def stop(self):
        """Stop the TCP server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        for client in self.clients:
            client.close()
        print("Server stopped")

def main():
    parser = argparse.ArgumentParser(description='TCP Server that sends periodic messages')
    parser.add_argument('--host', default='localhost', help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=0, help='Port to bind to (default: 0 for auto-assign)')
    parser.add_argument('--message', default='Hello from TCP Server!', help='Message to send')
    parser.add_argument('--interval', type=float, default=1.0, help='Send interval in seconds (default: 1.0)')
    
    args = parser.parse_args()
    
    server = TCPServer(args.host, args.port, args.message, args.interval)
    
    try:
        server.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.stop()

if __name__ == "__main__":
    main()
