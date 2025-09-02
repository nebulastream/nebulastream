#!/usr/bin/env python3
"""
Async TCP Server that sends a configurable string at specified intervals using asyncio
"""
import asyncio
import time
import argparse
import sys
import logging

class AsyncTCPServer:
    def __init__(self, host='localhost', port=0, message="Hello from TCP Server!", interval=1.0):
        self.host = host
        self.port = port
        self.message = message
        self.interval = interval
        self.clients = set()  # Using set for O(1) removal
        self.running = False
        self.server = None
        
    async def start(self):
        """Start the async TCP server"""
        try:
            self.server = await asyncio.start_server(
                self._handle_client, 
                self.host, 
                self.port
            )
            
            # Get the actual address and port
            addr = self.server.sockets[0].getsockname()
            print(f"Async TCP Server started on {addr[0]}:{addr[1]}")
            print(f"Message: '{self.message}'")
            print(f"Send interval: {self.interval} seconds")
            print("Waiting for connections...")
            
            self.running = True
            
            # Start the message broadcaster task
            broadcast_task = asyncio.create_task(self._broadcast_messages())
            
            # Start serving
            async with self.server:
                await self.server.serve_forever()
                
        except Exception as e:
            print(f"Error starting server: {e}")
            sys.exit(1)
    
    async def _handle_client(self, reader, writer):
        """Handle individual client connection"""
        client_address = writer.get_extra_info('peername')
        print(f"New connection #{len(self.clients) + 1} from {client_address}")
        
        # Add client to set
        self.clients.add(writer)
        
        try:
            while self.running:
                # Keep connection alive - wait for client to disconnect or send data
                try:
                    # Set a timeout to check periodically if server is still running
                    data = await asyncio.wait_for(reader.read(1024), timeout=1.0)
                    if not data:
                        break  # Client disconnected
                except asyncio.TimeoutError:
                    # Timeout is normal, just continue the loop
                    continue
                except Exception:
                    # Any other error means client disconnected
                    break
                    
        except Exception as e:
            pass
        finally:
            print(f"Client {client_address} disconnected")
            self.clients.discard(writer)  # discard won't raise if not present
            try:
                writer.close()
                await writer.wait_closed()
            except:
                pass
    
    async def _broadcast_messages(self):
        """Broadcast messages to all connected clients at specified intervals"""
        sequence_number = 1
        while self.running:
            if self.clients:
                # Create all send tasks concurrently
                send_tasks = []
                clients_to_remove = set()

                client_id = 1
                now = int(time.time_ns())
                for client in list(self.clients):  # Create a copy to avoid modification during iteration
                    if client.is_closing():
                        clients_to_remove.add(client)
                        continue
                    message_with_timestamp = f"{sequence_number},{now},{client_id},18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615,18446744073709551615\n"
                    message_bytes = message_with_timestamp.encode()
                    client_id += 1
                    # Create a task for each client send
                    task = asyncio.create_task(self._send_to_client(client, message_bytes))
                    send_tasks.append((task, client))
                sequence_number += 1
                
                # Remove closed clients
                self.clients -= clients_to_remove
                
                # Wait for all sends to complete (with timeout)
                if send_tasks:
                    await self._wait_for_sends(send_tasks)
            
            await asyncio.sleep(self.interval)
    
    async def _send_to_client(self, writer, message_bytes):
        """Send message to a single client"""
        try:
            writer.write(message_bytes)
            await writer.drain()
            return True
        except Exception:
            return False
    
    async def _wait_for_sends(self, send_tasks):
        """Wait for all send tasks to complete and handle failures"""
        clients_to_remove = set()
        
        # Wait for all tasks with a reasonable timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*[task for task, _ in send_tasks], return_exceptions=True),
                timeout=0.1  # 100ms timeout for all sends
            )
            
            # Check results and mark failed clients for removal
            for i, (result, (task, client)) in enumerate(zip(results, send_tasks)):
                if isinstance(result, Exception) or result is False:
                    clients_to_remove.add(client)
                    
        except asyncio.TimeoutError:
            # Some sends timed out, mark slow clients for removal
            for task, client in send_tasks:
                if not task.done():
                    clients_to_remove.add(client)
                    task.cancel()
        
        # Remove failed clients
        for client in clients_to_remove:
            self.clients.discard(client)
            try:
                client.close()
            except:
                pass
    
    async def stop(self):
        """Stop the TCP server"""
        self.running = False
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            
        # Close all client connections
        close_tasks = []
        for client in list(self.clients):
            try:
                client.close()
                close_tasks.append(client.wait_closed())
            except:
                pass
        
        if close_tasks:
            await asyncio.gather(*close_tasks, return_exceptions=True)
            
        self.clients.clear()
        print("Async server stopped")

async def main():
    parser = argparse.ArgumentParser(description='Async TCP Server that sends periodic messages')
    parser.add_argument('--host', default='localhost', help='Host to bind to (default: localhost)')
    parser.add_argument('--port', type=int, default=0, help='Port to bind to (default: 0 for auto-assign)')
    parser.add_argument('--message', default='Hello from TCP Server!', help='Message to send')
    parser.add_argument('--interval', type=float, default=1, help='Send interval in seconds (default: 0.1 for 10 msgs/sec)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
    
    server = AsyncTCPServer(args.host, args.port, args.message, args.interval)
    
    try:
        await server.start()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        await server.stop()

if __name__ == "__main__":
    asyncio.run(main())