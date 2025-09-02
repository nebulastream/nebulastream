use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;
use tokio::fs;
use tokio::sync::{Barrier, Mutex};
use std::sync::Arc;
use std::time::Instant;
use clap::Parser;
use anyhow::Result;

#[derive(Parser)]
#[command(name = "tcp-server")]
#[command(about = "High-performance TCP server for throughput testing")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,
    
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
    
    #[arg(short, long, default_value = "test_data.bin")]
    file: String,
    
    #[arg(long, default_value_t = 1)]
    repeat: u32,
    
    #[arg(long, default_value_t = 65536)]
    buffer_size: usize,
    
    #[arg(short = 'n', long, default_value_t = 1)]
    wait_for_clients: usize,
}

struct Server {
    data: Arc<Vec<u8>>,
    repeat_count: u32,
    buffer_size: usize,
    barrier: Arc<Barrier>,
    connected_clients: Arc<Mutex<usize>>,
    wait_for_clients: usize,
}

impl Server {
    async fn new(file_path: &str, repeat_count: u32, buffer_size: usize, wait_for_clients: usize) -> Result<Self> {
        let data = match fs::read(file_path).await {
            Ok(data) => {
                println!("Loaded file: {} ({} bytes)", file_path, data.len());
                data
            }
            Err(_) => {
                println!("File not found, generating {} MB of test data", buffer_size / (1024 * 1024));
                // Generate test data if file doesn't exist
                (0..buffer_size).map(|i| (i % 256) as u8).collect()
            }
        };
        
        Ok(Self {
            data: Arc::new(data),
            repeat_count,
            buffer_size,
            barrier: Arc::new(Barrier::new(wait_for_clients)),
            connected_clients: Arc::new(Mutex::new(0)),
            wait_for_clients,
        })
    }
    
    async fn handle_client(&self, mut stream: TcpStream, client_id: u64) -> Result<()> {
        // Set TCP_NODELAY for lower latency
        stream.set_nodelay(true)?;
        
        println!("Client {} waiting for all clients to connect...", client_id);
        
        // Wait for all clients to connect before starting
        self.barrier.wait().await;
        
        println!("Client {} starting data transmission", client_id);
        let start_time = Instant::now();
        let mut total_sent = 0u64;
        
        // Send data repeatedly
        for _ in 0..self.repeat_count {
            let mut offset = 0;
            while offset < self.data.len() {
                let end = std::cmp::min(offset + self.buffer_size, self.data.len());
                let chunk = &self.data[offset..end];
                
                match stream.write_all(chunk).await {
                    Ok(()) => {
                        total_sent += chunk.len() as u64;
                        offset = end;
                    }
                    Err(e) => {
                        eprintln!("Client {}: Write error: {}", client_id, e);
                        return Err(e.into());
                    }
                }
            }
        }
        
        // Flush any remaining data
        stream.flush().await?;
        
        let duration = start_time.elapsed();
        let throughput_mbps = (total_sent as f64) / (1024.0 * 1024.0) / duration.as_secs_f64();
        
        println!(
            "Client {} completed: {} MB in {:.2}s ({:.2} MB/s)",
            client_id,
            total_sent / (1024 * 1024),
            duration.as_secs_f64(),
            throughput_mbps
        );
        
        Ok(())
    }
    
    async fn run(&self, host: &str, port: u16) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", host, port)).await?;
        println!("Server listening on {}:{}", host, port);
        println!("Data size: {} bytes, repeat: {} times", self.data.len(), self.repeat_count);
        println!("Waiting for {} clients to connect before starting transmission", self.wait_for_clients);
        
        let mut client_counter = 0u64;
        
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    client_counter += 1;
                    let client_id = client_counter;
                    
                    {
                        let mut connected = self.connected_clients.lock().await;
                        *connected += 1;
                        println!("Client {} connected from {} ({}/{} clients connected)", 
                               client_id, addr, *connected, self.wait_for_clients);
                    }
                    
                    let server = self.clone();
                    
                    // Spawn a new task for each client
                    tokio::spawn(async move {
                        if let Err(e) = server.handle_client(stream, client_id).await {
                            eprintln!("Client {} error: {}", client_id, e);
                        }
                        
                        // Decrement connected clients count when client disconnects
                        {
                            let mut connected = server.connected_clients.lock().await;
                            *connected -= 1;
                        }
                    });
                    
                    // If we've reached the target number of clients, all subsequent clients
                    // will still use the same barrier, so they'll wait for the next "batch"
                    if client_counter == self.wait_for_clients as u64 {
                        println!("All {} clients connected! Data transmission will begin shortly...", self.wait_for_clients);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}

impl Clone for Server {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            repeat_count: self.repeat_count,
            buffer_size: self.buffer_size,
            barrier: Arc::clone(&self.barrier),
            connected_clients: Arc::clone(&self.connected_clients),
            wait_for_clients: self.wait_for_clients,
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    
    let server = Server::new(&args.file, args.repeat, args.buffer_size, args.wait_for_clients).await?;
    server.run(&args.host, args.port).await?;
    
    Ok(())
}