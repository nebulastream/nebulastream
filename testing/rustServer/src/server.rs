use tokio::net::{TcpListener, TcpStream};
use tokio::io::AsyncWriteExt;
use tokio::fs;
use tokio::sync::{mpsc, oneshot};
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
    batch_size: usize,
}

#[derive(Debug)]
enum ServerMessage {
    NewClient {
        stream: TcpStream,
        addr: std::net::SocketAddr,
        response_tx: oneshot::Sender<()>,
    },
}

struct ClientHandler {
    stream: TcpStream,
    client_id: u64,
    data: Arc<Vec<u8>>,
    repeat_count: u32,
    buffer_size: usize,
}

impl ClientHandler {
    fn new(
        stream: TcpStream,
        client_id: u64,
        data: Arc<Vec<u8>>,
        repeat_count: u32,
        buffer_size: usize,
    ) -> Self {
        Self {
            stream,
            client_id,
            data,
            repeat_count,
            buffer_size,
        }
    }

    async fn serve(mut self) -> Result<()> {
        // Set TCP_NODELAY for lower latency
        self.stream.set_nodelay(true)?;

        let start_time = Instant::now();
        let mut total_sent = 0u64;

        println!("Client {} starting data transmission", self.client_id);

        // Send data repeatedly
        for _ in 0..self.repeat_count {
            let mut offset = 0;
            while offset < self.data.len() {
                let end = std::cmp::min(offset + self.buffer_size, self.data.len());
                let chunk = &self.data[offset..end];

                match self.stream.write_all(chunk).await {
                    Ok(()) => {
                        total_sent += chunk.len() as u64;
                        offset = end;
                    }
                    Err(e) => {
                        eprintln!("Client {}: Write error: {}", self.client_id, e);
                        return Err(e.into());
                    }
                }
            }
        }

        // Flush any remaining data
        self.stream.flush().await?;

        let duration = start_time.elapsed();
        let throughput_mbps = (total_sent as f64) / (1024.0 * 1024.0) / duration.as_secs_f64();

        println!(
            "Client {} completed: {} MB in {:.2}s ({:.2} MB/s)",
            self.client_id,
            total_sent / (1024 * 1024),
            duration.as_secs_f64(),
            throughput_mbps
        );

        Ok(())
    }
}

struct Server {
    data: Arc<Vec<u8>>,
    repeat_count: u32,
    buffer_size: usize,
    batch_size: usize,
}

impl Server {
    async fn new(file_path: &str, repeat_count: u32, buffer_size: usize, batch_size: usize) -> Result<Self> {
        let data = match fs::read(file_path).await {
            Ok(data) => {
                println!("Loaded file: {} ({} bytes)", file_path, data.len());
                data
            }
            Err(_) => {
                println!("File not found, generating {} bytes of test data", buffer_size);
                // Generate test data if file doesn't exist
                (0..buffer_size).map(|i| (i % 256) as u8).collect()
            }
        };

        Ok(Self {
            data: Arc::new(data),
            repeat_count,
            buffer_size,
            batch_size,
        })
    }

    async fn run(&self, host: &str, port: u16) -> Result<()> {
        let listener = TcpListener::bind(format!("{}:{}", host, port)).await?;
        println!("Server listening on {}:{}", host, port);
        println!("Data size: {} bytes, repeat: {} times", self.data.len(), self.repeat_count);
        println!("Batch size: {} clients per batch", self.batch_size);
        println!();

        let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();

        // Spawn listener task
        let listener_tx = tx.clone();
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        let (response_tx, response_rx) = oneshot::channel();

                        if listener_tx.send(ServerMessage::NewClient {
                            stream,
                            addr,
                            response_tx,
                        }).is_err() {
                            break;
                        }

                        // Wait for the main loop to process this client
                        let _ = response_rx.await;
                    }
                    Err(e) => {
                        eprintln!("Failed to accept connection: {}", e);
                    }
                }
            }
        });

        let mut batch_number = 1u32;
        let mut global_client_id = 0u64;

        loop {
            println!("=== Batch {} ===", batch_number);
            println!("Waiting for {} clients to connect...", self.batch_size);

            let mut pending_clients = Vec::new();

            // Collect N clients
            while pending_clients.len() < self.batch_size {
                if let Some(msg) = rx.recv().await {
                    match msg {
                        ServerMessage::NewClient { stream, addr, response_tx } => {
                            global_client_id += 1;
                            pending_clients.push((stream, global_client_id));

                            println!(
                                "Client {} connected from {} ({}/{} for batch {})",
                                global_client_id,
                                addr,
                                pending_clients.len(),
                                self.batch_size,
                                batch_number
                            );

                            // Signal that we've processed this client
                            let _ = response_tx.send(());
                        }
                    }
                }
            }

            println!("Batch {} ready! Starting simultaneous data transmission to {} clients...",
                     batch_number, self.batch_size);

            // Now serve all clients in this batch simultaneously
            let mut client_tasks = Vec::new();

            for (stream, client_id) in pending_clients {
                let handler = ClientHandler::new(
                    stream,
                    client_id,
                    Arc::clone(&self.data),
                    self.repeat_count,
                    self.buffer_size,
                );

                let task = tokio::spawn(async move {
                    if let Err(e) = handler.serve().await {
                        eprintln!("Client {} error: {}", client_id, e);
                    }
                });

                client_tasks.push(task);
            }

            // Wait for all clients in this batch to complete
            let batch_start = Instant::now();
            for task in client_tasks {
                let _ = task.await;
            }

            let batch_duration = batch_start.elapsed();
            println!(
                "Batch {} completed in {:.2}s\n",
                batch_number,
                batch_duration.as_secs_f64()
            );

            batch_number += 1;
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let server = Server::new(&args.file, args.repeat, args.buffer_size, args.batch_size).await?;
    server.run(&args.host, args.port).await?;

    Ok(())
}