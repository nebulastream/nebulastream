use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Barrier, Mutex};
use tokio::time::{sleep, interval};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the file to serve
    #[clap(short, long)]
    file: PathBuf,

    /// Number of clients to wait for before serving
    #[clap(short = 'n', long, default_value = "1")]
    clients: usize,

    /// Number of times to send the file to each client
    #[clap(short = 'm', long, default_value = "1")]
    iterations: usize,

    /// Data rate in MB/s
    #[clap(short, long, default_value = "125")]
    rate: f64,

    /// Sending mode: uniform or burst
    #[clap(short, long, default_value = "uniform")]
    mode: SendingMode,

    /// Host to bind to
    #[clap(long, default_value = "127.0.0.1")]
    host: String,

    /// Port to bind to
    #[clap(short, long, default_value = "1111")]
    port: u16,

    /// Chunk size in KB for sending data (affects granularity of rate limiting)
    #[clap(long, default_value = "64")]
    chunk_size_kb: usize,
}

#[derive(Debug, Clone, Copy)]
enum SendingMode {
    Uniform,
    Burst,
}

impl std::str::FromStr for SendingMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "uniform" => Ok(SendingMode::Uniform),
            "burst" => Ok(SendingMode::Burst),
            _ => Err(format!("Invalid mode: {}. Use 'uniform' or 'burst'", s)),
        }
    }
}

struct Server {
    file_data: Arc<Vec<u8>>,
    args: Arc<Args>,
    barrier: Arc<Barrier>,
}

impl Server {
    fn new(args: Args) -> Result<Self, Box<dyn std::error::Error>> {
        // Read file data
        let mut file = File::open(&args.file)?;
        let mut file_data = Vec::new();
        file.read_to_end(&mut file_data)?;

        let barrier = Arc::new(Barrier::new(args.clients + 1)); // +1 for main task

        Ok(Server {
            file_data: Arc::new(file_data),
            args: Arc::new(args),
            barrier,
        })
    }

    async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let addr: SocketAddr = format!("{}:{}", self.args.host, self.args.port).parse()?;
        let listener = TcpListener::bind(addr).await?;

        println!("Server listening on {}", addr);
        println!("Waiting for {} clients to connect...", self.args.clients);
        println!("Will serve file {} times to each client", self.args.iterations);
        println!("Total rate: {} MB/s, Per client: {:.2} MB/s",
                 self.args.rate,
                 self.args.rate / self.args.clients as f64);
        println!("Mode: {:?}", self.args.mode);

        let client_counter = Arc::new(Mutex::new(0));
        let mut client_handles = Vec::new();

        // Accept N clients
        for _ in 0..self.args.clients {
            let (stream, peer_addr) = listener.accept().await?;

            let mut counter = client_counter.lock().await;
            *counter += 1;
            let client_id = *counter;

            println!("Client {} connected from {}", client_id, peer_addr);

            let barrier_clone = Arc::clone(&self.barrier);
            let file_data = Arc::clone(&self.file_data);
            let args = Arc::clone(&self.args);

            let handle = tokio::spawn(async move {
                if let Err(e) = handle_client(
                    stream,
                    client_id,
                    barrier_clone,
                    file_data,
                    args,
                ).await {
                    eprintln!("Error handling client {}: {}", client_id, e);
                }
            });

            client_handles.push(handle);
        }


        println!("All {} clients connected. Starting file transfer...", self.args.clients);

        let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");

        println!("Start timestamp: {}", now.as_secs_f64());
        // Signal all clients to start
        self.barrier.wait().await;

        // Wait for all clients to finish
        for handle in client_handles {
            handle.await?;
        }

        println!("All clients served. Server shutting down.");
        Ok(())
    }
}

async fn handle_client(
    mut stream: TcpStream,
    client_id: usize,
    barrier: Arc<Barrier>,
    file_data: Arc<Vec<u8>>,
    args: Arc<Args>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Wait for all clients to connect
    barrier.wait().await;

    let client_rate_mbps = args.rate / args.clients as f64;
    let client_rate_bytes_per_sec = (client_rate_mbps * 1024.0 * 1024.0) as usize;
    let chunk_size = args.chunk_size_kb * 1024;

    println!("Client {}: Starting transfer ({} iterations, {:.2} MB/s)",
             client_id, args.iterations, client_rate_mbps);

    match args.mode {
        SendingMode::Uniform => {
            send_uniform(
                &mut stream,
                client_id,
                &file_data,
                args.iterations,
                client_rate_bytes_per_sec,
                chunk_size,
            ).await?;
        }
        SendingMode::Burst => {
            send_burst(
                &mut stream,
                client_id,
                &file_data,
                args.iterations,
                client_rate_bytes_per_sec,
            ).await?;
        }
    }

    stream.shutdown().await?;
    println!("Client {}: Disconnected", client_id);

    Ok(())
}

async fn send_uniform(
    stream: &mut TcpStream,
    client_id: usize,
    file_data: &[u8],
    iterations: usize,
    rate_bytes_per_sec: usize,
    chunk_size: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let chunk_duration = Duration::from_secs_f64(chunk_size as f64 / rate_bytes_per_sec as f64);
    let mut interval = interval(chunk_duration);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    for iteration in 1..=iterations {
        let start = Instant::now();
        let mut bytes_sent = 0;

        for chunk in file_data.chunks(chunk_size) {
            interval.tick().await;
            stream.write_all(chunk).await?;
            bytes_sent += chunk.len();
        }

        stream.flush().await?;

        let elapsed = start.elapsed();
        let actual_rate = (bytes_sent as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0);

        println!("Client {}: Iteration {}/{} complete ({:.2} MB/s actual)",
                 client_id, iteration, iterations, actual_rate);
    }

    Ok(())
}

async fn send_burst(
    stream: &mut TcpStream,
    client_id: usize,
    file_data: &[u8],
    iterations: usize,
    rate_bytes_per_sec: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_size = file_data.len();
    let burst_duration = Duration::from_secs_f64(file_size as f64 / rate_bytes_per_sec as f64);

    for iteration in 1..=iterations {
        let start = Instant::now();

        // Send entire file in one burst
        stream.write_all(file_data).await?;
        stream.flush().await?;

        let elapsed = start.elapsed();
        let actual_rate = (file_size as f64 / elapsed.as_secs_f64()) / (1024.0 * 1024.0);

        println!("Client {}: Iteration {}/{} burst sent ({:.2} MB/s actual)",
                 client_id, iteration, iterations, actual_rate);

        // Pause to maintain average rate
        if iteration < iterations {
            let remaining_time = burst_duration.saturating_sub(elapsed);
            if !remaining_time.is_zero() {
                println!("Client {}: Pausing for {:.2}s", client_id, remaining_time.as_secs_f64());
                sleep(remaining_time).await;
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let server = Server::new(args)?;
    server.run().await?;
    Ok(())
}