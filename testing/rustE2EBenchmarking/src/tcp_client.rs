use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use tokio::time::{Duration, Instant, interval};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use clap::Parser;
use anyhow::Result;

#[derive(Parser)]
#[command(name = "tcp-client")]
#[command(about = "Multi-client TCP benchmark tool")]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    #[arg(short, long, default_value_t = 1)]
    clients: u32,

    #[arg(long, default_value_t = 30)]
    duration: u64,

    #[arg(long, default_value_t = 65536)]
    buffer_size: usize,

    #[arg(long, default_value_t = 10)]
    timeout: u64,
}

struct ClientStats {
    total_bytes: AtomicU64,
    total_connections: AtomicU64,
    active_connections: AtomicU64,
    failed_connections: AtomicU64,
}

impl ClientStats {
    fn new() -> Self {
        Self {
            total_bytes: AtomicU64::new(0),
            total_connections: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            failed_connections: AtomicU64::new(0),
        }
    }

    fn add_bytes(&self, bytes: u64) {
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn connection_started(&self) {
        self.total_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    fn connection_ended(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn connection_failed(&self) {
        self.failed_connections.fetch_add(1, Ordering::Relaxed);
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    fn get_stats(&self) -> (u64, u64, u64, u64) {
        (
            self.total_bytes.load(Ordering::Relaxed),
            self.total_connections.load(Ordering::Relaxed),
            self.active_connections.load(Ordering::Relaxed),
            self.failed_connections.load(Ordering::Relaxed),
        )
    }
}

struct BenchmarkClient {
    host: String,
    port: u16,
    buffer_size: usize,
    timeout: Duration,
    stats: Arc<ClientStats>,
}

impl BenchmarkClient {
    fn new(host: String, port: u16, buffer_size: usize, timeout: u64, stats: Arc<ClientStats>) -> Self {
        Self {
            host,
            port,
            buffer_size,
            timeout: Duration::from_secs(timeout),
            stats,
        }
    }

    async fn run_client(&self, client_id: u32) -> Result<()> {
        let mut buffer = vec![0u8; self.buffer_size];

        loop {
            self.stats.connection_started();

            match tokio::time::timeout(
                self.timeout,
                TcpStream::connect(format!("{}:{}", self.host, self.port))
            ).await {
                Ok(Ok(mut stream)) => {
                    // Set TCP_NODELAY for lower latency
                    if let Err(e) = stream.set_nodelay(true) {
                        eprintln!("Client {}: Failed to set TCP_NODELAY: {}", client_id, e);
                    }

                    let mut client_bytes = 0u64;
                    let client_start = Instant::now();

                    loop {
                        match tokio::time::timeout(self.timeout, stream.read(&mut buffer)).await {
                            Ok(Ok(bytes_read)) => {
                                if bytes_read == 0 {
                                    // Server closed connection
                                    break;
                                }

                                client_bytes += bytes_read as u64;
                                self.stats.add_bytes(bytes_read as u64);
                            }
                            Ok(Err(e)) => {
                                eprintln!("Client {}: Read error: {}", client_id, e);
                                break;
                            }
                            Err(_) => {
                                eprintln!("Client {}: Read timeout", client_id);
                                break;
                            }
                        }
                    }

                    let client_duration = client_start.elapsed();
                    let client_throughput = (client_bytes as f64) / (1024.0 * 1024.0) / client_duration.as_secs_f64();

                    println!(
                        "Client {} session ended: {:.1} MB in {:.2}s ({:.2} MB/s)",
                        client_id,
                        client_bytes as f64 / (1024.0 * 1024.0),
                        client_duration.as_secs_f64(),
                        client_throughput
                    );

                    self.stats.connection_ended();
                }
                Ok(Err(e)) => {
                    eprintln!("Client {}: Connection failed: {}", client_id, e);
                    self.stats.connection_failed();
                }
                Err(_) => {
                    eprintln!("Client {}: Connection timeout", client_id);
                    self.stats.connection_failed();
                }
            }

            // Small delay before reconnecting
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn print_stats(stats: Arc<ClientStats>, duration: Duration) {
    let mut interval = interval(Duration::from_secs(1));
    let start_time = Instant::now();
    let mut last_bytes = 0u64;
    let mut last_time = start_time;

    loop {
        interval.tick().await;

        let elapsed = start_time.elapsed();
        if elapsed >= duration {
            break;
        }

        let current_time = Instant::now();
        let (total_bytes, total_conn, active_conn, failed_conn) = stats.get_stats();

        // Calculate instantaneous throughput
        let time_diff = current_time.duration_since(last_time).as_secs_f64();
        let bytes_diff = total_bytes - last_bytes;
        let instant_throughput = if time_diff > 0.0 {
            (bytes_diff as f64) / (1024.0 * 1024.0) / time_diff
        } else {
            0.0
        };

        // Calculate average throughput
        let avg_throughput = (total_bytes as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64();

        println!(
            "[{:3.0}s] Total: {:.1} MB | Avg: {:.2} MB/s | Inst: {:.2} MB/s | Connections: {} active, {} total, {} failed",
            elapsed.as_secs_f64(),
            total_bytes as f64 / (1024.0 * 1024.0),
            avg_throughput,
            instant_throughput,
            active_conn,
            total_conn,
            failed_conn
        );

        last_bytes = total_bytes;
        last_time = current_time;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("Starting TCP benchmark:");
    println!("  Target: {}:{}", args.host, args.port);
    println!("  Clients: {}", args.clients);
    println!("  Duration: {}s", args.duration);
    println!("  Buffer size: {} bytes", args.buffer_size);
    println!("  Timeout: {}s", args.timeout);
    println!();

    let stats = Arc::new(ClientStats::new());
    let mut tasks = Vec::new();

    // Start stats reporting task
    let stats_clone = Arc::clone(&stats);
    let duration = Duration::from_secs(args.duration);
    let stats_task = tokio::spawn(async move {
        print_stats(stats_clone, duration).await;
    });

    // Start client tasks
    for client_id in 0..args.clients {
        let client = BenchmarkClient::new(
            args.host.clone(),
            args.port,
            args.buffer_size,
            args.timeout,
            Arc::clone(&stats),
        );

        let task = tokio::spawn(async move {
            if let Err(e) = client.run_client(client_id).await {
                eprintln!("Client {} error: {}", client_id, e);
            }
        });

        tasks.push(task);
    }

    // Wait for the benchmark duration
    tokio::time::sleep(duration).await;

    // Wait for stats task to complete
    let _ = stats_task.await;

    // Print final stats
    let (total_bytes, total_conn, active_conn, failed_conn) = stats.get_stats();
    let final_throughput = (total_bytes as f64) / (1024.0 * 1024.0) / args.duration as f64;

    println!("\n=== Final Results ===");
    println!("Total data received: {:.1} MB", total_bytes as f64 / (1024.0 * 1024.0));
    println!("Total connections: {}", total_conn);
    println!("Active connections: {}", active_conn);
    println!("Failed connections: {}", failed_conn);
    println!("Average throughput: {:.2} MB/s", final_throughput);
    println!("Success rate: {:.1}%",
             if total_conn > 0 {
                 ((total_conn - failed_conn) as f64 / total_conn as f64) * 100.0
             } else {
                 0.0
             }
    );

    // Cancel all client tasks
    for task in tasks {
        task.abort();
    }

    Ok(())
}