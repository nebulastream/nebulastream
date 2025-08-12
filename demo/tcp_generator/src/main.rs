use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncWriteExt, BufWriter};
use rand::Rng;
use std::time::Duration;
use tokio::time::sleep;
use csv::WriterBuilder;
use std::io::Cursor;
use tokio::signal;

const HOST: &str = "0.0.0.0";
const PORT: u16 = 9999;

async fn handle_client(mut socket: TcpStream, client_address: std::net::SocketAddr) {
    println!("[+] Accepted connection from {}", client_address);
    let mut monotonic_counter: u64 = 0;

    let mut writer = BufWriter::new(&mut socket);

    // Initial row
    let mut wtr = WriterBuilder::new()
        .quote_style(csv::QuoteStyle::Never)
        .escape(b'\\')
        .from_writer(vec![]);
    wtr.write_record(&[
        monotonic_counter.to_string(),
        monotonic_counter.to_string(),
        monotonic_counter.to_string(),
    ])
    .unwrap();
    let csv_line = String::from_utf8(wtr.into_inner().unwrap()).unwrap();
    if let Err(e) = writer.write_all(csv_line.as_bytes()).await {
        eprintln!("[-] Client {} disconnected: {}", client_address, e);
        return;
    }

    for _i in 0..2 {
        monotonic_counter += 1000;
        let random_val = rand::thread_rng().gen_range(0.0..100.0);

        let mut wtr = WriterBuilder::new()
            .quote_style(csv::QuoteStyle::Never)
            .escape(b'\\')
            .from_writer(vec![]);
        wtr.write_record(&[
            monotonic_counter.to_string(),
            format!("{:.4}", random_val),
            monotonic_counter.to_string(),
        ])
        .unwrap();
        let csv_line = String::from_utf8(wtr.into_inner().unwrap()).unwrap();

        if let Err(e) = writer.write_all(csv_line.as_bytes()).await {
            eprintln!("[-] Client {} disconnected: {}", client_address, e);
            break;
        }
        if let Err(e) = writer.flush().await {
            eprintln!("[-] Client {} disconnected during flush: {}", client_address, e);
            break;
        }
    }
    sleep(Duration::from_secs(1)).await;
    println!("[*] Closing connection to {}", client_address);
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(format!("{}:{}", HOST, PORT)).await?;
    println!("[*] Listening on {}:{}", HOST, PORT);

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((socket, addr)) => {
                        tokio::spawn(async move {
                            handle_client(socket, addr).await;
                        });
                    }
                    Err(e) => {
                        eprintln!("[!] Server accept error: {}", e);
                        // If the listener encounters an error, it might be unrecoverable.
                        // Consider breaking the loop or handling specific errors.
                        break;
                    }
                }
            }
            _ = signal::ctrl_c() => {
                println!("[*] Received Ctrl-C, shutting down server...");
                break; // Exit the loop on Ctrl-C
            }
        }
    }

    println!("[*] Closing server socket.");
    Ok(())
}