// TokioTcpSource: async TCP source with automatic reconnect.
//
// Connects to a TCP server, reads data into buffers, and emits them downstream.
// If the connection drops, it reconnects automatically with exponential backoff.
// If the initial connection cannot be established within `connect_timeout_secs`,
// the source fails with an error.

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{error, info, warn};

use nes_source_runtime::config::{ConfigParam, ParamType};
use nes_source_runtime::context::SourceContext;
use nes_source_runtime::error::{SourceError, SourceResult};
use nes_source_runtime::source::AsyncSource;

pub const CONFIG_SCHEMA: &[ConfigParam] = &[
    ConfigParam { name: "tcp_host", param_type: ParamType::String, default: None },
    ConfigParam { name: "tcp_port", param_type: ParamType::U32, default: None },
    ConfigParam { name: "connect_timeout_secs", param_type: ParamType::U64, default: Some("10") },
    ConfigParam { name: "reconnect_max_retries", param_type: ParamType::U64, default: Some("0") },
    ConfigParam { name: "reconnect_initial_delay_ms", param_type: ParamType::U64, default: Some("100") },
    ConfigParam { name: "reconnect_max_delay_ms", param_type: ParamType::U64, default: Some("5000") },
];

pub fn create_tcp_source(config: &HashMap<String, String>) -> Result<TcpSource, String> {
    let host = config.get("tcp_host")
        .ok_or("missing tcp_host")?
        .clone();
    let port = config.get("tcp_port")
        .ok_or("missing tcp_port")?
        .parse::<u32>()
        .map_err(|e| format!("invalid tcp_port: {e}"))?;
    let connect_timeout_secs = config.get("connect_timeout_secs")
        .unwrap_or(&"10".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid connect_timeout_secs: {e}"))?;
    let max_retries = config.get("reconnect_max_retries")
        .unwrap_or(&"0".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid reconnect_max_retries: {e}"))?;
    let initial_delay_ms = config.get("reconnect_initial_delay_ms")
        .unwrap_or(&"100".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid reconnect_initial_delay_ms: {e}"))?;
    let max_delay_ms = config.get("reconnect_max_delay_ms")
        .unwrap_or(&"5000".to_string())
        .parse::<u64>()
        .map_err(|e| format!("invalid reconnect_max_delay_ms: {e}"))?;

    Ok(TcpSource {
        host,
        port,
        connect_timeout: Duration::from_secs(connect_timeout_secs),
        max_retries,
        initial_delay: Duration::from_millis(initial_delay_ms),
        max_delay: Duration::from_millis(max_delay_ms),
    })
}

pub struct TcpSource {
    host: String,
    port: u32,
    connect_timeout: Duration,
    /// 0 = no reconnect (fail on first disconnect). u64::MAX = infinite retries.
    max_retries: u64,
    initial_delay: Duration,
    max_delay: Duration,
}

impl TcpSource {
    fn addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    async fn connect(&self) -> Result<TcpStream, SourceError> {
        let addr = self.addr();
        match timeout(self.connect_timeout, TcpStream::connect(&addr)).await {
            Ok(Ok(stream)) => {
                info!(addr = %addr, "TCP source connected");
                Ok(stream)
            }
            Ok(Err(e)) => Err(SourceError::new(format!(
                "failed to connect to {addr}: {e}"
            ))),
            Err(_) => Err(SourceError::new(format!(
                "connection to {addr} timed out after {}s",
                self.connect_timeout.as_secs()
            ))),
        }
    }

    async fn reconnect(&self, cancel: &tokio_util::sync::CancellationToken) -> Option<TcpStream> {
        let mut delay = self.initial_delay;
        let mut attempts = 0u64;

        loop {
            if cancel.is_cancelled() {
                return None;
            }

            if self.max_retries > 0 && attempts >= self.max_retries {
                error!(
                    addr = %self.addr(),
                    attempts = attempts,
                    "TCP source: max reconnect retries exhausted"
                );
                return None;
            }

            // Wait before reconnecting, respecting cancellation
            tokio::select! {
                _ = cancel.cancelled() => return None,
                _ = tokio::time::sleep(delay) => {}
            }

            attempts += 1;
            warn!(
                addr = %self.addr(),
                attempt = attempts,
                "TCP source: reconnecting..."
            );

            match self.connect().await {
                Ok(stream) => {
                    info!(addr = %self.addr(), attempt = attempts, "TCP source: reconnected");
                    return Some(stream);
                }
                Err(e) => {
                    warn!(addr = %self.addr(), error = %e, "TCP source: reconnect failed");
                    delay = (delay * 2).min(self.max_delay);
                }
            }
        }
    }
}

impl AsyncSource for TcpSource {
    async fn run(&mut self, ctx: &SourceContext) -> SourceResult {
        let cancel = ctx.cancellation_token();

        // Initial connection — fail hard if this doesn't work.
        let mut stream = match self.connect().await {
            Ok(s) => s,
            Err(e) => return SourceResult::Error(e),
        };

        loop {
            if cancel.is_cancelled() {
                return SourceResult::EndOfStream;
            }

            let mut buf = ctx.allocate_buffer().await;
            let slice = buf.as_mut_slice();

            let n = tokio::select! {
                _ = cancel.cancelled() => return SourceResult::EndOfStream,
                result = stream.read(slice) => {
                    match result {
                        Ok(0) => {
                            // EOF — peer closed connection
                            info!(addr = %self.addr(), "TCP source: peer closed connection (EOF)");
                            if self.max_retries == 0 {
                                return SourceResult::EndOfStream;
                            }
                            match self.reconnect(&cancel).await {
                                Some(new_stream) => {
                                    stream = new_stream;
                                    continue;
                                }
                                None if cancel.is_cancelled() => return SourceResult::EndOfStream,
                                None => return SourceResult::Error(SourceError::new(
                                    format!("TCP source: reconnect to {} failed after {} retries", self.addr(), self.max_retries)
                                )),
                            }
                        }
                        Ok(n) => n,
                        Err(e) => {
                            warn!(addr = %self.addr(), error = %e, "TCP source: read error");
                            if self.max_retries == 0 {
                                return SourceResult::Error(SourceError::new(
                                    format!("TCP read error on {}: {e}", self.addr())
                                ));
                            }
                            match self.reconnect(&cancel).await {
                                Some(new_stream) => {
                                    stream = new_stream;
                                    continue;
                                }
                                None if cancel.is_cancelled() => return SourceResult::EndOfStream,
                                None => return SourceResult::Error(SourceError::new(
                                    format!("TCP source: reconnect to {} failed after {} retries", self.addr(), self.max_retries)
                                )),
                            }
                        }
                    }
                }
            };

            // numberOfTuples = byte count for raw/CSV sources (InputFormatter convention)
            buf.set_number_of_tuples(n as u64);

            if let Err(_) = ctx.emit(buf).await {
                return SourceResult::EndOfStream;
            }
        }
    }
}
