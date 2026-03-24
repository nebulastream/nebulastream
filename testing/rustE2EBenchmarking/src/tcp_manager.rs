// src/tcp_manager.rs
use anyhow::Result;
use std::collections::HashMap;
use std::process::{Child, Command};
use tracing::{info, error};
use crate::config::TcpServerConfig;

pub struct TcpServerManager {
    servers: HashMap<String, TcpServerHandle>,
}

struct TcpServerHandle {
    process: Child,
    config: TcpServerConfig,
    connections_served: u32,
}

impl TcpServerManager {
    pub fn new() -> Self {
        Self {
            servers: HashMap::new(),
        }
    }

    pub async fn start_server(
        &mut self,
        name: String,
        config: TcpServerConfig,
        total_connections: u32,
    ) -> Result<()> {
        if self.servers.contains_key(&name) {
            anyhow::bail!("TCP server '{}' is already running", name);
        }

        // Build the tcp-server command
        // Todo: generalize
        let mut cmd = Command::new("/home/rudi/dima/nebulastream-public/testing/rustE2EBenchmarking/target/release/tcp-server");
        cmd.arg("--host").arg(&config.host)
            .arg("--port").arg(config.port.to_string())
            .arg("--file").arg(&config.file_path)
            .arg("--repeat").arg(config.repeat_count.to_string())
            .arg("--batch-size").arg(total_connections.to_string());

        if let Some(buffer_size) = config.buffer_size {
            cmd.arg("--buffer-size").arg(buffer_size.to_string());
        }

        info!("Starting TCP server with command: {:?}", cmd);

        let process = cmd.spawn()?;

        self.servers.insert(name.clone(), TcpServerHandle {
            process,
            config,
            connections_served: 0,
        });

        // Give server time to start
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        Ok(())
    }

    pub async fn stop_server(&mut self, name: &str) -> Result<()> {
        if let Some(mut handle) = self.servers.remove(name) {
            info!("Stopping TCP server '{}'", name);
            handle.process.kill()?;
            handle.process.wait()?;
        }
        Ok(())
    }

    pub async fn stop_all(&mut self) -> Result<()> {
        let server_names: Vec<String> = self.servers.keys().cloned().collect();
        for name in server_names {
            self.stop_server(&name).await?;
        }
        Ok(())
    }

    pub async fn get_stats(&self) -> Result<serde_json::Value> {
        let mut stats = serde_json::json!({});
        for (name, handle) in &self.servers {
            stats[name] = serde_json::json!({
                "connections_served": handle.connections_served,
                "host": handle.config.host,
                "port": handle.config.port,
            });
        }
        Ok(stats)
    }
}

impl Drop for TcpServerManager {
    fn drop(&mut self) {
        for (name, mut handle) in self.servers.drain() {
            if let Err(e) = handle.process.kill() {
                error!("Failed to kill TCP server '{}': {}", name, e);
            }
        }
    }
}