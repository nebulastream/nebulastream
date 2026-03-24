// src/system_manager.rs
use anyhow::Result;
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

pub struct SystemManager {
    binary_path: PathBuf,
    working_dir: PathBuf,
    parameters: HashMap<String, String>,
    process: Arc<RwLock<Option<Child>>>,
}

impl SystemManager {
    pub fn new(
        binary_path: PathBuf,
        working_dir: PathBuf,
        parameters: HashMap<String, String>,
    ) -> Self {
        Self {
            binary_path,
            working_dir,
            parameters,
            process: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn start(&self) -> Result<()> {
        let mut process_lock = self.process.write().await;

        if process_lock.is_some() {
            anyhow::bail!("System is already running");
        }

        let mut cmd = Command::new(&self.binary_path);
        cmd.current_dir(&self.working_dir);

        // Add all parameters as command-line arguments
        // Format: --parameter.name=value (no spaces)
        for (key, value) in &self.parameters {
            cmd.arg(format!("--{}={}", key, value));
        }

        info!("Starting system with command: {:?}", cmd);

        let process = cmd.spawn()?;
        *process_lock = Some(process);

        info!("System started successfully");
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        let mut process_lock = self.process.write().await;

        if let Some(mut process) = process_lock.take() {
            info!("Stopping system...");

            // Try graceful shutdown first
            #[cfg(unix)]
            {
                use nix::sys::signal::{self, Signal};
                use nix::unistd::Pid;

                let pid = process.id();
                let _ = signal::kill(Pid::from_raw(pid as i32), Signal::SIGTERM);

                // Wait a bit for graceful shutdown
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }

            // Force kill if still running
            match process.try_wait()? {
                Some(_) => info!("System stopped gracefully"),
                None => {
                    warn!("System didn't stop gracefully, forcing termination");
                    process.kill()?;
                    process.wait()?;
                    info!("System forcefully terminated");
                }
            }
        } else {
            warn!("System is not running");
        }

        Ok(())
    }

    pub async fn is_running(&self) -> bool {
        let process_lock = self.process.read().await;
        process_lock.is_some()
    }
}

impl Drop for SystemManager {
    fn drop(&mut self) {
        // Try to stop the system on drop
        let process = self.process.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let mut process_lock = process.write().await;
                if let Some(mut proc) = process_lock.take() {
                    let _ = proc.kill();
                }
            });
        });
    }
}