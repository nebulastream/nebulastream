// src/runner.rs
use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration, Instant};
use tracing::{info, warn, error};
use chrono::Local;
use std::fs;
use std::path::PathBuf;

use crate::config::{BenchmarkConfig, BenchmarkStep};
use crate::tcp_manager::TcpServerManager;
use crate::query_manager::QueryManager;
use crate::system_manager::SystemManager;

pub struct BenchmarkRunner {
    config: BenchmarkConfig,
    tcp_manager: Arc<RwLock<TcpServerManager>>,
    query_manager: Arc<QueryManager>,
    system_manager: Arc<SystemManager>,
    start_time: Option<Instant>,
    metrics: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl BenchmarkRunner {
    pub fn new(config: BenchmarkConfig) -> Self {
        let tcp_manager = Arc::new(RwLock::new(TcpServerManager::new()));
        let query_manager = Arc::new(QueryManager::new(
            config.system.working_directory.clone(),
        ));
        let system_manager = Arc::new(SystemManager::new(
            config.system.binary_path.clone(),
            config.system.working_directory.clone(),
            config.system.parameters.clone(),
        ));

        Self {
            config,
            tcp_manager,
            query_manager,
            system_manager,
            start_time: None,
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) -> Result<()> {
        info!("Starting benchmark: {}", self.config.name);
        if let Some(desc) = &self.config.description {
            info!("Description: {}", desc);
        }

        // Create output directories
        self.setup_directories().await?;

        // Initialize start time
        let start_time = Instant::now();

        // Execute benchmark sequence
        for (idx, step) in self.config.benchmark_sequence.iter().enumerate() {
            info!("Executing step {}/{}: {:?}",
                idx + 1, self.config.benchmark_sequence.len(), step);

            if let Err(e) = self.execute_step(step).await {
                error!("Step {} failed: {}", idx + 1, e);

                // Cleanup on failure
                self.cleanup().await?;
                return Err(e);
            }
        }

        let total_duration = start_time.elapsed();
        info!("Benchmark completed in {:.2}s", total_duration.as_secs_f64());

        // Save metrics if configured
        if let Some(metrics_file) = &self.config.output.metrics_file {
            self.save_metrics(metrics_file).await?;
        }

        // Cleanup
        self.cleanup().await?;

        Ok(())
    }

    async fn execute_step(&self, step: &BenchmarkStep) -> Result<()> {
        match step {
            BenchmarkStep::StartSystem => {
                info!("Starting NES system...");
                self.system_manager.start().await?;

                if let Some(delay) = self.config.system.startup_delay_ms {
                    info!("Waiting {}ms for system startup...", delay);
                    sleep(Duration::from_millis(delay)).await;
                }
            }

            BenchmarkStep::StopSystem => {
                info!("Stopping NES system...");

                if let Some(delay) = self.config.system.shutdown_delay_ms {
                    info!("Waiting {}ms before shutdown...", delay);
                    sleep(Duration::from_millis(delay)).await;
                }

                self.system_manager.stop().await?;
            }

            BenchmarkStep::StartTcpServers { servers } => {
                for server_name in servers {
                    let server_config = self.config.tcp_servers.iter()
                        .find(|s| &s.name == server_name)
                        .ok_or_else(|| anyhow::anyhow!("TCP server '{}' not found", server_name))?;

                    // Calculate total connections needed based on pending queries
                    let total_connections = self.calculate_connections_for_server(server_name).await?;

                    info!("Starting TCP server '{}' on {}:{} (expecting {} connections)",
                        server_name, server_config.host, server_config.port, total_connections);

                    let mut manager = self.tcp_manager.write().await;
                    manager.start_server(
                        server_name.clone(),
                        server_config.clone(),
                        total_connections,
                    ).await?;
                }
            }

            BenchmarkStep::StopTcpServers { servers } => {
                let mut manager = self.tcp_manager.write().await;
                for server_name in servers {
                    info!("Stopping TCP server '{}'", server_name);
                    manager.stop_server(server_name).await?;
                }
            }

            BenchmarkStep::SubmitQuery { query_id, count, delay_between_ms } => {
                let query_def = self.config.queries.iter()
                    .find(|q| &q.id == query_id)
                    .ok_or_else(|| anyhow::anyhow!("Query '{}' not found", query_id))?;

                info!("Submitting query '{}' {} times", query_id, count);

                for i in 1..=*count {
                    let output_path = self.get_query_output_path(query_id, i);

                    info!("Got output path");
                    self.query_manager.submit_query(
                        query_def.clone(),
                        i,
                        output_path,
                    ).await?;
                    info!("submitted query");
                    if i < *count {
                        if let Some(delay) = delay_between_ms {
                            sleep(Duration::from_millis(*delay)).await;
                        }
                        info!("waited: {}", i);
                    }
                }
            }

            BenchmarkStep::SubmitParallel { queries } => {
                info!("Submitting {} queries in parallel", queries.len());

                let mut tasks = Vec::new();
                for pq in queries {
                    let query_def = self.config.queries.iter()
                        .find(|q| q.id == pq.query_id)
                        .ok_or_else(|| anyhow::anyhow!("Query '{}' not found", pq.query_id))?;

                    for i in 1..=pq.count {
                        let output_path = self.get_query_output_path(&pq.query_id, i);
                        let query_manager = Arc::clone(&self.query_manager);
                        let query_def = query_def.clone();

                        let task = tokio::spawn(async move {
                            query_manager.submit_query(
                                query_def,
                                i,
                                output_path,
                            ).await
                        });

                        tasks.push(task);
                    }
                }

                // Wait for all parallel submissions to complete
                for task in tasks {
                    task.await??;
                }
            }

            BenchmarkStep::Wait { duration_ms } => {
                info!("Waiting for {}ms", duration_ms);
                sleep(Duration::from_millis(*duration_ms)).await;
            }

            BenchmarkStep::WaitForCompletion { timeout_ms } => {
                info!("Waiting for queries to complete...");

                let timeout = timeout_ms.map(Duration::from_millis);
                let start = Instant::now();

                loop {
                    if self.query_manager.all_queries_completed().await? {
                        info!("All queries completed");
                        break;
                    }

                    if let Some(timeout) = timeout {
                        if start.elapsed() > timeout {
                            warn!("Timeout reached while waiting for completion");
                            break;
                        }
                    }

                    sleep(Duration::from_secs(1)).await;
                }
            }

            BenchmarkStep::CollectMetrics { name } => {
                info!("Collecting metrics: {}", name);
                let metrics = self.collect_current_metrics().await?;

                let mut all_metrics = self.metrics.write().await;
                all_metrics.insert(name.clone(), metrics);
            }
        }

        Ok(())
    }

    async fn calculate_connections_for_server(&self, server_name: &str) -> Result<u32> {
        // Look ahead in the benchmark sequence to calculate total connections needed
        let mut total_connections = 0u32;
        let mut server_started = false;

        for step in &self.config.benchmark_sequence {
            match step {
                BenchmarkStep::StartTcpServers { servers } if servers.contains(&server_name.to_string()) => {
                    server_started = true;
                    total_connections = 0; // Reset count when server starts
                }

                BenchmarkStep::StopTcpServers { servers } if servers.contains(&server_name.to_string()) => {
                    if server_started {
                        break; // Stop counting when server stops
                    }
                }

                BenchmarkStep::SubmitQuery { query_id, count, .. } if server_started => {
                    if let Some(query) = self.config.queries.iter().find(|q| &q.id == query_id) {
                        if query.tcp_sources.contains(&server_name.to_string()) {
                            total_connections += count;
                        }
                    }
                }

                BenchmarkStep::SubmitParallel { queries } if server_started => {
                    for pq in queries {
                        if let Some(query) = self.config.queries.iter().find(|q| q.id == pq.query_id) {
                            if query.tcp_sources.contains(&server_name.to_string()) {
                                total_connections += pq.count;
                            }
                        }
                    }
                }

                _ => {}
            }
        }

        Ok(total_connections)
    }

    async fn setup_directories(&self) -> Result<()> {
        fs::create_dir_all(&self.config.output.results_dir)?;
        fs::create_dir_all(&self.config.output.log_dir)?;

        // Create timestamped subdirectory for this run
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        let run_dir = self.config.output.results_dir.join(format!("run_{}", timestamp));
        fs::create_dir_all(&run_dir)?;

        Ok(())
    }

    fn get_query_output_path(&self, query_id: &str, instance: u32) -> PathBuf {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S");
        self.config.output.results_dir
            .join(format!("{}_{}_{}_{}.csv",
                          self.config.name.replace(' ', "_"),
                          query_id,
                          instance,
                          timestamp
            ))
    }

    async fn collect_current_metrics(&self) -> Result<serde_json::Value> {
        // Collect various metrics
        let tcp_stats = self.tcp_manager.read().await.get_stats().await?;
        let query_stats = self.query_manager.get_stats().await?;

        Ok(serde_json::json!({
            "tcp_stats": tcp_stats,
            "query_stats": query_stats,
            "timestamp": Local::now().to_rfc3339(),
        }))
    }

    async fn save_metrics(&self, path: &PathBuf) -> Result<()> {
        let metrics = self.metrics.read().await;
        let json = serde_json::to_string_pretty(&*metrics)?;
        fs::write(path, json)?;
        info!("Metrics saved to {:?}", path);
        Ok(())
    }

    async fn cleanup(&self) -> Result<()> {
        info!("Cleaning up...");

        // Stop all TCP servers
        let mut tcp_manager = self.tcp_manager.write().await;
        tcp_manager.stop_all().await?;

        // Stop system if running
        if self.system_manager.is_running().await {
            self.system_manager.stop().await?;
        }

        Ok(())
    }
}