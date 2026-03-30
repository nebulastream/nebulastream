// src/config.rs
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkConfig {
    pub name: String,
    pub description: Option<String>,
    pub system: SystemConfig,
    pub tcp_servers: Vec<TcpServerConfig>,
    pub queries: Vec<QueryDefinition>,
    pub benchmark_sequence: Vec<BenchmarkStep>,
    pub output: OutputConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemConfig {
    pub binary_path: PathBuf,
    pub working_directory: PathBuf,
    pub parameters: HashMap<String, String>,
    pub startup_delay_ms: Option<u64>,
    pub shutdown_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpServerConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub file_path: PathBuf,
    pub repeat_count: u32,
    pub buffer_size: Option<usize>,
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinition {
    pub id: String,
    pub yaml_path: PathBuf,
    pub tcp_sources: Vec<String>, // Names of TCP servers this query uses
    pub output_suffix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum BenchmarkStep {
    StartSystem,
    StopSystem,
    StartTcpServers {
        servers: Vec<String>, // Names of servers to start
    },
    StopTcpServers {
        servers: Vec<String>,
    },
    SubmitQuery {
        query_id: String,
        count: u32,
        delay_between_ms: Option<u64>,
    },
    SubmitParallel {
        queries: Vec<ParallelQuery>,
    },
    Wait {
        duration_ms: u64,
    },
    WaitForCompletion {
        timeout_ms: Option<u64>,
    },
    CollectMetrics {
        name: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelQuery {
    pub query_id: String,
    pub count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    pub results_dir: PathBuf,
    pub log_dir: PathBuf,
    pub metrics_file: Option<PathBuf>,
    pub save_query_outputs: bool,
}

impl BenchmarkConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {:?}", path.as_ref()))?;

        toml::from_str(&contents)
            .with_context(|| format!("Failed to parse TOML from: {:?}", path.as_ref()))
    }

    pub fn validate(&self) -> Result<()> {
        // Validate system binary exists
        if !self.system.binary_path.exists() {
            anyhow::bail!("System binary not found: {:?}", self.system.binary_path);
        }

        // Validate working directory exists
        if !self.system.working_directory.exists() {
            anyhow::bail!("Working directory not found: {:?}", self.system.working_directory);
        }

        // Validate TCP server files exist
        for server in &self.tcp_servers {
            if !server.file_path.exists() {
                anyhow::bail!("TCP server file not found for '{}': {:?}",
                    server.name, server.file_path);
            }
        }

        // Validate query YAML files exist
        for query in &self.queries {
            if !query.yaml_path.exists() {
                anyhow::bail!("Query YAML not found for '{}': {:?}",
                    query.id, query.yaml_path);
            }

            // Validate TCP sources reference existing servers
            for source in &query.tcp_sources {
                if !self.tcp_servers.iter().any(|s| &s.name == source) {
                    anyhow::bail!("Query '{}' references unknown TCP server: '{}'",
                        query.id, source);
                }
            }
        }

        // Validate benchmark steps reference valid queries and servers
        for step in &self.benchmark_sequence {
            match step {
                BenchmarkStep::StartTcpServers { servers } |
                BenchmarkStep::StopTcpServers { servers } => {
                    for server in servers {
                        if !self.tcp_servers.iter().any(|s| &s.name == server) {
                            anyhow::bail!("Step references unknown TCP server: '{}'", server);
                        }
                    }
                }
                BenchmarkStep::SubmitQuery { query_id, .. } => {
                    if !self.queries.iter().any(|q| &q.id == query_id) {
                        anyhow::bail!("Step references unknown query: '{}'", query_id);
                    }
                }
                BenchmarkStep::SubmitParallel { queries } => {
                    for pq in queries {
                        if !self.queries.iter().any(|q| &q.id == &pq.query_id) {
                            anyhow::bail!("Parallel step references unknown query: '{}'", pq.query_id);
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub fn write_example<P: AsRef<Path>>(path: P) -> Result<()> {
        let example = BenchmarkConfig {
            name: "Example Benchmark".to_string(),
            description: Some("Example benchmark configuration for NES system".to_string()),

            system: SystemConfig {
                binary_path: PathBuf::from("/path/to/nes-single-node-worker"),
                working_directory: PathBuf::from("/path/to/working/dir"),
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("worker.queryEngine.numberOfWorkerThreads".to_string(), "23".to_string());
                    params.insert("worker.queryEngine.admissionQueueSize".to_string(), "5000".to_string());
                    params.insert("worker.queryEngine.taskQueueSize".to_string(), "100000".to_string());
                    params.insert("worker.defaultQueryExecution.executionMode".to_string(), "COMPILER".to_string());
                    params.insert("worker.numberOfBuffersInGlobalBufferManager".to_string(), "800000".to_string());
                    params.insert("worker.bufferSizeInBytes".to_string(), "65536".to_string());
                    params
                },
                startup_delay_ms: Some(5000),
                shutdown_delay_ms: Some(2000),
            },

            tcp_servers: vec![
                TcpServerConfig {
                    name: "yahoo_server".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 1111,
                    file_path: PathBuf::from("/path/to/yahoo_data.csv"),
                    repeat_count: 5,
                    buffer_size: Some(65536),
                    batch_size: Some(1),
                },
                TcpServerConfig {
                    name: "nexmark_server".to_string(),
                    host: "127.0.0.1".to_string(),
                    port: 2222,
                    file_path: PathBuf::from("/path/to/nexmark_data.csv"),
                    repeat_count: 10,
                    buffer_size: Some(65536),
                    batch_size: Some(2),
                },
            ],

            queries: vec![
                QueryDefinition {
                    id: "projection_yahoo".to_string(),
                    yaml_path: PathBuf::from("/path/to/q-projection_yahoo_tcp.yaml"),
                    tcp_sources: vec!["yahoo_server".to_string()],
                    output_suffix: Some("projection".to_string()),
                },
                QueryDefinition {
                    id: "q5_nexmark".to_string(),
                    yaml_path: PathBuf::from("/path/to/q5_nexmark_tcp.yaml"),
                    tcp_sources: vec!["nexmark_server".to_string()],
                    output_suffix: Some("nexmark".to_string()),
                },
            ],

            benchmark_sequence: vec![
                BenchmarkStep::StartSystem,
                BenchmarkStep::Wait { duration_ms: 5000 },

                // First phase: Yahoo queries
                BenchmarkStep::StartTcpServers {
                    servers: vec!["yahoo_server".to_string()],
                },
                BenchmarkStep::SubmitQuery {
                    query_id: "projection_yahoo".to_string(),
                    count: 5,
                    delay_between_ms: Some(100),
                },
                BenchmarkStep::Wait { duration_ms: 10000 },
                BenchmarkStep::CollectMetrics {
                    name: "yahoo_phase".to_string(),
                },

                // Second phase: Nexmark queries in parallel
                BenchmarkStep::StartTcpServers {
                    servers: vec!["nexmark_server".to_string()],
                },
                BenchmarkStep::SubmitParallel {
                    queries: vec![
                        ParallelQuery {
                            query_id: "q5_nexmark".to_string(),
                            count: 10,
                        },
                    ],
                },
                BenchmarkStep::WaitForCompletion {
                    timeout_ms: Some(60000),
                },
                BenchmarkStep::CollectMetrics {
                    name: "nexmark_phase".to_string(),
                },

                BenchmarkStep::StopSystem,
            ],

            output: OutputConfig {
                results_dir: PathBuf::from("./benchmark_results"),
                log_dir: PathBuf::from("./benchmark_logs"),
                metrics_file: Some(PathBuf::from("./metrics.json")),
                save_query_outputs: true,
            },
        };

        let toml_str = toml::to_string_pretty(&example)?;
        fs::write(path, toml_str)?;
        Ok(())
    }

    pub fn get_total_tcp_connections(&self, query_id: &str, count: u32) -> Result<HashMap<String, u32>> {
        let query = self.queries.iter()
            .find(|q| q.id == query_id)
            .ok_or_else(|| anyhow::anyhow!("Query '{}' not found", query_id))?;

        let mut connections = HashMap::new();
        for source_name in &query.tcp_sources {
            connections.insert(source_name.clone(), count);
        }

        Ok(connections)
    }
}