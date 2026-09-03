/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
mod start;

use anyhow::bail;
use clap::{Parser, Subcommand};
use coordinator_bridge::ffi::WorkerMode;
use model::identifier::QueryId;
use model::query::{DropQuery, GetQuery};
use model::request::Payload;
use model::statement::{Statement, StatementResult};
use model::worker::GetWorker;
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{Level, error};

pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Parser)]
#[command(name = "nebucli")]
struct Cli {
    /// Enable debug logging
    #[arg(short = 'd', long)]
    debug: bool,

    /// Run queries on an in-process embedded worker instead of deploying to remote workers
    #[arg(long)]
    embedded: bool,

    /// Path to setup file, or '-' for stdin
    #[arg(short = 's', long = "setup")]
    setup: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Submit queries from setup file or CLI args
    Start {
        /// Block until all queries reach the Completed state, not just Running
        #[arg(long = "until-completed")]
        until_completed: bool,
        #[arg(trailing_var_arg = true)]
        queries: Vec<String>,
    },
    /// Stop running queries by ID
    Stop {
        #[arg(required = true)]
        query_ids: Vec<i64>,
        /// Wait up to N seconds for queries to reach a terminal state
        #[arg(long, short)]
        wait: Option<u64>,
    },
    /// Show query or worker status
    Status {
        /// Wait up to N seconds for fragments to be polled freshly
        #[arg(long, short)]
        wait: Option<u64>,
        query_ids: Vec<i64>,
    },
    /// Print query plans without executing
    Dump {
        #[arg(trailing_var_arg = true)]
        queries: Vec<String>,
    },
}

fn default_db_path() -> String {
    let base = std::env::var("XDG_STATE_HOME").unwrap_or_else(|_| {
        let home = std::env::var("HOME").expect("HOME not set");
        format!("{home}/.local/state")
    });
    format!("{base}/nebucli/coordinator.db")
}

pub fn run() -> anyhow::Result<()> {
    let result = run_inner();
    if let Err(e) = &result {
        error!("{e:#}");
    }
    result
}

fn run_inner() -> anyhow::Result<()> {
    let cli = Cli::parse();

    if cli.debug {
        let file_appender = tracing_appender::rolling::never(".", "nes-cli.log");
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .with_ansi(false)
            .with_writer(file_appender)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_writer(std::io::stderr)
            .without_time()
            .with_target(false)
            .init();
    }

    let db = default_db_path();

    if let Some(parent) = std::path::Path::new(&db).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let setup = match cli.command {
        Command::Start { .. } | Command::Dump { .. } => {
            Some(start::load_setup_file(cli.setup.as_deref())?)
        }
        _ => None,
    };

    let optimizer_config = setup
        .as_ref()
        .map(|s| s.optimizer_config_json())
        .unwrap_or_default();
    let mode = if cli.embedded {
        WorkerMode::Embedded
    } else {
        WorkerMode::Remote
    };
    let handle = coordinator_bridge::start_coordinator(&db, mode, &optimizer_config)?;

    match cli.command {
        Command::Start {
            until_completed,
            queries,
        } => {
            start::run(&handle, setup.unwrap(), &queries, until_completed)?;
        }
        Command::Stop { query_ids, wait } => {
            let timeout = wait
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
            let errors: Vec<_> = std::thread::scope(|scope| {
                query_ids
                    .iter()
                    .map(|id| {
                        let stmt = Statement::DropQuery(
                            DropQuery::all()
                                .with_filters(GetQuery::all().with_id(QueryId::new(*id))),
                        );
                        scope.spawn(|| {
                            handle.send(Payload::parsed(stmt).until_terminated(Some(timeout)))
                        })
                    })
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|h| h.join().expect("stop thread panicked"))
                    .filter_map(|rsp| rsp.err())
                    .collect()
            });
            if !errors.is_empty() {
                for err in &errors {
                    error!("{err:#}");
                }
                bail!("{} stop request(s) failed", errors.len());
            }
        }
        Command::Status { wait, query_ids } => {
            let req = if query_ids.is_empty() {
                GetQuery::all().with_fragments()
            } else {
                GetQuery::all()
                    .with_ids(query_ids.into_iter().map(QueryId::new).collect())
                    .with_fragments()
            };
            let mut payload = Payload::parsed(Statement::GetQuery(req));
            if let Some(secs) = wait {
                payload = payload.poll_for(Some(Duration::from_secs(secs)));
            }
            let result = handle.send(payload)?;
            let StatementResult::Queries(queries) = result else {
                return Ok(());
            };

            let workers_resp =
                handle.send(Payload::parsed(Statement::GetWorker(GetWorker::all())))?;
            let worker_states: HashMap<NetworkAddr, _> = match workers_resp {
                StatementResult::Workers(workers) => workers
                    .into_iter()
                    .map(|w| (w.host_addr, w.current_state))
                    .collect(),
                _ => HashMap::new(),
            };

            let output: Vec<_> = queries
                .into_iter()
                .map(|(q, fragments)| {
                    let mut val = serde_json::to_value(&q).unwrap();
                    let augmented: Vec<_> = fragments
                        .into_iter()
                        .map(|f| {
                            let host = f.host_addr.clone();
                            let mut f_val = serde_json::to_value(&f).unwrap();
                            f_val.as_object_mut().unwrap().insert(
                                "worker_state".to_string(),
                                serde_json::to_value(worker_states.get(&host)).unwrap(),
                            );
                            f_val
                        })
                        .collect();
                    val.as_object_mut().unwrap().insert(
                        "fragments".to_string(),
                        serde_json::to_value(&augmented).unwrap(),
                    );
                    val
                })
                .collect();
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        Command::Dump { queries } => {
            start::send_setup(&handle, setup.unwrap())?;
            for query in &queries {
                let result = handle.send(Payload::sql(format!("EXPLAIN {query}")))?;
                println!("{result}");
            }
        }
    }

    Ok(())
}

#[unsafe(no_mangle)]
pub extern "C" fn nebucli_main() -> i32 {
    match run() {
        Ok(()) => 0,
        Err(e) => {
            eprintln!("Error: {}", e.root_cause());
            1
        }
    }
}
