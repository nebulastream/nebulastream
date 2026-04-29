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

use anyhow::{anyhow, bail};
use clap::{Parser, Subcommand};
use coordinator_bridge::ffi::WorkerMode;
use coordinator_bridge::CoordinatorHandle;
use model::identifier::QueryId;
use model::query::fragment::StopMode;
use model::query::{DropQuery, GetQuery};
use model::request::{Payload, Request};
use model::statement::{Statement, StatementResult};
use model::worker::endpoint::NetworkAddr;
use model::worker::GetWorker;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{error, Level};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

pub(crate) fn send_timed(
    handle: &CoordinatorHandle,
    payload: Payload,
) -> anyhow::Result<StatementResult> {
    send_with_timeout(handle, payload, DEFAULT_REQUEST_TIMEOUT)
}

pub(crate) fn send_with_timeout(
    handle: &CoordinatorHandle,
    payload: Payload,
    timeout: Duration,
) -> anyhow::Result<StatementResult> {
    let (rx, req) = Request::from(payload);
    handle
        .sender
        .send(req)
        .map_err(|_| anyhow!("coordinator shut down"))?;
    handle.block_on(async {
        match tokio::time::timeout(timeout, rx).await {
            Ok(reply) => reply?,
            Err(_) => bail!(
                "request timed out after {}s; see nes-cli.log for details",
                timeout.as_secs()
            ),
        }
    })
}

#[derive(Parser)]
#[command(name = "nebucli")]
struct Cli {
    /// Enable debug logging
    #[arg(short = 'd', long)]
    debug: bool,

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
        /// Use forceful termination instead of graceful
        #[arg(long, short)]
        force: bool,
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
    let handle = coordinator_bridge::start_coordinator(&db, WorkerMode::Remote, &optimizer_config);

    match cli.command {
        Command::Start {
            until_completed,
            queries,
        } => {
            start::run(&handle, setup.unwrap(), &queries, until_completed)?;
        }
        Command::Stop {
            query_ids,
            force,
            wait,
        } => {
            let mode = if force {
                StopMode::Forceful
            } else {
                StopMode::Graceful
            };
            let timeout = wait
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
            let errors: Vec<_> = std::thread::scope(|scope| {
                query_ids
                    .iter()
                    .map(|id| {
                        let stmt = Statement::DropQuery(
                            DropQuery::all()
                                .with_stop_mode(mode)
                                .with_filters(GetQuery::all().with_id(QueryId::new(*id))),
                        );
                        scope.spawn(|| {
                            send_with_timeout(
                                &handle,
                                Payload::structured(stmt).block_until_dropped(),
                                timeout,
                            )
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
            let mut payload = Payload::structured(Statement::GetQuery(req));
            if let Some(secs) = wait {
                payload = payload.poll_for(secs);
            }
            let timeout = wait
                .map(|s| Duration::from_secs(s) + Duration::from_secs(5))
                .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
            let result = send_with_timeout(&handle, payload, timeout)?;
            let StatementResult::Queries(queries) = result else {
                return Ok(());
            };

            let workers_resp = send_timed(
                &handle,
                Payload::structured(Statement::GetWorker(GetWorker::all())),
            )?;
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
                let result = send_timed(&handle, Payload::sql(format!("EXPLAIN {query}")))?;
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
