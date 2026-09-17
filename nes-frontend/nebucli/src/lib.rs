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

mod client;
mod output;
mod start;

pub use client::{Client, ClientError, WaitKind};
pub use output::{Format, Output};

use anyhow::bail;
use clap::{Parser, Subcommand};
use model::query::{QueryWithFragments, query_fragment};
use model::statement::StatementResult;
use model::worker;
use model::worker::endpoint::NetworkAddr;
use reqwest::Url;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::io::{IsTerminal, Write};
use std::time::{Duration, Instant};
use tracing::{Level, error};

pub(crate) const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

const DEFAULT_COORDINATOR: &str = "http://127.0.0.1:8081";

#[derive(Parser)]
#[command(name = "nebucli")]
struct Cli {
    #[arg(
        long,
        env = "NES_COORDINATOR",
        default_value = DEFAULT_COORDINATOR,
        global = true
    )]
    coordinator: Url,

    #[arg(short = 'd', long, global = true)]
    debug: bool,

    /// Path to setup file, or '-' for stdin
    #[arg(short = 's', long = "setup", global = true)]
    setup: Option<String>,

    #[arg(short = 'o', long, value_enum, default_value_t = Format::Auto, global = true)]
    output: Format,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
        #[arg(long, short)]
        wait: Option<u64>,
    },
    Status {
        query_ids: Vec<i64>,
    },
    Dump {
        #[arg(trailing_var_arg = true)]
        queries: Vec<String>,
    },
    Sql {
        statement: String,
    },
}

pub fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let terminal = std::io::stdout().is_terminal();
    let mut stdout = std::io::stdout().lock();
    execute(cli, &mut stdout, terminal)
}

pub fn run_with(
    args: impl IntoIterator<Item = String>,
    out: &mut impl Write,
    stdout_is_terminal: bool,
) -> anyhow::Result<()> {
    let cli = Cli::try_parse_from(args)?;
    execute(cli, out, stdout_is_terminal)
}

fn execute(cli: Cli, out: &mut impl Write, stdout_is_terminal: bool) -> anyhow::Result<()> {
    init_tracing(cli.debug);
    let result = run_inner(cli, out, stdout_is_terminal);
    if let Err(e) = &result {
        error!("{e:#}");
    }
    result
}

fn init_tracing(debug: bool) {
    let result = if debug {
        let file_appender = tracing_appender::rolling::never(".", "nes-cli.log");
        tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .with_ansi(false)
            .with_writer(file_appender)
            .try_init()
    } else {
        tracing_subscriber::fmt()
            .with_max_level(Level::WARN)
            .with_writer(std::io::stderr)
            .without_time()
            .with_target(false)
            .try_init()
    };
    drop(result);
}

fn run_inner(cli: Cli, out: &mut impl Write, stdout_is_terminal: bool) -> anyhow::Result<()> {
    let setup = match cli.command {
        Command::Start { .. } | Command::Dump { .. } => {
            Some(start::load_setup_file(cli.setup.as_deref())?)
        }
        _ => None,
    };
    if let Some(setup) = &setup {
        setup.warn_if_optimizer_set();
    }

    let client = Client::new(cli.coordinator)?;
    let mut output = Output::new(out, cli.output, stdout_is_terminal);

    match cli.command {
        Command::Start {
            until_completed,
            queries,
        } => {
            let rows = start::run(&client, setup.unwrap(), &queries, until_completed)?;
            output.result(&StatementResult::Queries(rows))?;
        }
        Command::Stop { query_ids, wait } => {
            let deadline =
                Instant::now() + wait.map_or(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs);
            let client = &client;
            let results: Vec<anyhow::Result<_>> = std::thread::scope(|scope| {
                query_ids
                    .iter()
                    .map(|id| scope.spawn(move || client.drop_query(*id, Some(deadline))))
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|handle| handle.join().expect("stop thread panicked"))
                    .collect()
            });
            let (dropped, errors): (Vec<_>, Vec<_>) = results.into_iter().partition(Result::is_ok);
            if !errors.is_empty() {
                let messages: Vec<String> = errors
                    .into_iter()
                    .map(|error| format!("{:#}", error.unwrap_err()))
                    .collect();
                bail!(
                    "{} stop request(s) failed: {}",
                    messages.len(),
                    messages.join("; ")
                );
            }
            let dropped = dropped.into_iter().map(Result::unwrap).collect();
            output.result(&StatementResult::DroppedQueries(dropped))?;
        }
        Command::Status { query_ids } => {
            let queries = client.queries(&query_ids, true)?;
            let workers = client.workers()?;
            if output.is_json() {
                output.json(&status_json(&queries, &workers))?;
            } else {
                output.result(&StatementResult::Queries(queries.clone()))?;
                for (worker, fragments) in fragments_by_worker(queries, workers) {
                    output.result(&StatementResult::WorkerStatus(worker, fragments))?;
                }
            }
        }
        Command::Dump { queries } => {
            let (statements, file_queries) = setup.unwrap().into_statements()?;
            start::register(&client, statements)?;
            let queries = if queries.is_empty() {
                file_queries
            } else {
                queries
            };
            for query in &queries {
                let explanation = client.explain(query)?;
                if output.is_json() {
                    output.json(&serde_json::json!({ "explanation": explanation }))?;
                } else {
                    output.text(&explanation)?;
                }
            }
        }
        Command::Sql { statement } => output.result(&client.sql(&statement)?)?,
    }

    Ok(())
}

fn status_json(queries: &[QueryWithFragments], workers: &[worker::Model]) -> Value {
    let worker_states: HashMap<&NetworkAddr, worker::WorkerState> = workers
        .iter()
        .map(|worker| (&worker.host_addr, worker.current_state))
        .collect();
    let rows = queries
        .iter()
        .map(|QueryWithFragments { query, fragments }| {
            let mut row = serde_json::to_value(query).expect("a query serializes");
            let fragments: Vec<Value> = fragments
                .iter()
                .map(|fragment| {
                    let mut value = serde_json::to_value(fragment).expect("a fragment serializes");
                    value["worker_state"] =
                        serde_json::to_value(worker_states.get(&fragment.host_addr))
                            .expect("a worker state serializes");
                    value
                })
                .collect();
            row["fragments"] = Value::Array(fragments);
            row
        })
        .collect();
    Value::Array(rows)
}

fn fragments_by_worker(
    queries: Vec<QueryWithFragments>,
    workers: Vec<worker::Model>,
) -> Vec<(worker::Model, Vec<query_fragment::Model>)> {
    let mut by_host: BTreeMap<String, Vec<query_fragment::Model>> = BTreeMap::new();
    for fragment in queries.into_iter().flat_map(|row| row.fragments) {
        by_host
            .entry(fragment.host_addr.to_string())
            .or_default()
            .push(fragment);
    }
    let mut workers: Vec<_> = workers
        .into_iter()
        .filter_map(|worker| {
            let fragments = by_host.remove(&worker.host_addr.to_string())?;
            Some((worker, fragments))
        })
        .collect();
    workers.sort_by(|a, b| a.0.host_addr.to_string().cmp(&b.0.host_addr.to_string()));
    workers
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
