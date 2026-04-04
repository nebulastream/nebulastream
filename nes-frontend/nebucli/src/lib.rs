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

use clap::{Parser, Subcommand};
use coordinator_bridge::ffi::WorkerMode;
use model::identifier::QueryId;
use model::query::fragment::StopMode;
use model::query::{DropQuery, GetQuery};
use model::request::Payload;
use model::statement::{Statement, StatementResult};

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
        /// Block until all queries reach a terminal state
        #[arg(long, short)]
        wait: bool,
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
    },
    /// Show query or worker status
    Status { query_ids: Vec<i64> },
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
    let cli = Cli::parse();

    if cli.debug {
        let file_appender = tracing_appender::rolling::never(".", "nes-cli.log");
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .with_writer(file_appender)
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
        Command::Start { wait, queries } => {
            start::run(&handle, setup.unwrap(), &queries, wait)?;
        }
        Command::Stop { query_ids, force } => {
            let mode = if force {
                StopMode::Forceful
            } else {
                StopMode::Graceful
            };
            for id in query_ids {
                let stmt = Statement::DropQuery(
                    DropQuery::all()
                        .with_stop_mode(mode)
                        .with_filters(GetQuery::all().with_id(QueryId::new(id)))
                        .should_block(),
                );
                if let Err(e) = handle.send(Payload::structured(stmt)) {
                    println!("{e}");
                }
            }
        }
        Command::Status { query_ids } => {
            let req = if query_ids.is_empty() {
                GetQuery::all().with_fragments().wait_for_poll()
            } else {
                GetQuery::all()
                    .with_ids(query_ids.into_iter().map(QueryId::new).collect())
                    .with_fragments()
                    .wait_for_poll()
            };
            let result = handle.send(Payload::structured(Statement::GetQuery(req)))?;
            if let StatementResult::Queries(queries) = result {
                let output: Vec<_> = queries
                    .into_iter()
                    .map(|(q, fragments)| {
                        let mut val = serde_json::to_value(&q).unwrap();
                        val.as_object_mut().unwrap().insert(
                            "fragments".to_string(),
                            serde_json::to_value(&fragments).unwrap(),
                        );
                        val
                    })
                    .collect();
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
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
