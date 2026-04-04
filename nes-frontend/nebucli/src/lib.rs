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
use model::request::RequestInput;

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
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
    }

    let db = default_db_path();

    if let Some(parent) = std::path::Path::new(&db).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let handle =
        coordinator_bridge::start_coordinator(&db, coordinator_bridge::ffi::WorkerMode::Remote);

    match cli.command {
        Command::Start { queries } => {
            start::run(&handle, cli.setup.as_deref(), &queries)?;
        }
        Command::Stop { query_ids, force } => {
            use model::query::{DropQuery, GetQuery, StopMode};
            use model::request::Statement;
            let mode = if force { StopMode::Forceful } else { StopMode::Graceful };
            for id in query_ids {
                let stmt = Statement::DropQuery(
                    DropQuery::all()
                        .with_stop_mode(mode)
                        .with_filters(GetQuery::all().with_id(id))
                        .should_block(),
                );
                let result = handle.send(RequestInput::structured(stmt))?;
                println!("{result}");
            }
        }
        Command::Status { query_ids } => {
            use model::query::GetQuery;
            use model::request::Statement;
            let req = if query_ids.is_empty() {
                GetQuery::all()
            } else {
                GetQuery::all().with_ids(query_ids)
            };
            let result = handle.send(RequestInput::structured(Statement::GetQuery(req)))?;
            println!("{result}");
        }
        Command::Dump { queries } => {
            start::load_setup(&handle, cli.setup.as_deref())?;
            for query in &queries {
                let result = handle.send(RequestInput::sql(format!("EXPLAIN {query}")))?;
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
