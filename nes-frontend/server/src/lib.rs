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

#![warn(clippy::pedantic, clippy::nursery)]
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::redundant_pub_crate,
    clippy::module_name_repetitions
)]

mod args;
mod error;
mod extract;
mod routes;
mod state;
mod wait;

pub use args::{Args, WorkerMode};
pub use error::ApiError;
pub use routes::router;
pub use state::AppState;

use anyhow::{Context, anyhow};
use axum::Router;
use controller::in_process::WorkerFactory;
use coordinator::SqlPlanner;
use model::database::Database;
use model::request::{StatementInput, Wait};
use model::statement::Statement;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

#[derive(Debug, Clone)]
pub struct Config {
    pub wait_cap: Duration,
    pub queue_capacity: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wait_cap: Duration::from_secs(30),
            queue_capacity: coordinator::DEFAULT_REQUEST_QUEUE_CAPACITY,
        }
    }
}

pub async fn start(
    db: Database,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn WorkerFactory>>,
    config: Config,
    bootstrap: Vec<Statement>,
) -> anyhow::Result<(Router, JoinHandle<()>)> {
    let (sender, receiver) = async_channel::bounded(config.queue_capacity);
    let coordinator = tokio::spawn(coordinator::run(db, planner, factory, receiver));
    let state = AppState::new(sender, &config);
    for statement in bootstrap {
        let described = format!("{statement:?}");
        if let Err(error) = state
            .submit(StatementInput::Parsed(statement), Wait::None)
            .await
        {
            coordinator.abort();
            return Err(anyhow!("{}: {}", error.body.error, error.body.message))
                .with_context(|| format!("bootstrap statement failed: {described}"));
        }
    }
    Ok((router(state), coordinator))
}

pub async fn serve(
    listener: TcpListener,
    router: Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown)
        .await?;
    Ok(())
}
