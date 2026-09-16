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
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

use anyhow::Context;
use clap::Parser;
use coordinator_bridge::WorkerMode;
use model::database::Database;
use model::statement::Statement;
use model::worker::CreateWorker;
use model::worker::endpoint::NetworkAddr;
use server::Args;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal;
use tokio::signal::unix::SignalKind;
use tracing::{error, info};

const DATA_PORT_OFFSET: u16 = 1000;

fn run() -> anyhow::Result<()> {
    let args = Args::parse();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to create the runtime")?;

    let mode = match args.worker_mode {
        server::WorkerMode::Embedded => WorkerMode::Embedded,
        server::WorkerMode::Remote => WorkerMode::Remote,
    };
    let default_host = coordinator_bridge::default_host(mode);
    let planner = coordinator_bridge::sql_planner(
        runtime.handle().clone(),
        &args.optimizer_config,
        default_host,
    );
    let (factory, bootstrap) = match mode {
        WorkerMode::Embedded => (
            Some(coordinator_bridge::embedded_worker_factory()),
            vec![embedded_worker(default_host)?],
        ),
        _ => (None, vec![]),
    };

    let served = runtime.block_on(async {
        let db = Database::open(args.state_backend()).await?;
        let (router, coordinator) =
            server::start(db, Some(planner), factory, args.config(), bootstrap).await?;
        let listener = TcpListener::bind(args.listen)
            .await
            .with_context(|| format!("failed to bind {}", args.listen))?;
        let address = listener.local_addr()?;
        info!("listening on http://{address}");
        eprintln!("listening on http://{address}");
        server::serve(listener, router, shutdown_signal()).await?;
        coordinator.await.context("the coordinator panicked")?;
        anyhow::Ok(())
    });
    runtime.shutdown_timeout(Duration::from_millis(500));
    served
}

fn embedded_worker(host: &str) -> anyhow::Result<Statement> {
    let host_addr: NetworkAddr = host
        .parse()
        .map_err(|error: String| anyhow::anyhow!(error))?;
    let data_addr = NetworkAddr::new(host_addr.host.clone(), host_addr.port + DATA_PORT_OFFSET)?;
    Ok(Statement::CreateWorker(CreateWorker {
        host_addr,
        data_addr,
        max_operators: None,
        peers: vec![],
        config: serde_json::json!({}),
        if_not_exists: true,
    }))
}

async fn shutdown_signal() {
    let mut terminate = signal::unix::signal(SignalKind::terminate())
        .expect("failed to install the termination signal handler");
    tokio::select! {
        result = signal::ctrl_c() => {
            if let Err(error) = result {
                error!(%error, "failed to listen for the interrupt signal");
            }
            info!("interrupted, shutting down");
        }
        _ = terminate.recv() => info!("terminated, shutting down"),
    }
    eprintln!("shutting down");
}

#[unsafe(no_mangle)]
pub extern "C" fn nes_server_main() -> i32 {
    match run() {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("Error: {error:#}");
            1
        }
    }
}
