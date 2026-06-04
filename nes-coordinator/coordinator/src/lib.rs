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

//! Coordinator process glue.
//!
//! The coordinator is the long-lived control-plane component. It accepts
//! client requests, writes the resulting desired state into the catalog,
//! and lets the controller drive workers to match what the catalog says.
//!
//! Two long-running tasks share one process, joined by `tokio::select!`:
//!
//!  - The `RequestHandler` owns the request channel. It runs each
//!    statement inside a database transaction, sends the reply back on
//!    the per-request oneshot, and — for blocking requests — parks the
//!    reply until the catalog reaches the state the caller asked for.
//!  - The `Controller` (from the `controller` crate) walks the catalog
//!    on a timer, spawns one task per active row, and tears those tasks
//!    down when a row goes away or is marked removed.
//!
//! The two sides do not share memory: the catalog is the only shared
//! state. They coordinate over two `watch` channels:
//!
//!  - `intent`: the request handler sends after every successful
//!    statement to nudge the controller to look at the catalog now
//!    rather than at the next tick.
//!  - `state`: the controller sends after every successful row update
//!    so the request handler can re-check whether any parked reply is
//!    ready.
//!
//! `watch` is chosen on purpose. The receiver only cares that something
//! changed; older notifications are useless once a newer one arrives.
//! Coalescing is built in, so a burst of updates collapses into one
//! wake-up instead of flooding either side.
//!
//! There are two entry points. `run` takes an already-built `Database`
//! and runs both tasks on the current runtime. `start_with_runtime`
//! builds a `Database` from a `StateBackend`, spawns `run` on the
//! provided runtime, and returns the sender end of the request channel
//! so callers can submit requests from outside the runtime.

mod request_handler;
mod sql_planner;

pub use sql_planner::SqlPlanner;

use crate::request_handler::RequestHandler;
use controller::Controller;
use controller::embedded::WorkerFactory;
use model::database::Database;
use model::request::Request;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Instrument, info_span};

#[cfg(not(madsim))]
use model::database::StateBackend;
#[cfg(not(madsim))]
use tracing::info;

pub const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 1024;

pub async fn run(
    db: Database,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn WorkerFactory>>,
    receiver: flume::Receiver<Request>,
) {
    let (intent_tx, intent_rx) = watch::channel(());
    let (state_tx, state_rx) = watch::channel(());

    let controller = Controller::new(db.connection().clone(), intent_rx, Arc::new(state_tx), factory);
    let handler = RequestHandler::new(receiver, db, intent_tx, state_rx, planner);

    tokio::select! {
        () = controller.run().instrument(info_span!("controller")) => {}
        () = handler.run().instrument(info_span!("request_handler")) => {}
    }
}

#[cfg(not(madsim))]
pub fn start_with_runtime(
    runtime: &tokio::runtime::Runtime,
    state_backend: Option<StateBackend>,
    planner: Option<Arc<dyn SqlPlanner>>,
    worker_factory: Option<Arc<dyn WorkerFactory>>,
    request_buffer_size: Option<usize>,
) -> flume::Sender<Request> {
    info!("starting");
    let (sender, receiver) =
        flume::bounded(request_buffer_size.unwrap_or(DEFAULT_REQUEST_QUEUE_CAPACITY));

    runtime.block_on(async {
        let db = Database::with(state_backend.unwrap_or(StateBackend::Memory))
            .await
            .expect("failed to create database state");
        db.migrate()
            .await
            .expect("failed to run database migrations");

        tokio::spawn(run(db, planner, worker_factory, receiver));
    });

    sender
}
