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

//! Coordinator process glue.
//!
//! The coordinator is the long-lived control-plane component.
//! It accepts client requests, writes the resulting desired state into the catalog,
//! and leaves it to the controller to bring the workers in line with the catalog.
//!
//! Two long-running tasks share one process, joined by `tokio::select!`:
//!
//!  - The `RequestHandler` owns the request channel.
//!    It runs each statement inside a database transaction and sends the reply on the per-request oneshot.
//!    For blocking requests it parks the reply until the catalog reaches the state that the caller asked for.
//!  - The `Controller` (from the `controller` crate) reads the catalog on a timer,
//!    spawns one task per active row, and stops those tasks when a row is deleted or marked removed.
//!
//! The two sides do not share memory: the catalog is the only shared state.
//! They coordinate over two `watch` channels:
//!
//!  - `intent`: the request handler sends after every successful statement,
//!    so the controller reads the catalog now rather than at the next tick.
//!  - `state`: the controller sends after every successful row update,
//!    so the request handler re-checks whether any parked reply is ready.
//!
//! `watch` is chosen because the receiver only needs to know that something changed;
//! older notifications are useless once a newer one arrives.
//! Coalescing is built in, so a burst of updates collapses into one wake-up.
//!
//! There are two entry points.
//! `run` takes an already-built `Database` and runs both tasks on the current runtime.
//! `start_with_runtime` builds the `Database` from a `StateBackend`, spawns `run` on the given runtime,
//! and returns the sender end of the request channel so requests can be submitted from outside the runtime.

use controller::Controller;
use controller::in_process::WorkerFactory;
use model::database::Database;
use model::request::Request;
use model::worker;
use model::worker::WorkerVersion;
use request_handler::{RequestHandler, SqlPlanner, WorkerMode, WorkerVersions};
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Instrument, info_span};

#[cfg(not(madsim))]
use model::database::StateBackend;
#[cfg(not(madsim))]
use tracing::info;

pub const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 1024;

struct ControllerVersions(Option<Arc<dyn WorkerFactory>>);

impl WorkerVersions for ControllerVersions {
    fn versions(
        &self,
        workers: Vec<worker::Model>,
    ) -> impl Future<Output = Vec<WorkerVersion>> + Send {
        controller::worker_versions(workers, self.0.clone())
    }
}

pub async fn run(
    db: Database,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn WorkerFactory>>,
    receiver: async_channel::Receiver<Request>,
) {
    let (intent_tx, intent_rx) = watch::channel(());
    let (state_tx, state_rx) = watch::channel(());

    let mode = if factory.is_some() {
        WorkerMode::Embedded
    } else {
        WorkerMode::Remote
    };
    let controller = Controller::new(db.clone(), intent_rx, state_tx, factory.clone());
    let handler = RequestHandler::new(
        receiver,
        db,
        intent_tx,
        state_rx,
        planner,
        mode,
        ControllerVersions(factory),
    );

    tokio::select! {
        () = controller.run().instrument(info_span!("controller")) => {}
        () = handler.run().instrument(info_span!("request_handler")) => {}
    }
}

#[cfg(not(madsim))]
pub fn start_with_runtime(
    runtime: &tokio::runtime::Runtime,
    state_backend: StateBackend,
    planner: Arc<dyn SqlPlanner>,
    worker_factory: Option<Arc<dyn WorkerFactory>>,
) -> anyhow::Result<async_channel::Sender<Request>> {
    info!("starting");
    let (sender, receiver) = async_channel::bounded(DEFAULT_REQUEST_QUEUE_CAPACITY);

    runtime.block_on(async {
        let db = Database::open(state_backend).await?;
        tokio::spawn(run(db, Some(planner), worker_factory, receiver));
        anyhow::Ok(())
    })?;

    Ok(sender)
}
