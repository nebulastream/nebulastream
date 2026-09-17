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

//! The coordinator accepts client requests and writes the resulting desired state into the catalog.
//! The controller updates the workers to match the catalog.
//!
//! [`run`] polls the `RequestHandler` and the `Controller` concurrently.
//! Apart from the worker factory, which both only read, the catalog is the only state that they share.
//! They notify each other over two `watch` channels:
//!
//! ```text
//!    RequestHandler                                    Controller
//!           │── after every successful statement ──────────▶│
//!           │◀──────── after it writes a fragment's state ──│
//!           │                                               │
//!           └─ statements ──▶ catalog ◀── fragment states ──┘
//! ```
//!
//! With the first, the controller reads the catalog at once, not at its next tick.
//! With the second, the request handler checks again whether a parked reply is ready.
//! The notifications contain no data, and a `watch` channel keeps only the latest one,
//! so a burst of updates becomes one wake-up.

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

/// Returns once every sender of `receiver` is dropped.
/// With a worker factory the workers run in this process, otherwise they are remote.
/// Without a planner, every SQL statement fails.
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

/// Opens the catalog and spawns [`run`] on `runtime`.
/// The returned sender submits requests from outside the runtime.
/// Blocks the calling thread while the catalog opens, so it must not be called from async code.
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
