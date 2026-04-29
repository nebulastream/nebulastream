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

mod request_handler;
mod sql_planner;

pub use sql_planner::SqlPlanner;

use crate::request_handler::RequestHandler;
use controller::Controller;
use controller::embedded::EmbeddedWorkerFactory;
use model::database::{Database, StateBackend};
use model::request::Request;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{Instrument, info, info_span};

pub const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 1024;

pub async fn run(
    db: Database,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
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
    worker_factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
    request_buffer_size: Option<usize>,
) -> flume::Sender<Request> {
    info!("starting");
    let (sender, receiver) =
        flume::bounded(request_buffer_size.unwrap_or(DEFAULT_REQUEST_QUEUE_CAPACITY));

    runtime.block_on(async {
        let db = Database::with(state_backend.unwrap_or_default())
            .await
            .expect("failed to create database state");
        db.migrate()
            .await
            .expect("failed to run database migrations");

        tokio::spawn(run(db, planner, worker_factory, receiver));
    });

    sender
}
