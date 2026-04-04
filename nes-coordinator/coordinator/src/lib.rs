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
use catalog::{QueryStateManager, WorkerStateManager};
use catalog::database::{Database, StateBackend};
use controller::worker::embedded::EmbeddedWorkerFactory;
use controller::worker::worker_controller::WorkerController;
use model::request::Request;
use controller::query::query_controller::QueryController;
use controller::worker::worker_registry::WorkerRegistry;
use controller::util::join_set::JoinSet;
use std::pin::Pin;
use strum::Display;
use std::sync::Arc;
use std::time::Duration;
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
use tracing::{Instrument, error, info, info_span};

const DEFAULT_CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, Display)]
enum Service {
    WorkerController,
    QueryController,
    RequestHandler,
}

const SERVICES: [Service; 3] = [
    Service::WorkerController,
    Service::QueryController,
    Service::RequestHandler,
];

struct Supervisor {
    db: Database,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
    query_sm: Arc<QueryStateManager>,
    worker_sm: Arc<WorkerStateManager>,
    registry: WorkerRegistry,
    receiver: flume::Receiver<Request>,
    services: JoinSet<(Service, bool)>,
}

impl Supervisor {
    fn new(
        db: Database,
        planner: Option<Arc<dyn SqlPlanner>>,
        factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
        query_sm: Arc<QueryStateManager>,
        worker_sm: Arc<WorkerStateManager>,
        registry: WorkerRegistry,
        receiver: flume::Receiver<Request>,
    ) -> Self {
        Self {
            db,
            planner,
            factory,
            query_sm,
            worker_sm,
            registry,
            receiver,
            services: JoinSet::new(),
        }
    }

    fn spawn(&mut self, svc: Service) {
        let fut: Pin<Box<dyn Future<Output = ()> + Send>> = match svc {
            Service::WorkerController => Box::pin(
                WorkerController::new(self.worker_sm.clone(), self.registry.clone(), self.factory.clone())
                    .run(),
            ),
            Service::QueryController => Box::pin(
                QueryController::new(self.query_sm.clone(), self.worker_sm.clone(), self.registry.handle())
                    .run(),
            ),
            Service::RequestHandler => Box::pin(
                RequestHandler::new(self.receiver.clone(), self.db.clone(), self.planner.clone(), self.query_sm.clone(), self.worker_sm.clone())
                    .run(),
            ),
        };
        let span = info_span!("service", name = %svc);
        self.services.spawn(
            async move { (svc, AssertUnwindSafe(fut).catch_unwind().await.is_err()) }
                .instrument(span),
        );
    }

    async fn run(mut self) {
        for svc in SERVICES {
            self.spawn(svc);
        }

        while let Some(result) = self.services.join_next().await {
            match result {
                Ok((svc, true)) => {
                    error!("{svc} panicked, restarting");
                    self.spawn(svc);
                }
                Ok((svc, false)) => {
                    info!("{svc} exited, shutting down");
                    break;
                }
                Err(e) => {
                    error!("service task failed: {e}");
                    break;
                }
            }
        }
    }
}

#[cfg(not(madsim))]
pub fn start_with_runtime(
    runtime: &tokio::runtime::Runtime,
    state_backend: Option<StateBackend>,
    planner: Option<Arc<dyn SqlPlanner>>,
    factory: Option<Arc<dyn EmbeddedWorkerFactory>>,
    request_buffer_size: Option<usize>,
) -> flume::Sender<Request> {
    info!("starting");
    let (sender, receiver) = flume::bounded(request_buffer_size.unwrap_or(DEFAULT_CAPACITY));

    runtime.block_on(async {
        let db = Database::with(state_backend.unwrap_or_default())
            .await
            .expect("failed to create database state");
        db.migrate().await.expect("failed to run database migrations");
        let query_sm = QueryStateManager::from(db.clone());
        let worker_sm = WorkerStateManager::from(db.clone());
        let registry = WorkerRegistry::default();

        tokio::spawn(Supervisor::new(db, planner, factory, query_sm, worker_sm, registry, receiver).run());
    });

    sender
}

#[cfg(madsim)]
pub async fn start_for_sim(db_path: &str) -> flume::Sender<Request> {
    info!("starting (sim, db={db_path})");
    let (handle, receiver) = flume::bounded(DEFAULT_CAPACITY);

    let db = Database::with(StateBackend::sqlite(db_path))
        .await
        .expect("failed to create database");
    db.migrate().await.expect("migration failed");
    let query_sm = QueryStateManager::from(db.clone());
    let worker_sm = WorkerStateManager::from(db.clone());
    let registry = WorkerRegistry::default();

    tokio::spawn(Supervisor::new(db, None, None, query_sm, worker_sm, registry, receiver).run());

    handle
}
