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

use crate::request_handler::RequestHandler;
use catalog::Catalog;
use catalog::database::{Database, StateBackend};
use controller::worker::worker_controller::WorkerController;
use model::request::Request;
use controller::query::query_controller::QueryController;
use controller::worker::worker_registry::WorkerRegistry;
use controller::worker::poly_join_set::JoinSet;
use std::pin::Pin;
use strum::Display;
use std::sync::Arc;
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
    catalog: Arc<Catalog>,
    registry: WorkerRegistry,
    receiver: flume::Receiver<Request>,
    services: JoinSet<(Service, bool)>,
}

impl Supervisor {
    fn new(
        catalog: Arc<Catalog>,
        registry: WorkerRegistry,
        receiver: flume::Receiver<Request>,
    ) -> Self {
        Self {
            catalog,
            registry,
            receiver,
            services: JoinSet::new(),
        }
    }

    fn spawn(&mut self, svc: Service) {
        // Returns a future that wraps the service
        let fut: Pin<Box<dyn Future<Output = ()> + Send>> = match svc {
            Service::WorkerController => Box::pin(
                WorkerController::new(self.catalog.worker.clone(), self.registry.clone())
                    .run()
                    .instrument(info_span!("worker_controller")),
            ),
            Service::QueryController => Box::pin(
                QueryController::new(self.catalog.clone(), self.registry.handle())
                    .run()
                    .instrument(info_span!("query_controller")),
            ),
            Service::RequestHandler => Box::pin(
                RequestHandler::new(self.receiver.clone(), self.catalog.clone())
                    .run()
                    .instrument(info_span!("request_handler")),
            ),
        };
        self.services
            .spawn(async move { (svc, AssertUnwindSafe(fut).catch_unwind().await.is_err()) });
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

pub fn start(
    state_backend: Option<StateBackend>,
    request_buffer_size: Option<usize>,
) -> (flume::Sender<Request>, Arc<Catalog>, tokio::runtime::Runtime) {
    info!("starting");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_time()
        .enable_io()
        .build()
        .expect("failed to create runtime");

    let (sender, receiver) = flume::bounded(request_buffer_size.unwrap_or(DEFAULT_CAPACITY));

    let catalog = runtime.block_on(async {
        let state = Database::with(state_backend.unwrap_or_default())
            .await
            .expect("failed to create database state");
        let catalog = Catalog::from(state);
        let registry = WorkerRegistry::default();

        tokio::spawn(Supervisor::new(catalog.clone(), registry, receiver).run());
        catalog
    });

    (sender, catalog, runtime)
}

#[cfg(madsim)]
pub async fn start_for_sim(db_path: &str) -> flume::Sender<Request> {
    info!("starting (sim, db={db_path})");
    let (handle, receiver) = flume::bounded(DEFAULT_CAPACITY);

    let db = Database::with(StateBackend::sqlite(db_path))
        .await
        .expect("failed to create database");
    db.migrate().await.expect("migration failed");
    let catalog = Catalog::from(db);
    let registry = WorkerRegistry::default();

    tokio::spawn(Supervisor::new(catalog, registry, receiver).run());

    handle
}
