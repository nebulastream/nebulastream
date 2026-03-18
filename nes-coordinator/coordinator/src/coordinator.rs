use crate::request_handler::RequestHandler;
use anyhow::Result;
use catalog::Catalog;
use catalog::database::{Database, StateBackend};
use controller::worker::worker_controller::WorkerController;
use common::into_request;
use common::request::Request;
use controller::query::query_controller::QueryController;
use model::query;
use model::query::fragment::{self as fragment};
use model::query::{CreateQuery, DropQuery, GetQuery};
use model::sink::{self, CreateSink, DropSink, GetSink};
use model::source::logical_source::{
    self, CreateLogicalSource, DropLogicalSource, GetLogicalSource,
};
use model::source::physical_source::{
    self, CreatePhysicalSource, DropPhysicalSource, GetPhysicalSource,
};
use model::worker::{self, CreateWorker, DropWorker, GetWorker};
use controller::worker::worker_registry::WorkerRegistry;
use controller::worker::poly_join_set::JoinSet;
use std::pin::Pin;
use strum::Display;
use std::sync::Arc;
use tracing::{Instrument, error, info, info_span};

use common::supervised::supervised;

pub(crate) type CreateLogicalSourceRequest = Request<CreateLogicalSource, Result<logical_source::Model>>;
pub(crate) type CreatePhysicalSourceRequest =
    Request<CreatePhysicalSource, Result<physical_source::Model>>;
pub(crate) type CreateSinkRequest = Request<CreateSink, Result<sink::Model>>;
pub(crate) type CreateQueryRequest = Request<CreateQuery, Result<query::Model>>;
pub(crate) type CreateWorkerRequest = Request<CreateWorker, Result<worker::Model>>;

pub(crate) type DropLogicalSourceRequest =
    Request<DropLogicalSource, Result<Option<logical_source::Model>>>;
pub(crate) type DropPhysicalSourceRequest =
    Request<DropPhysicalSource, Result<Vec<physical_source::Model>>>;
pub(crate) type DropSinkRequest = Request<DropSink, Result<Vec<sink::Model>>>;
pub(crate) type DropQueryRequest = Request<DropQuery, Result<Vec<query::Model>>>;
pub(crate) type DropWorkerRequest = Request<DropWorker, Result<worker::Model>>;

pub(crate) type GetLogicalSourceRequest = Request<GetLogicalSource, Result<Option<logical_source::Model>>>;
pub(crate) type GetPhysicalSourceRequest = Request<GetPhysicalSource, Result<Vec<physical_source::Model>>>;
pub(crate) type GetSinkRequest = Request<GetSink, Result<Vec<sink::Model>>>;
pub(crate) type GetQueryRequest = Request<GetQuery, Result<Vec<(query::Model, Vec<fragment::Model>)>>>;
pub(crate) type GetWorkerRequest = Request<GetWorker, Result<Vec<worker::Model>>>;

#[derive(Debug)]
pub enum CoordinatorRequest {
    CreateLogicalSource(CreateLogicalSourceRequest),
    CreatePhysicalSource(CreatePhysicalSourceRequest),
    CreateSink(CreateSinkRequest),
    CreateQuery(CreateQueryRequest),
    CreateWorker(CreateWorkerRequest),
    DropLogicalSource(DropLogicalSourceRequest),
    DropPhysicalSource(DropPhysicalSourceRequest),
    DropSink(DropSinkRequest),
    DropQuery(DropQueryRequest),
    DropWorker(DropWorkerRequest),
    GetLogicalSource(GetLogicalSourceRequest),
    GetPhysicalSource(GetPhysicalSourceRequest),
    GetSink(GetSinkRequest),
    GetQuery(GetQueryRequest),
    GetWorker(GetWorkerRequest),
}

into_request!(
    CreateLogicalSource,
    CreateLogicalSourceRequest,
    CoordinatorRequest
);
into_request!(
    CreatePhysicalSource,
    CreatePhysicalSourceRequest,
    CoordinatorRequest
);
into_request!(CreateSink, CreateSinkRequest, CoordinatorRequest);
into_request!(CreateQuery, CreateQueryRequest, CoordinatorRequest);
into_request!(CreateWorker, CreateWorkerRequest, CoordinatorRequest);
into_request!(
    DropLogicalSource,
    DropLogicalSourceRequest,
    CoordinatorRequest
);
into_request!(
    DropPhysicalSource,
    DropPhysicalSourceRequest,
    CoordinatorRequest
);
into_request!(DropSink, DropSinkRequest, CoordinatorRequest);
into_request!(DropQuery, DropQueryRequest, CoordinatorRequest);
into_request!(DropWorker, DropWorkerRequest, CoordinatorRequest);
into_request!(
    GetLogicalSource,
    GetLogicalSourceRequest,
    CoordinatorRequest
);
into_request!(
    GetPhysicalSource,
    GetPhysicalSourceRequest,
    CoordinatorRequest
);
into_request!(GetSink, GetSinkRequest, CoordinatorRequest);
into_request!(GetQuery, GetQueryRequest, CoordinatorRequest);
into_request!(GetWorker, GetWorkerRequest, CoordinatorRequest);

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
    receiver: flume::Receiver<CoordinatorRequest>,
    services: JoinSet<(Service, bool)>,
}

impl Supervisor {
    fn new(
        catalog: Arc<Catalog>,
        registry: WorkerRegistry,
        receiver: flume::Receiver<CoordinatorRequest>,
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
            .spawn(async move { (svc, supervised(fut).await) });
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
pub fn start(
    state_backend: Option<StateBackend>,
    request_buffer_size: Option<usize>,
) -> flume::Sender<CoordinatorRequest> {
    info!("starting");
    let (handle, receiver) = flume::bounded(request_buffer_size.unwrap_or(DEFAULT_CAPACITY));

    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_time()
            .enable_io()
            .build()
            .expect("failed to create runtime");

        rt.block_on(async move {
            let state = Database::with(state_backend.unwrap_or_default())
                .await
                .expect("failed to create database state");
            let catalog = Catalog::from(state);
            let registry = WorkerRegistry::default();

            Supervisor::new(catalog, registry, receiver).run().await
        });

        rt.shutdown_background();
    });

    handle
}

#[cfg(any(test, feature = "testing", madsim))]
pub async fn start_for_test() -> flume::Sender<CoordinatorRequest> {
    info!("starting");
    let (handle, receiver) = flume::bounded(DEFAULT_CAPACITY);

    let catalog = Catalog::for_test().await;
    let registry = WorkerRegistry::default();

    tokio::spawn(Supervisor::new(catalog, registry, receiver).run());

    handle
}

#[cfg(madsim)]
pub async fn start_for_sim(db_path: &str) -> flume::Sender<CoordinatorRequest> {
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
