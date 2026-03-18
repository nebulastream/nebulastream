use crate::worker::worker_registry::WorkerRegistry;
use catalog::worker_catalog::WorkerCatalog;
use model::request::Request;
use model::query::StopMode;
use model::query::fragment::FragmentId;
use model::worker;
use model::worker::WorkerState;
use model::worker::endpoint::GrpcAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::select;
use tokio::sync::oneshot;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::{Channel, Endpoint};
use tracing::{Instrument, debug, error, info, instrument, warn};

pub mod worker_rpc_service {
    tonic::include_proto!("worker_rpc");
}

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;
use worker_rpc_service::{
    QueryStatusRequest, RegisterQueryReply, RegisterQueryRequest, StartQueryRequest,
    StopQueryRequest,
};

pub(crate) use worker_rpc_service::Error as FragmentError;
pub(crate) use worker_rpc_service::QueryStatusReply;

#[derive(Error, Debug)]
pub(crate) enum WorkerClientErr {
    #[error("Failed to connect to {1}: {0}")]
    Connection(tonic::transport::Error, GrpcAddr),

    #[error("gRPC error at '{addr}': {status}")]
    Grpc {
        addr: GrpcAddr,
        status: tonic::Status,
    },
}

impl WorkerClientErr {
    pub(crate) fn grpc_error(addr: GrpcAddr, status: tonic::Status) -> Self {
        WorkerClientErr::Grpc { addr, status }
    }
}

pub(crate) type RegisterFragmentRequest =
    Request<FragmentId, Result<RegisterQueryReply, WorkerClientErr>>;
pub(crate) type StartFragmentRequest = Request<FragmentId, Result<(), WorkerClientErr>>;
pub(crate) type StopFragmentRequest = Request<(FragmentId, StopMode), Result<(), WorkerClientErr>>;
pub(crate) type GetFragmentStatusRequest =
    Request<FragmentId, Result<QueryStatusReply, WorkerClientErr>>;

pub(crate) enum Rpc {
    RegisterFragment(RegisterFragmentRequest),
    StartFragment(StartFragmentRequest),
    StopFragment(StopFragmentRequest),
    GetFragmentStatus(GetFragmentStatusRequest),
}

pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_MAX_RETRIES: usize = 8;
const CONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
pub(crate) const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const RPC_CHANNEL_CAPACITY: usize = 64;
const ENDPOINT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);
const ENDPOINT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(60);

async fn connect(grpc_addr: &GrpcAddr) -> Result<Channel, WorkerClientErr> {
    let endpoint = Endpoint::from_shared(format!("http://{}", grpc_addr))
        .map_err(|e| WorkerClientErr::Connection(e, grpc_addr.clone()))?
        .http2_keep_alive_interval(ENDPOINT_KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(ENDPOINT_KEEP_ALIVE_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT);

    let grpc_addr = grpc_addr.clone();
    Retry::spawn(connect_retry_strategy(), || async {
        endpoint.connect().await.map_err(|e| {
            debug!("retrying connection");
            WorkerClientErr::Connection(e, grpc_addr.clone())
        })
    })
    .await
}

fn connect_retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(100)
        .max_delay(CONNECT_MAX_DELAY)
        .map(jitter)
        .take(CONNECT_MAX_RETRIES)
}

fn reply_to<R, E>(tx: oneshot::Sender<Result<R, WorkerClientErr>>, res: Result<R, E>)
where
    E: Into<WorkerClientErr>,
{
    let res = res.map_err(|e| e.into());
    if tx.send(res).is_err() {
        debug!("requesting task dropped the receiver channel");
    }
}

// Spawns an RPC call on a separate task so the active loop isn't blocked.
// The caller (QueryController) awaits the result via the oneshot in the Request.
macro_rules! dispatch {
    ($grpc_addr:expr, $client:expr, $tx:expr, $method:ident, $req:expr) => {
        let addr: GrpcAddr = $grpc_addr.clone();
        let span = tracing::Span::current();
        tokio::spawn(
            async move {
                let res = match tokio::time::timeout(
                    RPC_TIMEOUT,
                    $client.$method($req),
                )
                .await
                {
                    Ok(r) => r
                        .map(|resp| resp.into_inner())
                        .map_err(|status| {
                            WorkerClientErr::grpc_error(addr, status)
                        }),
                    Err(_) => Err(WorkerClientErr::grpc_error(
                        addr,
                        tonic::Status::deadline_exceeded("RPC timeout"),
                    )),
                };
                reply_to($tx, res);
            }
            .instrument(span),
        );
    };
}

// Infinite loop: connect → serve RPCs + health check → on failure mark
// Unreachable → backoff → reconnect. Runs as a single task per worker,
// owned by WorkerController. Only exits when aborted.
pub(crate) struct WorkerTask {
    worker: worker::Model,
    catalog: Arc<WorkerCatalog>,
    registry: WorkerRegistry,
}

impl WorkerTask {
    pub(crate) fn new(
        worker: worker::Model,
        catalog: Arc<WorkerCatalog>,
        registry: WorkerRegistry,
    ) -> Self {
        Self { worker, catalog, registry }
    }

    fn addr(&self) -> &GrpcAddr {
        &self.worker.grpc_addr
    }

    #[instrument(skip_all, fields(grpc_addr = %self.addr()))]
    pub(crate) async fn run(self) {
        loop {
            match connect(self.addr()).await {
                Ok(channel) => self.client(channel).await,
                Err(e) => {
                    error!("failed to connect: {e}");
                    self.catalog
                        .set_worker_state(self.worker.clone().into(), WorkerState::Unreachable)
                        .await
                        .ok();
                }
            }
            debug!("reconnecting after backoff");
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    }

    // Runs while the gRPC channel is healthy. Breaks on health check failure
    // or channel close, then caller (run) will reconnect.
    async fn client(&self, channel: Channel) {
        // Both clients share the same HTTP/2 channel
        let worker_client = WorkerRpcServiceClient::new(channel.clone());
        let mut health_client = health_proto::health_client::HealthClient::new(channel);

        // Publish the RPC sender so QueryController can dispatch fragment RPCs.
        // Guard auto-unregisters on drop (return, break, or task abort).
        let (rpc_sender, rpc_receiver) = flume::bounded(RPC_CHANNEL_CAPACITY);
        let _guard = self.registry.register(self.addr().clone(), rpc_sender);

        if let Err(e) = self
            .catalog
            .set_worker_state(self.worker.clone().into(), WorkerState::Active)
            .await
        {
            warn!("failed to mark worker active: {e}");
            return;
        }
        info!("worker active");

        // Skip the first immediate tick so we don't health-check before any RPC
        let mut health_interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);
        health_interval.tick().await;

        loop {
            select! {
                // QueryController sends RPCs via the flume channel
                rpc = rpc_receiver.recv_async() => {
                    match rpc {
                        Ok(rpc) => self.dispatch_rpc(&worker_client, rpc),
                        // All senders dropped — WorkerController removed this worker
                        Err(_) => {
                            debug!("RPC channel closed");
                            break;
                        }
                    }
                }
                _ = health_interval.tick() => {
                    let req = tonic::Request::new(
                        health_proto::HealthCheckRequest { service: String::new() }
                    );
                    let healthy = match tokio::time::timeout(PROBE_TIMEOUT, health_client.check(req)).await {
                        Ok(Ok(_)) => true,
                        _ => false,
                    };
                    if !healthy {
                        warn!("health check failed");
                        break;
                    }
                }
            }
        }

        self.catalog
            .set_worker_state(self.worker.clone().into(), WorkerState::Unreachable)
            .await
            .ok();
        info!("worker unreachable");
    }

    fn dispatch_rpc(&self, client: &WorkerRpcServiceClient<Channel>, rpc: Rpc) {
        let mut client = client.clone();
        match rpc {
            Rpc::RegisterFragment(Request {
                payload: id,
                reply_to: tx,
            }) => {
                dispatch!(
                    self.addr(), client, tx, register_query,
                    RegisterQueryRequest { query_id: u64::try_from(id).unwrap() }
                );
            }
            Rpc::StartFragment(Request {
                payload: id,
                reply_to: tx,
            }) => {
                dispatch!(
                    self.addr(), client, tx, start_query,
                    StartQueryRequest { query_id: u64::try_from(id).unwrap() }
                );
            }
            Rpc::StopFragment(Request {
                payload: (id, stop_mode),
                reply_to: tx,
            }) => {
                dispatch!(
                    self.addr(), client, tx, stop_query,
                    StopQueryRequest {
                        query_id: u64::try_from(id).unwrap(),
                        termination_type: stop_mode.into(),
                    }
                );
            }
            Rpc::GetFragmentStatus(Request {
                payload: id,
                reply_to: tx,
            }) => {
                dispatch!(
                    self.addr(), client, tx, request_query_status,
                    QueryStatusRequest { query_id: u64::try_from(id).unwrap() }
                );
            }
        }
    }
}
