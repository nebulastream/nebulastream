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

use crate::worker::worker_registry::WorkerRegistry;
use catalog::worker_catalog::WorkerCatalog;
use model::query::StopMode;
use model::worker;
use model::worker::WorkerState;
use model::worker::endpoint::NetworkAddr;
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
    pub mod nes {
        tonic::include_proto!("nes");
    }
    tonic::include_proto!("_");
}

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use worker_rpc_service::nes::SerializableQueryId;
use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;
use worker_rpc_service::{
    QueryStatusRequest, RegisterQueryReply, RegisterQueryRequest, StartQueryRequest,
    StopQueryRequest,
};

pub(crate) use worker_rpc_service::Error as FragmentError;
pub(crate) use worker_rpc_service::QueryStatusReply;

fn query_id(id: i64) -> Option<SerializableQueryId> {
    Some(SerializableQueryId { id })
}

#[derive(Error, Debug)]
pub(crate) enum WorkerTaskError {
    #[error("failed to connect to {addr}: {err}")]
    Connection {
        addr: NetworkAddr,
        err: tonic::transport::Error,
    },

    #[error("gRPC error at {addr}: {status}")]
    Grpc {
        addr: NetworkAddr,
        status: tonic::Status,
    },
}

pub(crate) struct Request<P, R> {
    pub payload: P,
    pub reply_to: oneshot::Sender<R>,
}

impl<P, R> Request<P, R> {
    pub fn new(payload: P) -> (oneshot::Receiver<R>, Self) {
        let (tx, rx) = oneshot::channel();
        (
            rx,
            Self {
                payload,
                reply_to: tx,
            },
        )
    }
}

fn reply_to<R, E>(tx: oneshot::Sender<Result<R, WorkerTaskError>>, res: Result<R, E>)
where
    E: Into<WorkerTaskError>,
{
    let res = res.map_err(|e| e.into());
    if tx.send(res).is_err() {
        debug!("requesting task dropped the receiver channel");
    }
}

/// Payload: (fragment_id, serialized protobuf plan bytes)
pub(crate) type RegisterFragmentRequest =
    Request<(i64, Vec<u8>), Result<RegisterQueryReply, WorkerTaskError>>;
pub(crate) type StartFragmentRequest = Request<i64, Result<(), WorkerTaskError>>;
pub(crate) type StopFragmentRequest = Request<(i64, StopMode), Result<(), WorkerTaskError>>;
pub(crate) type GetFragmentStatusRequest =
    Request<i64, Result<QueryStatusReply, WorkerTaskError>>;

pub(crate) enum Rpc {
    RegisterFragment(RegisterFragmentRequest),
    StartFragment(StartFragmentRequest),
    StopFragment(StopFragmentRequest),
    GetFragmentStatus(GetFragmentStatusRequest),
}

pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_MAX_RETRIES: usize = 10;
const CONNECT_MAX_DELAY: Duration = Duration::from_secs(5);
pub(crate) const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const PROBE_TIMEOUT: Duration = Duration::from_secs(1);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const RPC_CHANNEL_CAPACITY: usize = 64;
const ENDPOINT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);
const ENDPOINT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(60);

async fn connect(addr: &NetworkAddr) -> Result<Channel, WorkerTaskError> {
    let endpoint = Endpoint::from_shared(format!("http://{}", addr))
        .map_err(|e| WorkerTaskError::Connection {
            addr: addr.clone(),
            err: e,
        })?
        .http2_keep_alive_interval(ENDPOINT_KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(ENDPOINT_KEEP_ALIVE_TIMEOUT)
        .connect_timeout(CONNECT_TIMEOUT);

    let addr = addr.clone();
    Retry::spawn(connect_retry_strategy(), || async {
        endpoint.connect().await.map_err(|e| {
            debug!("retrying connection");
            WorkerTaskError::Connection {
                addr: addr.clone(),
                err: e,
            }
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

// Spawns an RPC call on a separate task so the active loop isn't blocked.
// The caller (QueryController) awaits the result via the oneshot in the Request.
macro_rules! send_wait_reply {
    ($addr:expr, $client:expr, $tx:expr, $method:ident, $req:expr) => {
        let addr: NetworkAddr = $addr.clone();
        let mut client = $client.clone();
        let span = tracing::Span::current();
        tokio::spawn(
            async move {
                let res = match tokio::time::timeout(RPC_TIMEOUT, client.$method($req)).await {
                    Ok(r) => r
                        .map(|resp| resp.into_inner())
                        .map_err(|status| WorkerTaskError::Grpc { addr, status }),
                    Err(_) => Err(WorkerTaskError::Grpc {
                        addr,
                        status: tonic::Status::deadline_exceeded("RPC timeout"),
                    }),
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
        Self {
            worker,
            catalog,
            registry,
        }
    }

    fn addr(&self) -> &NetworkAddr {
        &self.worker.host_addr
    }

    #[instrument(skip_all, fields(host_addr = %self.addr()))]
    pub(crate) async fn run(self) {
        loop {
            match connect(self.addr()).await {
                Ok(channel) => self.client(channel).await,
                Err(e) => {
                    error!("failed to connect: {e}");
                    self.catalog
                        .mark_worker(self.worker.clone().into(), WorkerState::Unreachable)
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
            .mark_worker(self.worker.clone().into(), WorkerState::Active)
            .await
        {
            warn!("failed to mark worker active: {e}");
            return;
        }
        info!("worker active");

        // Skip the first immediate tick so we don't health check before any RPC
        let mut health_interval = tokio::time::interval(HEALTH_CHECK_INTERVAL);
        health_interval.tick().await;

        loop {
            select! {
                // Cancellation-safe - no requests will be lost if we cancel future on health tick
                rpc = rpc_receiver.recv_async() => {
                    match rpc {
                        Ok(rpc) => self.send(&worker_client, rpc),
                        // All senders dropped — WorkerController removed this worker
                        Err(_) => {
                            debug!("RPC channel closed");
                            break;
                        }
                    }
                }
                _ = health_interval.tick() => {
                    let req = tonic::Request::new(health_proto::HealthCheckRequest::default());
                    if !matches!(tokio::time::timeout(PROBE_TIMEOUT, health_client.check(req)).await, Ok(Ok(_))) {
                        warn!("health check failed");
                        break;
                    }
                }
            }
        }

        self.catalog
            .mark_worker(self.worker.clone().into(), WorkerState::Unreachable)
            .await
            .ok();
        warn!("worker unreachable");
    }

    fn send(&self, client: &WorkerRpcServiceClient<Channel>, rpc: Rpc) {
        match rpc {
            Rpc::RegisterFragment(Request {
                payload: (id, plan_bytes),
                reply_to: tx,
            }) => {
                let plan = prost::Message::decode(plan_bytes.as_slice())
                    .unwrap_or_else(|_| worker_rpc_service::nes::SerializableQueryPlan {
                        query_id: query_id(id),
                        ..Default::default()
                    });
                send_wait_reply!(
                    self.addr(),
                    client,
                    tx,
                    register_query,
                    RegisterQueryRequest {
                        query_plan: Some(plan),
                    }
                );
            }
            Rpc::StartFragment(Request {
                payload: id,
                reply_to: tx,
            }) => {
                send_wait_reply!(
                    self.addr(),
                    client,
                    tx,
                    start_query,
                    StartQueryRequest {
                        query_id: query_id(id)
                    }
                );
            }
            Rpc::StopFragment(Request {
                payload: (id, stop_mode),
                reply_to: tx,
            }) => {
                send_wait_reply!(
                    self.addr(),
                    client,
                    tx,
                    stop_query,
                    StopQueryRequest {
                        query_id: query_id(id),
                        termination_type: stop_mode.into(),
                    }
                );
            }
            Rpc::GetFragmentStatus(Request {
                payload: id,
                reply_to: tx,
            }) => {
                send_wait_reply!(
                    self.addr(),
                    client,
                    tx,
                    request_query_status,
                    QueryStatusRequest {
                        query_id: query_id(id)
                    }
                );
            }
        }
    }
}
