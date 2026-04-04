use crate::error::{Retryable, WorkerTaskError};
use crate::fragment::{FragmentClient, FragmentStatus, Outcome};
use crate::remote::worker_rpc_service;
use model::identifier::FragmentId;
use model::query::fragment::{DesiredFragmentState, FragmentError, FragmentState, StopMode};
use model::worker::endpoint::NetworkAddr;
use std::future::Future;
use std::time::Duration;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::Channel;
use tracing::{debug, warn};

pub(super) use super::WorkerRpcServiceClient;

const RPC_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);
const RPC_TOTAL_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_RETRY_INIT: u64 = 50;
const RPC_MAX_RETRIES: usize = 5;

fn rpc_retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(RPC_RETRY_INIT)
        .factor(2)
        .map(jitter)
        .take(RPC_MAX_RETRIES)
}

fn query_id(id: i64) -> Option<worker_rpc_service::nes::SerializableQueryId> {
    Some(worker_rpc_service::nes::SerializableQueryId { id })
}

pub(super) struct RemoteFragmentClient {
    client: WorkerRpcServiceClient<Channel>,
    host_addr: NetworkAddr,
}

impl RemoteFragmentClient {
    pub(super) fn new(client: WorkerRpcServiceClient<Channel>, host_addr: NetworkAddr) -> Self {
        Self { client, host_addr }
    }

    async fn rpc<F, R>(&self, f: F) -> Result<R, WorkerTaskError>
    where
        F: Fn(
                WorkerRpcServiceClient<Channel>,
            ) -> std::pin::Pin<
                Box<dyn Future<Output = Result<tonic::Response<R>, tonic::Status>> + Send>,
            > + Clone
            + Send
            + 'static,
        R: Send + 'static,
    {
        let addr = self.host_addr.clone();
        tokio::time::timeout(RPC_TOTAL_TIMEOUT, {
            let client = self.client.clone();
            let addr = addr.clone();
            RetryIf::spawn(
                rpc_retry_strategy(),
                move || {
                    let client = client.clone();
                    let addr = addr.clone();
                    let func = f.clone();
                    async move {
                        match tokio::time::timeout(RPC_ATTEMPT_TIMEOUT, func(client)).await {
                            Ok(Ok(resp)) => Ok(resp.into_inner()),
                            Ok(Err(status)) => {
                                warn!("rpc error: {status:?}");
                                Err(WorkerTaskError::Grpc { addr, status })
                            }
                            Err(_) => {
                                warn!("rpc attempt timeout");
                                Err(WorkerTaskError::Grpc {
                                    addr,
                                    status: tonic::Status::deadline_exceeded("RPC attempt timeout"),
                                })
                            }
                        }
                    }
                },
                |err: &WorkerTaskError| err.retryable(),
            )
        })
        .await
        .unwrap_or_else(|_| {
            warn!("rpc total timeout");
            Err(WorkerTaskError::Grpc {
                addr,
                status: tonic::Status::deadline_exceeded("RPC total timeout"),
            })
        })
    }
}

impl FragmentClient for RemoteFragmentClient {
    #[tracing::instrument(level = "debug", skip(self, plan))]
    async fn register(&self, id: FragmentId, plan: &[u8]) -> Outcome {
        let mut query_plan: worker_rpc_service::nes::SerializableQueryPlan =
            prost::Message::decode(plan).unwrap_or_default();
        query_plan.query_id = query_id(*id);
        let req = worker_rpc_service::RegisterQueryRequest {
            query_plan: Some(query_plan),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.register_query(req).await })
            })
            .await
        {
            Ok(_) => Outcome::Transition(FragmentState::Registered),
            Err(err) if err.retryable() => Outcome::Retry(FragmentError::from(&err)),
            Err(err) if err.is_not_found() => Outcome::Transition(FragmentState::Pending),
            Err(err) => Outcome::Failed(FragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn start(&self, id: FragmentId) -> Outcome {
        let req = worker_rpc_service::StartQueryRequest {
            query_id: query_id(*id),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.start_query(req).await })
            })
            .await
        {
            Ok(_) => Outcome::Transition(FragmentState::Running),
            Err(err) if err.retryable() => Outcome::Retry(FragmentError::from(&err)),
            Err(err) if err.is_not_found() => Outcome::Transition(FragmentState::Pending),
            Err(err) => Outcome::Failed(FragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&self, id: FragmentId, mode: StopMode) -> Outcome {
        let req = worker_rpc_service::StopQueryRequest {
            query_id: query_id(*id),
            termination_type: i32::from(mode),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.stop_query(req).await })
            })
            .await
        {
            Ok(_) => Outcome::Accepted,
            Err(err) if err.is_not_found() => Outcome::Transition(FragmentState::Stopped),
            Err(err) => Outcome::Retry(FragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn poll(&self, id: FragmentId, desired_state: DesiredFragmentState) -> Outcome {
        let req = worker_rpc_service::QueryStatusRequest {
            query_id: query_id(*id),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.request_query_status(req).await })
            })
            .await
        {
            Ok(reply) => {
                let metrics = reply.metrics.as_ref();
                let status = FragmentStatus {
                    state: reply.state.into(),
                    start_timestamp: metrics.and_then(|m| m.start_unix_time_in_ms),
                    stop_timestamp: metrics.and_then(|m| m.stop_unix_time_in_ms),
                    error: metrics
                        .and_then(|m| m.error.as_ref())
                        .map(FragmentError::from),
                };
                debug!("{status:?}");
                Outcome::Status(status)
            }
            Err(err) if err.is_not_found() => match desired_state {
                DesiredFragmentState::Completed => Outcome::Transition(FragmentState::Pending),
                DesiredFragmentState::Stopped => Outcome::Transition(FragmentState::Stopped),
            },
            Err(err) => Outcome::Retry(FragmentError::from(&err)),
        }
    }
}
