use crate::error::{Retryable, WorkerTaskError};
use crate::fragment::{Client, Outcome, Status};
use crate::remote::worker_rpc_service;
use model::identifier::FragmentId;
use model::query::fragment::{DesiredFragmentState, FragmentError, FragmentTransition, StopMode};
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

fn query_id(id: i64) -> worker_rpc_service::nes::SerializableQueryId {
    worker_rpc_service::nes::SerializableQueryId { id }
}

/// Adapts a gRPC worker client to the async client interface used by the
/// lifecycle driver. Each method runs through a shared RPC layer that adds
/// a per-attempt timeout, an overall deadline, and bounded retries on
/// transient errors.
pub(super) struct FragmentClient {
    client: WorkerRpcServiceClient<Channel>,
    host_addr: NetworkAddr,
}

impl FragmentClient {
    pub(super) const fn new(
        client: WorkerRpcServiceClient<Channel>,
        host_addr: NetworkAddr,
    ) -> Self {
        Self { client, host_addr }
    }

    #[allow(clippy::result_large_err)]
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

impl Client for FragmentClient {
    #[tracing::instrument(level = "debug", skip(self, plan))]
    async fn register(&self, id: FragmentId, plan: &[u8]) -> Outcome {
        let mut query_plan: worker_rpc_service::nes::SerializableQueryPlan =
            match prost::Message::decode(plan) {
                Ok(plan) => plan,
                Err(e) => {
                    return Outcome::Failed(FragmentError::Internal {
                        code: 0,
                        msg: format!("failed to decode stored query plan: {e}"),
                        trace: String::new(),
                    });
                }
            };
        query_plan.query_id = Some(query_id(*id));
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
            Ok(_) => Outcome::Transition(FragmentTransition::Registered),
            Err(err) if err.retryable() => Outcome::Retry(FragmentError::from(&err)),
            Err(err) if err.is_not_found() => Outcome::Transition(FragmentTransition::Pending),
            Err(err) => Outcome::Failed(FragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn start(&self, id: FragmentId) -> Outcome {
        let req = worker_rpc_service::StartQueryRequest {
            query_id: Some(query_id(*id)),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.start_query(req).await })
            })
            .await
        {
            Ok(()) => Outcome::Transition(FragmentTransition::running_now()),
            Err(err) if err.retryable() => Outcome::Retry(FragmentError::from(&err)),
            Err(err) if err.is_not_found() => Outcome::Transition(FragmentTransition::Pending),
            Err(err) => Outcome::Failed(FragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&self, id: FragmentId, mode: StopMode) -> Outcome {
        let req = worker_rpc_service::StopQueryRequest {
            query_id: Some(query_id(*id)),
            termination_type: i32::from(mode),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.stop_query(req).await })
            })
            .await
        {
            Ok(()) => Outcome::Accepted,
            Err(err) if err.is_not_found() => {
                Outcome::Transition(FragmentTransition::stopped_now())
            }
            Err(err) => Outcome::Retry(FragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn poll(&self, id: FragmentId, desired_state: DesiredFragmentState) -> Outcome {
        let req = worker_rpc_service::QueryStatusRequest {
            query_id: Some(query_id(*id)),
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
                let status = Status {
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
                DesiredFragmentState::Completed => Outcome::Transition(FragmentTransition::Pending),
                DesiredFragmentState::Stopped => {
                    Outcome::Transition(FragmentTransition::stopped_now())
                }
            },
            Err(err) => Outcome::Retry(FragmentError::from(&err)),
        }
    }
}
