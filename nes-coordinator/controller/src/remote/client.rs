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

use crate::config::{
    REMOTE_FRAGMENT_POLL_INTERVAL, RPC_ATTEMPT_TIMEOUT, RPC_MAX_RETRIES, RPC_RETRY_INIT,
    RPC_TOTAL_TIMEOUT,
};
use crate::error::{Retryable, WorkerTaskError};
use crate::fragment::{Client, Outcome, QueryFragmentStatus};
use crate::remote::worker_rpc_service;
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState, QueryFragmentTransition,
};
use model::worker::endpoint::NetworkAddr;
use std::future::Future;
use std::time::Duration;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::Channel;
use tracing::{debug, warn};

pub(super) use super::WorkerRpcServiceClient;

fn rpc_retry_strategy() -> impl Iterator<Item = Duration> {
    ExponentialBackoff::from_millis(RPC_RETRY_INIT)
        .factor(2)
        .map(jitter)
        .take(RPC_MAX_RETRIES)
}

fn query_id(id: i64) -> worker_rpc_service::nes::SerializableQueryId {
    worker_rpc_service::nes::SerializableQueryId::from_fragment_id(id)
}

/// Adapts a gRPC worker client to the async client interface used by the
/// lifecycle driver. Each method runs through a shared RPC layer that adds
/// a per-attempt timeout, an overall deadline, and bounded retries on
/// transient errors.
pub(super) struct QueryFragmentClient {
    client: WorkerRpcServiceClient<Channel>,
    host_addr: NetworkAddr,
}

impl QueryFragmentClient {
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
            Err(WorkerTaskError::Timeout { addr })
        })
    }
}

impl Client for QueryFragmentClient {
    fn poll_interval(&self) -> Duration {
        REMOTE_FRAGMENT_POLL_INTERVAL
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn start(&self, id: QueryFragmentId, plan: &[u8]) -> Outcome {
        let mut query_plan: worker_rpc_service::nes::SerializableQueryPlan =
            match prost::Message::decode(plan) {
                Ok(plan) => plan,
                Err(e) => {
                    return Outcome::Failed(QueryFragmentError::Internal {
                        code: 0,
                        msg: format!("failed to decode stored query plan: {e}"),
                        trace: String::new(),
                    });
                }
            };
        query_plan.query_id = Some(query_id(*id));
        let req = worker_rpc_service::StartQueryRequest {
            query_plan: Some(query_plan),
        };
        match self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.start_query(req).await })
            })
            .await
        {
            Ok(_) => Outcome::Transition(QueryFragmentTransition::Started),
            Err(err) if err.retryable() => Outcome::Retry(QueryFragmentError::from(&err)),
            Err(err) if err.is_not_found() => Outcome::Transition(QueryFragmentTransition::Pending),
            Err(err) => Outcome::Failed(QueryFragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&self, id: QueryFragmentId) -> Outcome {
        let req = worker_rpc_service::StopQueryRequest {
            query_id: Some(query_id(*id)),
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
                Outcome::Transition(QueryFragmentTransition::stopped_now())
            }
            Err(err) if err.retryable() => Outcome::Retry(QueryFragmentError::from(&err)),
            Err(err) => Outcome::Failed(QueryFragmentError::from(&err)),
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn observe(
        &self,
        id: QueryFragmentId,
        desired_state: DesiredQueryFragmentState,
    ) -> Outcome {
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
                let state = match QueryFragmentState::try_from(reply.state) {
                    Ok(state) => state,
                    Err(unknown) => {
                        return Outcome::Retry(QueryFragmentError::Transport {
                            msg: format!("worker reported unknown fragment state: {unknown}"),
                        });
                    }
                };
                let metrics = reply.metrics.as_ref();
                let status = QueryFragmentStatus {
                    state,
                    start_timestamp: metrics.and_then(|m| m.start_unix_time_in_ms),
                    stop_timestamp: metrics.and_then(|m| m.stop_unix_time_in_ms),
                    error: metrics
                        .and_then(|m| m.error.as_ref())
                        .map(QueryFragmentError::from),
                };
                debug!("{status:?}");
                Outcome::Status(status)
            }
            Err(err) if err.is_not_found() => match desired_state {
                DesiredQueryFragmentState::Completed => {
                    Outcome::Transition(QueryFragmentTransition::Pending)
                }
                DesiredQueryFragmentState::Stopped => {
                    Outcome::Transition(QueryFragmentTransition::stopped_now())
                }
            },
            Err(err) if err.retryable() => Outcome::Retry(QueryFragmentError::from(&err)),
            Err(err) => Outcome::Failed(QueryFragmentError::from(&err)),
        }
    }
}
