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
    REMOTE_FRAGMENT_POLL_INTERVAL, RETRY_BACKOFF_BASE, RETRY_BACKOFF_FACTOR_MS,
    RPC_ATTEMPT_TIMEOUT, RPC_MAX_RETRIES, RPC_TOTAL_TIMEOUT,
};
use crate::error::{Retryable, WorkerTaskError};
use crate::query_fragment::{CallError, RawStatus, Transport};
use crate::remote::worker_rpc_service;
use crate::remote::worker_rpc_service::nes::SerializableQueryPlan;
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use model::worker::endpoint::NetworkAddr;
use std::future::Future;
use std::time::Duration;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::Channel;
use tracing::{debug, warn};

pub(super) use super::WorkerRpcServiceClient;

fn rpc_retry_strategy() -> impl Iterator<Item = Duration> {
    // The argument is the growth rate, not the first delay:
    // the n-th delay is base to the power of n, times the factor.
    ExponentialBackoff::from_millis(RETRY_BACKOFF_BASE)
        .factor(RETRY_BACKOFF_FACTOR_MS)
        .map(jitter)
        .take(RPC_MAX_RETRIES)
}

fn query_id(id: i64) -> worker_rpc_service::nes::SerializableQueryId {
    worker_rpc_service::nes::SerializableQueryId::from_fragment_id(id)
}

/// The transport to a worker behind a socket.
/// Each call runs through a shared RPC layer
/// that adds a per-attempt timeout, an overall deadline, and bounded retries on transient errors.
pub(crate) struct RemoteTransport {
    client: WorkerRpcServiceClient<Channel>,
    host_addr: NetworkAddr,
}

impl RemoteTransport {
    pub(crate) const fn new(
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

/// A worker behind a socket can fail to answer for a while; those failures are retried.
/// An unknown id is state rather than failure; anything else failed for good.
fn classify(err: WorkerTaskError) -> CallError {
    if err.is_not_found() {
        CallError::NotFound
    } else if err.retryable() {
        CallError::Transient(QueryFragmentError::from(&err))
    } else {
        CallError::Failed(QueryFragmentError::from(&err))
    }
}

impl Transport for RemoteTransport {
    fn poll_interval(&self) -> Duration {
        REMOTE_FRAGMENT_POLL_INTERVAL
    }

    #[tracing::instrument(level = "debug", skip(self, plan))]
    async fn start(&self, plan: SerializableQueryPlan) -> Result<(), CallError> {
        let req = worker_rpc_service::StartQueryRequest {
            query_plan: Some(plan),
        };
        self.rpc(move |mut client| {
            let req = req.clone();
            Box::pin(async move { client.start_query(req).await })
        })
        .await
        .map(|_| ())
        .map_err(classify)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
        let req = worker_rpc_service::StopQueryRequest {
            query_id: Some(query_id(*id)),
        };
        self.rpc(move |mut client| {
            let req = req.clone();
            Box::pin(async move { client.stop_query(req).await })
        })
        .await
        .map_err(classify)
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn status(&self, id: QueryFragmentId) -> Result<RawStatus, CallError> {
        let req = worker_rpc_service::QueryStatusRequest {
            query_id: Some(query_id(*id)),
        };
        let reply = self
            .rpc(move |mut client| {
                let req = req.clone();
                Box::pin(async move { client.request_query_status(req).await })
            })
            .await
            .map_err(classify)?;
        let metrics = reply.metrics.as_ref();
        let status = RawStatus {
            state: reply.state,
            start_ms: metrics.and_then(|m| m.start_unix_time_in_ms),
            stop_ms: metrics.and_then(|m| m.stop_unix_time_in_ms),
            error: metrics
                .and_then(|m| m.error.as_ref())
                .map(QueryFragmentError::from),
        };
        debug!(state = status.state, "status");
        Ok(status)
    }
}
