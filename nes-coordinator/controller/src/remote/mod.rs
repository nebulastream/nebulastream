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

//! Runs the fragments of a worker in another process over gRPC.

mod client;

use crate::config::{
    CONNECT_MAX_DELAY, CONNECT_MAX_RETRIES, CONNECT_TIMEOUT, ENDPOINT_KEEP_ALIVE_INTERVAL,
    ENDPOINT_KEEP_ALIVE_TIMEOUT, HEALTH_CHECK_INTERVAL, PROBE_TIMEOUT, RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_FACTOR_MS,
};
use crate::error::WorkerTaskError;
use crate::worker_task::Backend;
use client::RemoteTransport;
use model::worker::endpoint::NetworkAddr;
use std::time::Duration;
use tokio_retry::Retry;
use tokio_retry::strategy::{ExponentialBackoff, jitter};
use tonic::transport::{Channel, Endpoint};
use tracing::warn;

pub mod worker_rpc_service {
    pub mod nes {
        tonic::include_proto!("nes");

        impl SerializableQueryId {
            /// The wire format has two string ids
            /// while the coordinator identifies fragments by one integer.
            /// Put the integer in the local id, rendered as a decimal string,
            /// until the wire format itself moves to an integer id.
            pub fn from_fragment_id(id: i64) -> Self {
                Self {
                    local_query_id: id.to_string(),
                    distributed_query_id: String::new(),
                }
            }
        }
    }
    tonic::include_proto!("_");
}

pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use health_proto::health_client::HealthClient;
pub use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

fn connect_retry_strategy() -> impl Iterator<Item = Duration> {
    // Same arithmetic as the RPC schedule: base to the power of n, times the factor.
    ExponentialBackoff::from_millis(RETRY_BACKOFF_BASE)
        .factor(RETRY_BACKOFF_FACTOR_MS)
        .max_delay(CONNECT_MAX_DELAY)
        .map(jitter)
        .take(CONNECT_MAX_RETRIES)
}

#[tracing::instrument(level = "info")]
async fn connect(addr: &NetworkAddr) -> Result<Channel, WorkerTaskError> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
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
            warn!("worker {addr} unreachable: {e}");
            WorkerTaskError::Connection {
                addr: addr.clone(),
                err: e,
            }
        })
    })
    .await
}

async fn health_check(client: &HealthClient<Channel>) -> bool {
    let mut client = client.clone();
    let req = tonic::Request::new(health_proto::HealthCheckRequest::default());
    matches!(
        tokio::time::timeout(PROBE_TIMEOUT, client.check(req)).await,
        Ok(Ok(_))
    )
}

pub(crate) struct RemoteBackend;

impl Backend for RemoteBackend {
    type Link = Channel;
    type Transport = RemoteTransport;

    async fn connect(&self, addr: &NetworkAddr) -> anyhow::Result<Channel> {
        connect(addr).await.map_err(|err| {
            anyhow::anyhow!("failed to connect after {CONNECT_MAX_RETRIES} attempts: {err}")
        })
    }

    fn transport(&self, link: &Channel, addr: &NetworkAddr) -> RemoteTransport {
        RemoteTransport::new(WorkerRpcServiceClient::new(link.clone()), addr.clone())
    }

    fn heartbeat(&self) -> Option<Duration> {
        Some(HEALTH_CHECK_INTERVAL)
    }

    async fn healthy(&self, link: &Channel) -> bool {
        health_check(&HealthClient::new(link.clone())).await
    }
}
