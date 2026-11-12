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

use crate::config::{CONNECT_TIMEOUT, HEALTH_CHECK_INTERVAL, RPC_TIMEOUT};
use crate::worker_task::{ActiveFragments, Backend};
use anyhow::Context;
use client::RemoteTransport;
use model::identifier::QueryFragmentId;
use model::worker::endpoint::NetworkAddr;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};
use tracing::warn;

#[allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::result_large_err,
    reason = "generated code"
)]
pub mod worker_rpc_service {
    pub mod nes {
        tonic::include_proto!("nes");
    }
    tonic::include_proto!("_");
}

impl worker_rpc_service::nes::SerializableQueryId {
    /// The wire format has two string ids
    /// while the coordinator identifies fragments by one integer.
    /// Put the integer in the local id, rendered as a decimal string,
    /// until the wire format itself moves to an integer id.
    #[must_use]
    pub fn from_fragment_id(id: i64) -> Self {
        Self {
            local_query_id: id.to_string(),
            distributed_query_id: String::new(),
        }
    }

    /// Reverse of the constructor above; an id that did not come from a fragment id parses to `None`.
    #[must_use]
    pub fn fragment_id(&self) -> Option<i64> {
        self.local_query_id.parse().ok()
    }
}

#[allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::result_large_err,
    reason = "generated code"
)]
pub mod health_proto {
    tonic::include_proto!("grpc.health.v1");
}

use health_proto::health_check_response::ServingStatus;
use health_proto::health_client::HealthClient;
pub use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

#[tracing::instrument(level = "info")]
async fn connect(addr: &NetworkAddr) -> anyhow::Result<Channel> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .with_context(|| format!("failed to connect to {addr}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT);

    endpoint
        .connect()
        .await
        .inspect_err(|e| warn!(%addr, error = %e, "worker unreachable"))
        .with_context(|| format!("failed to connect to {addr}"))
}

async fn health_check(client: &HealthClient<Channel>) -> bool {
    let mut client = client.clone();
    let req = tonic::Request::new(health_proto::HealthCheckRequest::default());
    client
        .check(req)
        .await
        .is_ok_and(|response| response.into_inner().status == ServingStatus::Serving as i32)
}

pub(crate) struct RemoteBackend;

impl Backend for RemoteBackend {
    type Transport = RemoteTransport;

    const HEARTBEAT: Option<Duration> = Some(HEALTH_CHECK_INTERVAL);

    async fn connect(&self, addr: &NetworkAddr) -> anyhow::Result<RemoteTransport> {
        Ok(RemoteTransport::new(connect(addr).await?, addr.clone()))
    }

    async fn healthy(&self, transport: &RemoteTransport) -> bool {
        health_check(&HealthClient::new(transport.channel().clone())).await
    }

    async fn active_fragments(
        &self,
        transport: &RemoteTransport,
        after: u64,
    ) -> anyhow::Result<ActiveFragments> {
        let request = worker_rpc_service::WorkerStatusRequest {
            after_unix_timestamp_in_milli_seconds: after,
        };
        let reply = transport
            .client()
            .request_status(request)
            .await?
            .into_inner();
        let fragments = reply
            .active_queries
            .into_iter()
            .filter_map(|query| {
                let id = query.query_id.as_ref().and_then(|id| id.fragment_id());
                if id.is_none() {
                    warn!(query_id = ?query.query_id, "worker lists an active query without a fragment id");
                }
                Some((
                    QueryFragmentId::new(id?),
                    query.started_unix_timestamp_in_milli_seconds,
                ))
            })
            .collect();
        Ok(ActiveFragments {
            fragments,
            until: reply.until_unix_timestamp_in_milli_seconds,
        })
    }
}
