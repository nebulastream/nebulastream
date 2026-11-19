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

//! Reaches a worker in another process over gRPC.

mod client;

use crate::config::{CONNECT_TIMEOUT, RPC_TIMEOUT};
use anyhow::Context;
use client::RemoteTransport;
use model::worker::endpoint::NetworkAddr;
use tonic::transport::Endpoint;
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
    /// The wire format has two string ids, while the coordinator identifies a fragment by one integer.
    /// The integer becomes the local id as a decimal string, and the distributed id stays empty.
    #[must_use]
    pub fn from_fragment_id(id: i64) -> Self {
        Self {
            local_query_id: id.to_string(),
            distributed_query_id: String::new(),
        }
    }

    #[must_use]
    pub fn fragment_id(&self) -> Option<i64> {
        self.local_query_id.parse().ok()
    }
}

pub use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

/// The channel dials again on the next call after the worker went away, so a connection is made once per worker loop.
#[tracing::instrument(level = "info")]
pub(crate) async fn connect(addr: NetworkAddr) -> anyhow::Result<RemoteTransport> {
    let endpoint = Endpoint::from_shared(format!("http://{addr}"))
        .with_context(|| format!("failed to connect to {addr}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT);

    let channel = endpoint
        .connect()
        .await
        .inspect_err(|e| warn!(%addr, error = %e, "worker unreachable"))
        .with_context(|| format!("failed to connect to {addr}"))?;
    Ok(RemoteTransport::new(channel, addr))
}
