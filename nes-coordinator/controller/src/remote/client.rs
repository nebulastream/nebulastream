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

use crate::config::REMOTE_FRAGMENT_POLL_INTERVAL;
use crate::error::classify;
use crate::query_fragment::{CallError, RawStatus, Transport};
use crate::remote::worker_rpc_service;
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use model::worker::endpoint::NetworkAddr;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::{debug, warn};

pub(super) use super::WorkerRpcServiceClient;

/// The transport to a worker behind a socket.
#[derive(Clone)]
pub(crate) struct RemoteTransport {
    channel: Channel,
    host_addr: NetworkAddr,
}

impl RemoteTransport {
    pub(crate) const fn new(channel: Channel, host_addr: NetworkAddr) -> Self {
        Self { channel, host_addr }
    }

    pub(super) fn client(&self) -> WorkerRpcServiceClient<Channel> {
        WorkerRpcServiceClient::new(self.channel.clone())
    }

    pub(super) const fn channel(&self) -> &Channel {
        &self.channel
    }

    fn call_error(&self, status: &tonic::Status) -> CallError {
        warn!(?status, "rpc error");
        classify(&self.host_addr, status)
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
        self.client()
            .start_query(req)
            .await
            .map(|_| ())
            .map_err(|status| self.call_error(&status))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
        let req = worker_rpc_service::StopQueryRequest {
            query_id: Some(SerializableQueryId::from_fragment_id(*id)),
        };
        self.client()
            .stop_query(req)
            .await
            .map(|_| ())
            .map_err(|status| self.call_error(&status))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn status(&self, id: QueryFragmentId) -> Result<RawStatus, CallError> {
        let req = worker_rpc_service::QueryStatusRequest {
            query_id: Some(SerializableQueryId::from_fragment_id(*id)),
        };
        let reply = self
            .client()
            .request_query_status(req)
            .await
            .map_err(|status| self.call_error(&status))?
            .into_inner();
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
