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

use super::{CallError, WorkerClient};
use crate::config::{CONNECT_TIMEOUT, RPC_TIMEOUT};
use anyhow::Context;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use model::worker::endpoint::NetworkAddr;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Status};
use tracing::warn;
use worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use worker_rpc_service::{
    StartQueryRequest, StopQueryRequest, WorkerStatusRequest, WorkerStatusResponse,
};

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
    #[must_use]
    pub fn from_fragment_id(id: i64) -> Self {
        Self { id }
    }

    /// `None` for the invalid id 0.
    #[must_use]
    pub fn get_fragment_id(&self) -> Option<i64> {
        (self.id != 0).then_some(self.id)
    }
}

pub use worker_rpc_service::worker_rpc_service_client::WorkerRpcServiceClient;

/// The returned client stays usable after the worker went away, because its channel reconnects on the next call.
#[tracing::instrument(level = "info")]
pub(crate) async fn connect(addr: NetworkAddr) -> anyhow::Result<RemoteClient> {
    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .with_context(|| format!("failed to connect to {addr}"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(RPC_TIMEOUT)
        .connect()
        .await
        .inspect_err(|e| warn!(%addr, error = %e, "worker unreachable"))
        .with_context(|| format!("failed to connect to {addr}"))?;
    Ok(RemoteClient { channel, addr })
}

#[derive(Clone)]
pub(crate) struct RemoteClient {
    channel: Channel,
    addr: NetworkAddr,
}

impl WorkerClient for RemoteClient {
    #[tracing::instrument(level = "debug", skip(self, plan))]
    async fn start(&self, plan: SerializableQueryPlan) -> Result<(), CallError> {
        WorkerRpcServiceClient::new(self.channel.clone())
            .start_query(StartQueryRequest {
                query_plan: Some(plan),
            })
            .await
            .map(|_| ())
            .map_err(|status| {
                warn!(?status, "rpc error");
                classify(&self.addr, &status)
            })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
        WorkerRpcServiceClient::new(self.channel.clone())
            .stop_query(StopQueryRequest {
                query_id: Some(SerializableQueryId::from_fragment_id(*id)),
            })
            .await
            .map(|_| ())
            .map_err(|status| {
                warn!(?status, "rpc error");
                classify(&self.addr, &status)
            })
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_status(&self, after: u64) -> anyhow::Result<WorkerStatusResponse> {
        let request = WorkerStatusRequest {
            after_unix_timestamp_in_milli_seconds: after,
        };
        Ok(WorkerRpcServiceClient::new(self.channel.clone())
            .request_status(request)
            .await?
            .into_inner())
    }
}

fn classify(addr: &NetworkAddr, status: &Status) -> CallError {
    match status.code() {
        Code::AlreadyExists => CallError::AlreadyExists,
        code if is_retryable(code) => CallError::Transient(QueryFragmentError::Transport {
            msg: format!("gRPC error at '{addr}': {status}"),
        }),
        _ => CallError::Permanent(QueryFragmentError::Internal(CodedError::relayed(
            get_reported_code(status).unwrap_or(ErrorCode::UnknownException),
            status.message().to_string(),
            get_metadata(status, "trace"),
        ))),
    }
}

/// Codes that mean the call could not complete for now, not that the request failed.
/// `UNKNOWN` is excluded because it does not tell whether the failure is temporary.
/// tonic 0.13 reports a call that runs into the channel's timeout as `CANCELLED`.
const fn is_retryable(code: Code) -> bool {
    matches!(
        code,
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted | Code::Cancelled
    )
}

fn get_metadata(status: &Status, key: &str) -> String {
    status
        .metadata()
        .get(key)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn get_reported_code(status: &Status) -> Option<ErrorCode> {
    get_metadata(status, "code")
        .parse::<u16>()
        .ok()
        .map(ErrorCode::from_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    #[test]
    fn codes() {
        let addr = NetworkAddr::new("worker", 8080).unwrap();
        for status in [
            Status::unavailable("down"),
            Status::deadline_exceeded("slow"),
            Status::cancelled("timeout expired"),
        ] {
            assert!(matches!(
                classify(&addr, &status),
                CallError::Transient(QueryFragmentError::Transport { .. })
            ));
        }
        for status in [Status::internal("bug"), Status::unknown("unclear")] {
            assert!(matches!(classify(&addr, &status), CallError::Permanent(_)));
        }
        assert!(matches!(
            classify(&addr, &Status::already_exists("known id")),
            CallError::AlreadyExists
        ));
        let mut status = Status::internal("failed");
        let code = MetadataValue::from(ErrorCode::QueryCompilerError as u16);
        status.metadata_mut().insert("code", code);
        let CallError::Permanent(QueryFragmentError::Internal(coded)) = classify(&addr, &status)
        else {
            panic!("expected an internal error");
        };
        assert_eq!(
            coded.code,
            ErrorCode::QueryCompilerError,
            "the worker's code is kept"
        );
    }
}
