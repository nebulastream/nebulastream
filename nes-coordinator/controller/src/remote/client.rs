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

use super::WorkerRpcServiceClient;
use crate::calls::{CallError, Transport};
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use crate::remote::worker_rpc_service::{
    StartQueryRequest, StopQueryRequest, WorkerStatusRequest, WorkerStatusResponse,
};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use model::worker::endpoint::NetworkAddr;
use tonic::transport::Channel;
use tonic::{Code, Status};
use tracing::warn;

#[derive(Clone)]
pub(crate) struct RemoteTransport {
    channel: Channel,
    addr: NetworkAddr,
}

impl RemoteTransport {
    pub(crate) const fn new(channel: Channel, addr: NetworkAddr) -> Self {
        Self { channel, addr }
    }
}

impl Transport for RemoteTransport {
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

/// The code that the worker attached to a failed call, if it attached one.
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

    fn classify(status: &Status) -> CallError {
        super::classify(&NetworkAddr::new("worker", 8080).unwrap(), status)
    }

    #[test]
    fn retryable_codes() {
        for status in [
            Status::unavailable("down"),
            Status::deadline_exceeded("slow"),
            Status::cancelled("timeout expired"),
        ] {
            assert!(matches!(
                classify(&status),
                CallError::Transient(QueryFragmentError::Transport { .. })
            ));
        }
        for status in [Status::internal("bug"), Status::unknown("unclear")] {
            assert!(matches!(classify(&status), CallError::Permanent(_)));
        }
        assert!(matches!(
            classify(&Status::already_exists("known id")),
            CallError::AlreadyExists
        ));
    }

    #[test]
    fn failed_keeps_worker_code() {
        let mut status = Status::internal("failed");
        let code = MetadataValue::from(ErrorCode::QueryCompilerError as u16);
        status.metadata_mut().insert("code", code);
        let CallError::Permanent(QueryFragmentError::Internal(coded)) = classify(&status) else {
            panic!("expected an internal error");
        };
        assert_eq!(coded.code, ErrorCode::QueryCompilerError);
    }
}
