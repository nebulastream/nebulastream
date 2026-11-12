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
use crate::query_fragment::{CallError, RawStatus, Transport};
use crate::remote::worker_rpc_service;
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::error::{CodedError, ErrorCode};
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

fn classify(addr: &NetworkAddr, status: &tonic::Status) -> CallError {
    let code = reported_code(status);
    // A status read returns `NOT_FOUND` for an unknown id,
    // while a stop sends the worker's own code in the trailing metadata, so both count.
    if status.code() == tonic::Code::NotFound || code == Some(ErrorCode::QueryNotFound) {
        CallError::NotFound
    } else if status.code() == tonic::Code::AlreadyExists {
        CallError::AlreadyExists
    } else if retryable(status.code()) {
        CallError::Transient(QueryFragmentError::Transport {
            msg: format!("gRPC error at '{addr}': {status}"),
        })
    } else {
        CallError::Failed(QueryFragmentError::Internal(CodedError::relayed(
            code.unwrap_or(ErrorCode::UnknownException),
            status.message().to_string(),
            meta_str(status, "trace"),
        )))
    }
}

/// Codes that say the call could not complete for now, not that the request failed.
/// `UNKNOWN` is excluded because it does not say whether the failure will clear.
const fn retryable(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Aborted
            | tonic::Code::Cancelled
    )
}

fn meta_str(status: &tonic::Status, key: &str) -> String {
    status
        .metadata()
        .get(key)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// The code the worker attached to a failed call, if it attached one.
fn reported_code(status: &tonic::Status) -> Option<ErrorCode> {
    meta_str(status, "code")
        .parse::<u16>()
        .ok()
        .map(ErrorCode::from_code)
}

impl From<&worker_rpc_service::Error> for QueryFragmentError {
    fn from(err: &worker_rpc_service::Error) -> Self {
        let code =
            u16::try_from(err.code).map_or(ErrorCode::UnknownException, ErrorCode::from_code);
        Self::Internal(CodedError::relayed(
            code,
            err.message.clone(),
            err.stack_trace.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn classify(status: &tonic::Status) -> CallError {
        super::classify(&NetworkAddr::new("worker", 8080).unwrap(), status)
    }

    fn with_code(code: ErrorCode) -> tonic::Status {
        let mut status = tonic::Status::internal("failed");
        status.metadata_mut().insert(
            "code",
            MetadataValue::try_from((code as u16).to_string()).unwrap(),
        );
        status
    }

    #[test]
    fn transport_trouble_is_retryable_and_an_application_error_is_not() {
        for status in [
            tonic::Status::unavailable("down"),
            tonic::Status::deadline_exceeded("slow"),
            tonic::Status::cancelled("timeout expired"),
        ] {
            assert!(matches!(
                classify(&status),
                CallError::Transient(QueryFragmentError::Transport { .. })
            ));
        }
        for status in [
            tonic::Status::internal("bug"),
            tonic::Status::unknown("unclear"),
        ] {
            assert!(matches!(classify(&status), CallError::Failed(_)));
        }
    }

    #[test]
    fn both_spellings_of_an_unknown_query_count_as_not_found() {
        assert!(matches!(
            classify(&tonic::Status::not_found("gone")),
            CallError::NotFound
        ));
        assert!(matches!(
            classify(&with_code(ErrorCode::QueryNotFound)),
            CallError::NotFound
        ));
    }

    #[test]
    fn only_the_already_exists_status_counts_as_already_exists() {
        assert!(matches!(
            classify(&tonic::Status::already_exists("known id")),
            CallError::AlreadyExists
        ));
    }

    #[test]
    fn a_terminal_error_keeps_the_code_that_the_worker_reported() {
        let CallError::Failed(QueryFragmentError::Internal(coded)) =
            classify(&with_code(ErrorCode::QueryCompilerError))
        else {
            panic!("expected an internal error");
        };
        assert_eq!(coded.code, ErrorCode::QueryCompilerError);
    }
}
