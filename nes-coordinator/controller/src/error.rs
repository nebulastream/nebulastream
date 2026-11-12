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

//! Error types for a single worker RPC and the classification of which failures are worth retrying.

use model::error::{CodedError, ErrorCode};
use model::query::query_fragment::QueryFragmentError;
use model::worker::endpoint::NetworkAddr;
use thiserror::Error;

use crate::remote::worker_rpc_service::Error as ProtoError;

/// Failures of a single RPC against an out-of-process worker.
/// Each variant has enough context to appear in logs and to map into a persisted query_fragment-level error.
#[derive(Error, Debug)]
pub(crate) enum WorkerTaskError {
    #[error("failed to connect to {addr}: {err}")]
    Connection {
        addr: NetworkAddr,
        #[source]
        err: tonic::transport::Error,
    },

    #[error("gRPC error at {addr}: {status}")]
    Grpc {
        addr: NetworkAddr,
        status: tonic::Status,
    },
}

impl WorkerTaskError {
    /// A status read answers `NOT_FOUND` for an unknown id, while a stop reports the worker's own
    /// code for it in the trailing metadata, so both spellings count.
    pub(crate) fn is_not_found(&self) -> bool {
        match self {
            Self::Grpc { status, .. } => {
                status.code() == tonic::Code::NotFound
                    || reported_code(status) == Some(ErrorCode::QueryNotFound)
            }
            Self::Connection { .. } => false,
        }
    }

    pub(crate) fn is_already_exists(&self) -> bool {
        match self {
            Self::Grpc { status, .. } => status.code() == tonic::Code::AlreadyExists,
            Self::Connection { .. } => false,
        }
    }
}

/// gRPC status codes worth retrying:
/// the transport or peer could not complete the call for now, rather than a terminal application error.
/// Unknown is left out because it does not say whether the failure will clear up.
const fn retryable(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Aborted
            | tonic::Code::Cancelled
    )
}

impl WorkerTaskError {
    /// Classifies an error as transient (worth retrying) or terminal.
    pub(crate) fn retryable(&self) -> bool {
        match self {
            // A failed connection is a prolonged but still transient outage:
            // retry at the query_fragment level instead of giving the query_fragment up.
            // The query_fragment retry budget bounds it.
            Self::Connection { .. } => true,
            Self::Grpc { status, .. } => retryable(status.code()),
        }
    }
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

impl From<&ProtoError> for QueryFragmentError {
    fn from(err: &ProtoError) -> Self {
        let code =
            u16::try_from(err.code).map_or(ErrorCode::UnknownException, ErrorCode::from_code);
        Self::Internal(CodedError::relayed(
            code,
            err.message.clone(),
            err.stack_trace.clone(),
        ))
    }
}

impl From<&WorkerTaskError> for QueryFragmentError {
    fn from(err: &WorkerTaskError) -> Self {
        match err {
            WorkerTaskError::Connection { addr, err } => Self::Transport {
                msg: format!("Connection to '{addr}' failed: {err}"),
            },
            WorkerTaskError::Grpc { addr, status } if retryable(status.code()) => Self::Transport {
                msg: format!("gRPC error at '{addr}': {status}"),
            },
            WorkerTaskError::Grpc { status, .. } => Self::Internal(CodedError::relayed(
                reported_code(status).unwrap_or(ErrorCode::UnknownException),
                status.message().to_string(),
                meta_str(status, "trace"),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn addr() -> NetworkAddr {
        NetworkAddr::new("worker", 8080).unwrap()
    }

    fn grpc(status: tonic::Status) -> WorkerTaskError {
        WorkerTaskError::Grpc {
            addr: addr(),
            status,
        }
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
        assert!(grpc(tonic::Status::unavailable("down")).retryable());
        assert!(grpc(tonic::Status::deadline_exceeded("slow")).retryable());
        assert!(grpc(tonic::Status::cancelled("timeout expired")).retryable());
        assert!(!grpc(tonic::Status::internal("bug")).retryable());
        assert!(!grpc(tonic::Status::unknown("unclear")).retryable());
    }

    #[test]
    fn both_spellings_of_an_unknown_query_count_as_not_found() {
        assert!(grpc(tonic::Status::not_found("gone")).is_not_found());
        assert!(grpc(with_code(ErrorCode::QueryNotFound)).is_not_found());
        assert!(!grpc(tonic::Status::internal("bug")).is_not_found());
    }

    #[test]
    fn only_the_already_exists_status_counts_as_already_exists() {
        assert!(grpc(tonic::Status::already_exists("known id")).is_already_exists());
        assert!(!grpc(tonic::Status::internal("bug")).is_already_exists());
        assert!(!grpc(tonic::Status::not_found("gone")).is_already_exists());
    }

    #[test]
    fn a_terminal_error_keeps_the_code_that_the_worker_reported() {
        let err = grpc(with_code(ErrorCode::QueryNotFound));
        let QueryFragmentError::Internal(coded) = QueryFragmentError::from(&err) else {
            panic!("expected an internal error");
        };
        assert_eq!(coded.code, ErrorCode::QueryNotFound);

        let err = grpc(tonic::Status::unavailable("down"));
        assert!(matches!(
            QueryFragmentError::from(&err),
            QueryFragmentError::Transport { .. }
        ));
    }
}
