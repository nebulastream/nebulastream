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

use model::query::fragment::FragmentError;
use model::worker::endpoint::NetworkAddr;
use thiserror::Error;

use crate::remote::worker_rpc_service::Error as ProtoError;

/// Classifies an error as transient (worth retrying) or terminal. Used by
/// the RPC layer to gate retries and by the lifecycle driver to decide
/// between a retry and a fatal failure.
pub(crate) trait Retryable {
    fn retryable(&self) -> bool;
}

/// Failures of a single RPC against an out-of-process worker: either the
/// transport never connected, or the request came back with a non-success
/// status. Each variant carries enough context to surface in logs and to map
/// cleanly into a persisted fragment-level error.
#[derive(Error, Debug)]
pub(crate) enum WorkerTaskError {
    #[error("failed to connect to {addr}: {err}")]
    Connection {
        addr: NetworkAddr,
        err: tonic::transport::Error,
    },

    #[error("gRPC error at {addr}: {status}")]
    Grpc {
        addr: NetworkAddr,
        status: tonic::Status,
    },
}

impl WorkerTaskError {
    pub(crate) fn is_not_found(&self) -> bool {
        matches!(
            self,
            Self::Grpc { status, .. } if status.code() == tonic::Code::NotFound
        )
    }
}

impl Retryable for WorkerTaskError {
    fn retryable(&self) -> bool {
        match self {
            Self::Connection { .. } => true,
            Self::Grpc { status, .. } => matches!(
                status.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unknown
                    | tonic::Code::Aborted
            ),
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

impl From<&ProtoError> for FragmentError {
    fn from(err: &ProtoError) -> Self {
        Self::Internal {
            code: u16::try_from(err.code).unwrap_or(u16::MAX),
            msg: err.message.clone(),
            trace: err.stack_trace.clone(),
        }
    }
}

impl From<&WorkerTaskError> for FragmentError {
    fn from(err: &WorkerTaskError) -> Self {
        match err {
            WorkerTaskError::Connection { addr, err } => Self::Transport {
                msg: format!("Connection to '{addr}' failed: {err}"),
            },
            WorkerTaskError::Grpc { addr, status }
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                ) =>
            {
                Self::Transport {
                    msg: format!("gRPC error at '{addr}': {status}"),
                }
            }
            WorkerTaskError::Grpc { status, .. } => Self::Internal {
                code: meta_str(status, "code").parse().unwrap_or(0),
                msg: status.message().to_string(),
                trace: meta_str(status, "trace"),
            },
        }
    }
}
