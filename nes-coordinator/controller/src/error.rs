use model::query::fragment::FragmentError;
use model::worker::endpoint::NetworkAddr;
use thiserror::Error;

use crate::remote::worker_rpc_service::Error as ProtoError;

pub(crate) trait Retryable {
    fn retryable(&self) -> bool;
}

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
        FragmentError::Internal {
            code: err.code as u16,
            msg: err.message.clone(),
            trace: err.stack_trace.clone(),
        }
    }
}

impl From<&WorkerTaskError> for FragmentError {
    fn from(err: &WorkerTaskError) -> Self {
        match err {
            WorkerTaskError::Connection { addr, err } => FragmentError::Transport {
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
                FragmentError::Transport {
                    msg: format!("gRPC error at '{addr}': {status}"),
                }
            }
            WorkerTaskError::Grpc { status, .. } => FragmentError::Internal {
                code: meta_str(status, "code").parse().unwrap_or(0),
                msg: status.message().to_string(),
                trace: meta_str(status, "trace"),
            },
        }
    }
}
