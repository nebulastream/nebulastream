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

use crate::worker::worker_task::{Rpc, WorkerTaskError};
use catalog::error::Retryable;
use model::query::fragment::FragmentError;
use model::worker::endpoint::NetworkAddr;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum WorkerError {
    #[error("Worker client '{0}' unavailable")]
    ClientUnavailable(NetworkAddr),

    #[error("RPC error")]
    ClientError(#[from] WorkerTaskError),

    #[error("Worker '{0}' has been removed")]
    WorkerRemoved(NetworkAddr),
}

impl WorkerError {
    pub(crate) fn fragment_not_found(&self) -> bool {
        matches!(
            self,
            Self::ClientError(WorkerTaskError::Grpc { status, .. })
            if status.code() == tonic::Code::NotFound
        )
    }
}

impl Retryable for WorkerError {
    fn retryable(&self) -> bool {
        match self {
            Self::ClientUnavailable(_) | Self::ClientError(WorkerTaskError::Connection { .. }) => {
                true
            }
            Self::ClientError(WorkerTaskError::Grpc { status, .. }) => matches!(
                status.code(),
                tonic::Code::Unavailable
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Unknown
                    | tonic::Code::Aborted
            ),
            Self::WorkerRemoved(_) => false,
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

impl From<WorkerError> for FragmentError {
    fn from(e: WorkerError) -> Self {
        match e {
            WorkerError::ClientUnavailable(addr) => FragmentError::WorkerCommunication {
                msg: format!("Worker '{addr}' unavailable"),
            },
            WorkerError::WorkerRemoved(addr) => FragmentError::WorkerCommunication {
                msg: format!("Worker '{addr}' removed"),
            },
            WorkerError::ClientError(WorkerTaskError::Connection { addr, err }) => {
                FragmentError::WorkerCommunication {
                    msg: format!("Connection to '{addr}' failed: {err}"),
                }
            }
            WorkerError::ClientError(WorkerTaskError::Grpc { addr, status })
                if matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                ) =>
            {
                FragmentError::WorkerCommunication {
                    msg: format!("gRPC error at '{addr}': {status}"),
                }
            }
            WorkerError::ClientError(WorkerTaskError::Grpc { status, .. }) => {
                FragmentError::WorkerInternal {
                    code: meta_str(&status, "code").parse().unwrap_or(0),
                    msg: status.message().to_string(),
                    trace: meta_str(&status, "trace"),
                }
            }
        }
    }
}

pub struct WorkerRegistryHandle {
    shared: Arc<RwLock<HashMap<NetworkAddr, flume::Sender<Rpc>>>>,
}

impl Clone for WorkerRegistryHandle {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl WorkerRegistryHandle {
    pub(crate) async fn send(
        &self,
        addr: &NetworkAddr,
        rpc: Rpc,
    ) -> Result<(), WorkerError> {
        let sender: Result<flume::Sender<Rpc>, WorkerError> = {
            let workers = self
                .shared
                .read()
                .expect("no one should panic while holding this lock");

            workers
                .get(addr)
                .cloned()
                .ok_or_else(|| WorkerError::ClientUnavailable(addr.clone()))
        };

        sender?
            .send_async(rpc)
            .await
            .map_err(|_| WorkerError::ClientUnavailable(addr.clone()))
    }

}

#[derive(Default)]
pub struct WorkerRegistry {
    shared: Arc<RwLock<HashMap<NetworkAddr, flume::Sender<Rpc>>>>,
}

impl Clone for WorkerRegistry {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl WorkerRegistry {
    pub fn handle(&self) -> WorkerRegistryHandle {
        WorkerRegistryHandle {
            shared: self.shared.clone(),
        }
    }

    pub(crate) fn register(&self, addr: NetworkAddr, sender: flume::Sender<Rpc>) -> RegistryGuard {
        self.shared
            .write()
            .expect("no one should panic while holding this lock")
            .insert(addr.clone(), sender);
        RegistryGuard {
            registry: self.clone(),
            addr,
        }
    }

    fn unregister(&self, addr: &NetworkAddr) {
        self.shared
            .write()
            .expect("no one should panic while holding this lock")
            .remove(addr);
    }
}

pub(crate) struct RegistryGuard {
    registry: WorkerRegistry,
    addr: NetworkAddr,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        self.registry.unregister(&self.addr);
    }
}
