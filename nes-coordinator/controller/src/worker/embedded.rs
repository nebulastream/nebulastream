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

use crate::worker::transport::{WorkerTransport, serve};
use crate::worker::worker_registry::WorkerRegistry;
use crate::worker::worker_task::worker_rpc_service::nes::SerializableQueryId;
use crate::worker::worker_task::{
    HEALTH_CHECK_INTERVAL, QueryStatusReply, RPC_CHANNEL_CAPACITY, Request, Rpc, WorkerTaskError,
    worker_rpc_service,
};
use catalog::WorkerStateManager;
use model::query::StopMode;
use model::query::fragment::FragmentState;
use model::worker;
use model::worker::WorkerState;
use std::sync::Arc;
use tracing::{info, instrument, warn};

pub struct EmbeddedWorkerError {
    pub code: u16,
    pub msg: String,
    pub trace: String,
}

pub trait EmbeddedWorker: Send + Sync + 'static {
    fn register_fragment(&self, plan: Vec<u8>) -> Result<(), EmbeddedWorkerError>;
    fn start_fragment(&self, id: i64) -> Result<(), EmbeddedWorkerError>;
    fn stop_fragment(&self, id: i64, mode: StopMode) -> Result<(), EmbeddedWorkerError>;
    fn get_fragment_status(&self, id: i64) -> Result<FragmentState, EmbeddedWorkerError>;
}

pub trait EmbeddedWorkerFactory: Send + Sync + 'static {
    fn create(&self, config_json: &str) -> Arc<dyn EmbeddedWorker>;
}

/// Transport that dispatches RPCs directly to an in-process worker.
struct EmbeddedTransport {
    worker: Arc<dyn EmbeddedWorker>,
}

fn embedded_err(e: EmbeddedWorkerError) -> WorkerTaskError {
    WorkerTaskError::Embedded {
        code: e.code,
        msg: e.msg,
        trace: e.trace,
    }
}

impl WorkerTransport for EmbeddedTransport {
    fn dispatch(&self, rpc: Rpc) {
        let worker = self.worker.clone();
        tokio::task::spawn_blocking(move || match rpc {
            Rpc::RegisterFragment(Request {
                payload: (id, plan),
                reply_to: tx,
            }) => {
                let res = worker
                    .register_fragment(plan)
                    .map(|()| worker_rpc_service::RegisterQueryReply {
                        query_id: Some(SerializableQueryId { id }),
                    })
                    .map_err(embedded_err);
                let _ = tx.send(res);
            }
            Rpc::StartFragment(Request {
                payload: id,
                reply_to: tx,
            }) => {
                let res = worker.start_fragment(id).map_err(embedded_err);
                let _ = tx.send(res);
            }
            Rpc::StopFragment(Request {
                payload: (id, mode),
                reply_to: tx,
            }) => {
                let res = worker.stop_fragment(id, mode).map_err(embedded_err);
                let _ = tx.send(res);
            }
            Rpc::GetFragmentStatus(Request {
                payload: id,
                reply_to: tx,
            }) => {
                let res = worker
                    .get_fragment_status(id)
                    .map(|state| QueryStatusReply {
                        query_id: Some(SerializableQueryId { id }),
                        state: state.into(),
                        metrics: None,
                    })
                    .map_err(embedded_err);
                let _ = tx.send(res);
            }
        });
    }

    async fn health_check(&self) -> bool {
        // Embedded workers are always healthy if they exist.
        true
    }
}

pub(crate) struct EmbeddedWorkerTask {
    worker: worker::Model,
    handle: Arc<dyn EmbeddedWorker>,
    catalog: Arc<WorkerStateManager>,
    registry: WorkerRegistry,
}

impl EmbeddedWorkerTask {
    pub(crate) fn new(
        worker: worker::Model,
        handle: Arc<dyn EmbeddedWorker>,
        catalog: Arc<WorkerStateManager>,
        registry: WorkerRegistry,
    ) -> Self {
        Self {
            worker,
            handle,
            catalog,
            registry,
        }
    }

    #[instrument(skip_all, fields(host_addr = %self.worker.host_addr))]
    pub(crate) async fn run(self) {
        let addr = self.worker.host_addr.clone();
        let transport = EmbeddedTransport {
            worker: self.handle.clone(),
        };

        let (rpc_sender, rpc_receiver) = flume::bounded(RPC_CHANNEL_CAPACITY);
        let _guard = self.registry.register(addr, rpc_sender);

        if let Err(e) = self
            .catalog
            .mark_worker(self.worker.clone().into(), WorkerState::Active)
            .await
        {
            warn!("failed to mark embedded worker active: {e}");
            return;
        }
        info!("embedded worker active");

        serve(&transport, &rpc_receiver, HEALTH_CHECK_INTERVAL).await;

        info!("embedded worker task exiting");
    }
}
