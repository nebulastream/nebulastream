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

use super::{InProcessWorker, run_blocking};
use crate::remote::worker_rpc_service::WorkerStatusResponse;
use crate::remote::worker_rpc_service::nes::SerializableQueryPlan;
use crate::worker_task::{CallError, Transport};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use std::sync::Arc;

/// A call to a worker in this process never fails transiently.
fn classify(error: QueryFragmentError) -> CallError {
    match &error {
        QueryFragmentError::Internal(coded) if coded.code == ErrorCode::QueryAlreadyRegistered => {
            CallError::AlreadyExists
        }
        _ => CallError::Failed(error),
    }
}

async fn call<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, QueryFragmentError> + Send + 'static,
) -> Result<T, CallError> {
    run_blocking(f)
        .await
        .unwrap_or_else(|join_err| {
            Err(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::UnknownException,
                join_err,
            )))
        })
        .map_err(classify)
}

#[derive(Clone)]
pub(crate) struct InProcessTransport {
    worker: Arc<dyn InProcessWorker>,
}

impl InProcessTransport {
    pub(crate) fn new(worker: Arc<dyn InProcessWorker>) -> Self {
        Self { worker }
    }
}

impl Transport for InProcessTransport {
    async fn start(&self, plan: SerializableQueryPlan) -> Result<(), CallError> {
        let prepared = prost::Message::encode_to_vec(&plan);
        let worker = self.worker.clone();
        call(move || worker.start_query_fragment(prepared)).await
    }

    async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
        let worker = self.worker.clone();
        let id = *id;
        call(move || worker.stop_query_fragment(id)).await
    }

    async fn report(&self, after: u64) -> anyhow::Result<WorkerStatusResponse> {
        let worker = self.worker.clone();
        let bytes = run_blocking(move || worker.worker_status(after)).await??;
        Ok(prost::Message::decode(bytes.as_slice())?)
    }
}
