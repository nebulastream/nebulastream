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
use crate::config::EMBEDDED_FRAGMENT_POLL_INTERVAL;
use crate::query_fragment::{CallError, RawStatus, Transport};
use crate::remote::worker_rpc_service::nes::SerializableQueryPlan;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use std::sync::Arc;
use std::time::Duration;

/// A call to a worker in this process never fails transiently.
/// An unknown id is state, not failure: the status log is append-only and gets its first entry before any work is queued,
/// so the id was never started there.
/// A known id on start means that the worker accepted an earlier start of the fragment.
fn classify(error: QueryFragmentError) -> CallError {
    match &error {
        QueryFragmentError::Internal(coded) if coded.code == ErrorCode::QueryNotFound => {
            CallError::NotFound
        }
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

/// The transport to a worker in this process.
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
    fn poll_interval(&self) -> Duration {
        EMBEDDED_FRAGMENT_POLL_INTERVAL
    }

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

    async fn status(&self, id: QueryFragmentId) -> Result<RawStatus, CallError> {
        let worker = self.worker.clone();
        let id = *id;
        call(move || worker.get_query_fragment_status(id)).await
    }
}
