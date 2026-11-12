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

//! The transport to a worker in this process.
//! Each call is a function call, run off the async runtime.

use super::InProcessWorker;
use crate::config::EMBEDDED_FRAGMENT_POLL_INTERVAL;
use crate::query_fragment::{CallError, RawStatus, Transport};
use crate::remote::worker_rpc_service::nes::SerializableQueryPlan;
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use std::sync::Arc;
use std::time::Duration;

/// Runs a blocking worker call off the async runtime.
/// Under madsim this transport is unused and blocking tasks are unavailable,
/// so the call runs inline.
#[cfg(not(madsim))]
async fn run_blocking<T, F>(f: F) -> Result<T, QueryFragmentError>
where
    F: FnOnce() -> Result<T, QueryFragmentError> + Send + 'static,
    T: Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(join_err) => match join_err.try_into_panic() {
            Ok(panic) => std::panic::resume_unwind(panic),
            Err(join_err) => Err(QueryFragmentError::Internal(CodedError::new(
                ErrorCode::UnknownException,
                join_err,
            ))),
        },
    }
}

#[cfg(madsim)]
async fn run_blocking<T, F>(f: F) -> Result<T, QueryFragmentError>
where
    F: FnOnce() -> Result<T, QueryFragmentError> + Send + 'static,
    T: Send + 'static,
{
    f()
}

/// A worker in this process either answers or fails for good.
/// Two codes mean state, not failure.
/// An unknown id: the status log is append-only and gets its first entry before any work is queued,
/// so an unknown id was never started there.
/// A known id on start: the worker accepted an earlier start of the same fragment.
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
        run_blocking(move || worker.start_query_fragment(prepared))
            .await
            .map_err(classify)
    }

    async fn stop(&self, id: QueryFragmentId) -> Result<(), CallError> {
        let worker = self.worker.clone();
        let id = *id;
        run_blocking(move || worker.stop_query_fragment(id))
            .await
            .map_err(classify)
    }

    async fn status(&self, id: QueryFragmentId) -> Result<RawStatus, CallError> {
        let worker = self.worker.clone();
        let id = *id;
        run_blocking(move || worker.get_query_fragment_status(id))
            .await
            .map_err(classify)
    }
}
