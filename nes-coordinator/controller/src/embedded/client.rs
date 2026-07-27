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

use crate::fragment::{Client, Outcome};
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentTransition, StopMode,
};
use std::sync::Arc;

use super::Worker;

/// Turns a panic from a blocking worker call into an error outcome. The
/// panic payload holds the message; there is no backtrace to recover.
#[cfg(not(madsim))]
fn panic_to_error(join_err: tokio::task::JoinError) -> QueryFragmentError {
    let msg = match join_err.try_into_panic() {
        Ok(panic) => panic
            .downcast_ref::<&str>()
            .map(|s| (*s).to_string())
            .or_else(|| panic.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "worker call panicked".to_string()),
        Err(join_err) => join_err.to_string(),
    };
    QueryFragmentError::Internal {
        code: 0,
        msg,
        trace: String::new(),
    }
}

/// Runs a blocking worker call off the async runtime. Under madsim the embedded
/// client is unused and `spawn_blocking` is unavailable, so the call runs inline.
#[cfg(not(madsim))]
async fn run_blocking<F>(f: F) -> Result<(), QueryFragmentError>
where
    F: FnOnce() -> Result<(), QueryFragmentError> + Send + 'static,
{
    match tokio::task::spawn_blocking(f).await {
        Ok(result) => result,
        Err(join_err) => Err(panic_to_error(join_err)),
    }
}

#[cfg(madsim)]
async fn run_blocking<F>(f: F) -> Result<(), QueryFragmentError>
where
    F: FnOnce() -> Result<(), QueryFragmentError> + Send + 'static,
{
    f()
}

/// Adapts the synchronous in-process worker interface to the async client
/// interface used by the lifecycle driver. The mutating calls that reach the
/// engine (start, stop) can block, so they run on a blocking thread. Status
/// reads are cheap and stay inline.
pub(super) struct QueryFragmentClient {
    worker: Arc<dyn Worker>,
}

impl QueryFragmentClient {
    pub(super) fn new(worker: Arc<dyn Worker>) -> Self {
        Self { worker }
    }
}

impl Client for QueryFragmentClient {
    async fn register(&self, id: QueryFragmentId, plan: &[u8]) -> Outcome {
        let mut query_plan: SerializableQueryPlan = match prost::Message::decode(plan) {
            Ok(plan) => plan,
            Err(e) => {
                return Outcome::Failed(QueryFragmentError::Internal {
                    code: 0,
                    msg: format!("failed to decode stored query plan: {e}"),
                    trace: String::new(),
                });
            }
        };
        query_plan.query_id = Some(SerializableQueryId { id: *id });
        let prepared = prost::Message::encode_to_vec(&query_plan);
        match self.worker.register_fragment(prepared) {
            Ok(()) => Outcome::Transition(QueryFragmentTransition::Registered),
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn start(&self, id: QueryFragmentId) -> Outcome {
        let worker = self.worker.clone();
        let id = *id;
        match run_blocking(move || worker.start_fragment(id)).await {
            Ok(()) => Outcome::Transition(QueryFragmentTransition::running_now()),
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn stop(&self, id: QueryFragmentId, mode: StopMode) -> Outcome {
        let worker = self.worker.clone();
        let id = *id;
        match run_blocking(move || worker.stop_fragment(id, mode)).await {
            Ok(()) => Outcome::Accepted,
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn observe(
        &self,
        id: QueryFragmentId,
        _desired_state: DesiredQueryFragmentState,
    ) -> Outcome {
        match self.worker.get_fragment_status(*id) {
            Ok(status) => Outcome::Status(status),
            Err(err) => Outcome::Failed(err),
        }
    }
}
