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

use crate::config::EMBEDDED_FRAGMENT_POLL_INTERVAL;
use crate::fragment::{Client, Outcome};
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentTransition,
};
use std::sync::Arc;
use std::time::Duration;

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
async fn run_blocking<T, F>(f: F) -> Result<T, QueryFragmentError>
where
    F: FnOnce() -> Result<T, QueryFragmentError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .unwrap_or_else(|join_err| Err(panic_to_error(join_err)))
}

#[cfg(madsim)]
async fn run_blocking<T, F>(f: F) -> Result<T, QueryFragmentError>
where
    F: FnOnce() -> Result<T, QueryFragmentError> + Send + 'static,
    T: Send + 'static,
{
    f()
}

/// Exception code the worker reports for a query id it has no record of.
const QUERY_NOT_FOUND: u16 = 5000;

/// The worker's status log is append-only and gets its first entry before any work is queued, so an
/// unknown id was never started there. Nothing runs for it, so callers treat this as state, not failure.
fn is_not_found(error: &QueryFragmentError) -> bool {
    matches!(error, QueryFragmentError::Internal { code, .. } if *code == QUERY_NOT_FOUND)
}

/// Adapts the synchronous in-process worker interface to the async client
/// interface used by the lifecycle driver. Every call reaches the engine
/// through a lock that a query compilation can hold for a long time, so all of
/// them run on a blocking thread rather than on the runtime.
pub(super) struct QueryFragmentClient {
    worker: Arc<dyn Worker>,
}

impl QueryFragmentClient {
    pub(super) fn new(worker: Arc<dyn Worker>) -> Self {
        Self { worker }
    }
}

impl Client for QueryFragmentClient {
    fn poll_interval(&self) -> Duration {
        EMBEDDED_FRAGMENT_POLL_INTERVAL
    }

    async fn start(&self, id: QueryFragmentId, plan: &[u8]) -> Outcome {
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
        query_plan.query_id = Some(SerializableQueryId::from_fragment_id(*id));
        let prepared = prost::Message::encode_to_vec(&query_plan);

        let worker = self.worker.clone();
        match run_blocking(move || worker.start_query_fragment(prepared)).await {
            // Not `Running`: the fragment has only been accepted at this point, and the state
            // machine requires Pending to advance to Started. `observe` promotes it once the
            // worker reports it producing.
            Ok(()) => Outcome::Transition(QueryFragmentTransition::Started),
            Err(err) if is_not_found(&err) => Outcome::Transition(QueryFragmentTransition::Pending),
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn stop(&self, id: QueryFragmentId) -> Outcome {
        let worker = self.worker.clone();
        let id = *id;
        match run_blocking(move || worker.stop_query_fragment(id)).await {
            Ok(()) => Outcome::Accepted,
            Err(err) if is_not_found(&err) => {
                Outcome::Transition(QueryFragmentTransition::stopped_now())
            }
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn observe(
        &self,
        id: QueryFragmentId,
        desired_state: DesiredQueryFragmentState,
    ) -> Outcome {
        let worker = self.worker.clone();
        let id = *id;
        match run_blocking(move || worker.get_query_fragment_status(id)).await {
            Ok(status) => Outcome::Status(status),
            Err(err) if is_not_found(&err) => match desired_state {
                DesiredQueryFragmentState::Completed => {
                    Outcome::Transition(QueryFragmentTransition::Pending)
                }
                DesiredQueryFragmentState::Stopped => {
                    Outcome::Transition(QueryFragmentTransition::stopped_now())
                }
            },
            Err(err) => Outcome::Failed(err),
        }
    }
}
