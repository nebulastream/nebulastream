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

//! The one fragment client, over whichever transport reaches the worker.
//! A transport answers the three calls that a worker takes for one fragment.
//! The client maps the answers onto the driver's outcomes,
//! so an unknown id, an unknown state and a failed call mean the same thing
//! for a worker in this process and for one behind a socket.

use super::{Client, Outcome, QueryFragmentStatus};
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::error::{CodedError, ErrorCode};
use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState, QueryFragmentTransition,
};
use std::future::Future;
use std::time::Duration;

/// Why a call did not answer.
pub(crate) enum CallError {
    /// The worker has no record of the id, which is state rather than failure.
    NotFound,
    /// The call may succeed when repeated; counted against the retry budget.
    Transient(QueryFragmentError),
    /// The call failed for good.
    Failed(QueryFragmentError),
}

/// A fragment's status as the worker reports it, before the state is decoded.
pub struct RawStatus {
    pub state: i32,
    pub start_ms: Option<u64>,
    pub stop_ms: Option<u64>,
    pub error: Option<QueryFragmentError>,
}

/// The three calls that a worker takes for one fragment, however it is reached.
pub(crate) trait Transport: Send + Sync {
    /// How long to wait between status reads of one fragment.
    /// A transport that reaches its worker in process can afford a short interval;
    /// one that pays a round trip per read cannot.
    fn poll_interval(&self) -> Duration;

    fn start(
        &self,
        plan: SerializableQueryPlan,
    ) -> impl Future<Output = Result<(), CallError>> + Send;
    fn stop(&self, id: QueryFragmentId) -> impl Future<Output = Result<(), CallError>> + Send;
    fn status(
        &self,
        id: QueryFragmentId,
    ) -> impl Future<Output = Result<RawStatus, CallError>> + Send;
}

pub(crate) struct FragmentClient<T> {
    transport: T,
}

impl<T> FragmentClient<T> {
    pub(crate) const fn new(transport: T) -> Self {
        Self { transport }
    }
}

impl<T: Transport> Client for FragmentClient<T> {
    fn poll_interval(&self) -> Duration {
        self.transport.poll_interval()
    }

    async fn start(&self, id: QueryFragmentId, plan: &[u8]) -> Outcome {
        // The stored plan has no id; the worker knows the plan by the fragment's id.
        let mut query_plan: SerializableQueryPlan = match prost::Message::decode(plan) {
            Ok(plan) => plan,
            Err(e) => {
                return Outcome::Failed(QueryFragmentError::Internal(CodedError::new(
                    ErrorCode::UnknownException,
                    format!("failed to decode stored query plan: {e}"),
                )));
            }
        };
        query_plan.query_id = Some(SerializableQueryId::from_fragment_id(*id));
        match self.transport.start(query_plan).await {
            // Started rather than Running: the worker has only accepted the fragment at this point,
            // and the state machine advances a pending fragment to started before anything else.
            // The status read promotes it once the worker reports that it is producing.
            Ok(()) => Outcome::Transition(QueryFragmentTransition::Started),
            Err(CallError::NotFound) => Outcome::Transition(QueryFragmentTransition::Pending),
            Err(CallError::Transient(err)) => Outcome::Retry(err),
            Err(CallError::Failed(err)) => Outcome::Failed(err),
        }
    }

    async fn stop(&self, id: QueryFragmentId) -> Outcome {
        match self.transport.stop(id).await {
            Ok(()) => Outcome::Accepted,
            Err(CallError::NotFound) => Outcome::Transition(QueryFragmentTransition::stopped_now()),
            Err(CallError::Transient(err)) => Outcome::Retry(err),
            Err(CallError::Failed(err)) => Outcome::Failed(err),
        }
    }

    async fn observe(
        &self,
        id: QueryFragmentId,
        desired_state: DesiredQueryFragmentState,
    ) -> Outcome {
        match self.transport.status(id).await {
            Ok(raw) => {
                // A state value that this build does not know is read again rather than acted on,
                // since acting on it would change the fragment's state on a guess.
                let state = match QueryFragmentState::try_from(raw.state) {
                    Ok(state) => state,
                    Err(unknown) => {
                        return Outcome::Retry(QueryFragmentError::Transport {
                            msg: format!("worker reported unknown fragment state: {unknown}"),
                        });
                    }
                };
                Outcome::Status(QueryFragmentStatus {
                    state,
                    start_timestamp: raw.start_ms,
                    stop_timestamp: raw.stop_ms,
                    error: raw.error,
                })
            }
            Err(CallError::NotFound) => match desired_state {
                DesiredQueryFragmentState::Completed => {
                    Outcome::Transition(QueryFragmentTransition::Pending)
                }
                DesiredQueryFragmentState::Stopped => {
                    Outcome::Transition(QueryFragmentTransition::stopped_now())
                }
            },
            Err(CallError::Transient(err)) => Outcome::Retry(err),
            Err(CallError::Failed(err)) => Outcome::Failed(err),
        }
    }
}
