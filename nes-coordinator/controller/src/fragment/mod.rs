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

//! Defines how a worker is asked about one fragment: a client trait that each
//! worker backend implements, and the result type a single call returns.

mod task;

pub(super) use task::FragmentTask;

use model::identifier::QueryFragmentId;
use model::query::query_fragment::{
    DesiredQueryFragmentState, QueryFragmentError, QueryFragmentState, QueryFragmentTransition,
};
use std::future::Future;
use std::time::Duration;

/// Result of a single worker-facing call. The lifecycle driver inspects it
/// to decide the next state transition, whether to retry, and whether to
/// stop polling.
pub(super) enum Outcome {
    Transition(QueryFragmentTransition),
    Status(QueryFragmentStatus),
    Accepted,
    Failed(QueryFragmentError),
    Retry(QueryFragmentError),
}

/// A fragment's state as the worker reports it, normalized into a shape the
/// lifecycle driver can map onto a state transition.
#[derive(Debug)]
pub struct QueryFragmentStatus {
    pub state: QueryFragmentState,
    pub start_timestamp: Option<u64>,
    pub stop_timestamp: Option<u64>,
    pub error: Option<QueryFragmentError>,
}

/// Worker-facing operations for one fragment. In-process and out-of-process
/// backends each provide their own implementation; the lifecycle driver is
/// generic over this trait.
///
/// These calls may be retried after a lost response, so the worker side has to
/// be idempotent: re-issuing a call that already took effect must succeed
/// rather than fail. The worker does not guarantee this yet; see issue #1817.
pub(super) trait Client: Send + Sync {
    /// How long to wait between status reads of one fragment.
    /// A backend that reaches its worker in process can afford a short interval, one that pays a
    /// round trip per read cannot.
    fn poll_interval(&self) -> Duration;

    fn start(&self, id: QueryFragmentId, plan: &[u8]) -> impl Future<Output = Outcome> + Send;
    fn stop(&self, id: QueryFragmentId) -> impl Future<Output = Outcome> + Send;
    fn observe(
        &self,
        id: QueryFragmentId,
        desired_state: DesiredQueryFragmentState,
    ) -> impl Future<Output = Outcome> + Send;
}
