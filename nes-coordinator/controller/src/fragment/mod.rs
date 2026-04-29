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

mod task;

pub(super) use task::Task;

use model::identifier::FragmentId;
use model::query::fragment::{
    DesiredFragmentState, FragmentError, FragmentState, FragmentTransition, StopMode,
};
use std::future::Future;
use std::time::Duration;

pub(super) const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Result of a single worker-facing call. The lifecycle driver inspects it
/// to decide the next state transition, whether to retry, and whether to
/// stop polling.
pub(super) enum Outcome {
    Transition(FragmentTransition),
    Status(Status),
    Accepted,
    Failed(FragmentError),
    Retry(FragmentError),
}

/// A fragment's state as the worker reports it, normalized into a shape the
/// lifecycle driver can map onto a state transition.
#[derive(Debug)]
pub struct Status {
    pub state: FragmentState,
    pub start_timestamp: Option<u64>,
    pub stop_timestamp: Option<u64>,
    pub error: Option<FragmentError>,
}

/// Worker-facing operations for one fragment. In-process and out-of-process
/// backends each provide their own implementation; the lifecycle driver is
/// generic over this trait.
pub(super) trait Client: Send + Sync {
    fn register(&self, id: FragmentId, plan: &[u8]) -> impl Future<Output = Outcome> + Send;
    fn start(&self, id: FragmentId) -> impl Future<Output = Outcome> + Send;
    fn stop(&self, id: FragmentId, mode: StopMode) -> impl Future<Output = Outcome> + Send;
    fn poll(
        &self,
        id: FragmentId,
        desired_state: DesiredFragmentState,
    ) -> impl Future<Output = Outcome> + Send;
}
