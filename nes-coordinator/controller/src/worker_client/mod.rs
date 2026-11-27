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

//! The clients through which a worker loop reaches its worker, in this process or over gRPC.

pub mod in_process;
pub mod remote;

use model::identifier::QueryFragmentId;
use model::query::query_fragment::QueryFragmentError;
use remote::worker_rpc_service::WorkerStatusResponse;
use remote::worker_rpc_service::nes::SerializableQueryPlan;

/// A failed worker call, classified by what the loop does next.
pub(crate) enum CallError {
    /// The worker already accepted a start for the fragment's id.
    AlreadyExists,
    /// Repeating the call may succeed.
    Transient(QueryFragmentError),
    /// Repeating the call cannot succeed, so a failed start fails the fragment.
    Permanent(QueryFragmentError),
}

/// The calls to one worker, over gRPC or into this process.
/// Every call task gets a clone, so all clones must reach the same worker.
pub(crate) trait WorkerClient: Clone + Send + Sync + 'static {
    fn start(
        &self,
        plan: SerializableQueryPlan,
    ) -> impl Future<Output = Result<(), CallError>> + Send;

    fn stop(&self, id: QueryFragmentId) -> impl Future<Output = Result<(), CallError>> + Send;

    /// Lists every active fragment, and the fragments that terminated at or after `after`, in the worker's milliseconds.
    fn get_status(
        &self,
        after: u64,
    ) -> impl Future<Output = anyhow::Result<WorkerStatusResponse>> + Send;
}
