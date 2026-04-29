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
use model::identifier::FragmentId;
use model::query::fragment::{DesiredFragmentState, FragmentError, FragmentTransition, StopMode};
use std::sync::Arc;

use super::Worker;

/// Adapts the synchronous in-process worker interface to the async client
/// interface used by the lifecycle driver. Worker calls are made inline;
/// the adapter only repackages success and error into result variants.
pub(super) struct FragmentClient {
    worker: Arc<dyn Worker>,
}

impl FragmentClient {
    pub(super) fn new(worker: Arc<dyn Worker>) -> Self {
        Self { worker }
    }
}

impl Client for FragmentClient {
    async fn register(&self, id: FragmentId, plan: &[u8]) -> Outcome {
        let mut query_plan: SerializableQueryPlan = match prost::Message::decode(plan) {
            Ok(plan) => plan,
            Err(e) => {
                return Outcome::Failed(FragmentError::Internal {
                    code: 0,
                    msg: format!("failed to decode stored query plan: {e}"),
                    trace: String::new(),
                });
            }
        };
        query_plan.query_id = Some(SerializableQueryId { id: *id });
        let prepared = prost::Message::encode_to_vec(&query_plan);
        match self.worker.register_fragment(prepared) {
            Ok(()) => Outcome::Transition(FragmentTransition::Registered),
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn start(&self, id: FragmentId) -> Outcome {
        match self.worker.start_fragment(*id) {
            Ok(()) => Outcome::Transition(FragmentTransition::running_now()),
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn stop(&self, id: FragmentId, mode: StopMode) -> Outcome {
        match self.worker.stop_fragment(*id, mode) {
            Ok(()) => Outcome::Accepted,
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn poll(&self, id: FragmentId, _desired_state: DesiredFragmentState) -> Outcome {
        match self.worker.get_fragment_status(*id) {
            Ok(status) => Outcome::Status(status),
            Err(err) => Outcome::Failed(err),
        }
    }
}
