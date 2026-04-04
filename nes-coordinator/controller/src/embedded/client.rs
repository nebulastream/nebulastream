use crate::fragment::{FragmentClient, FragmentStatus, Outcome};
use crate::remote::worker_rpc_service::nes::{SerializableQueryId, SerializableQueryPlan};
use model::identifier::FragmentId;
use model::query::fragment::{DesiredFragmentState, FragmentError, FragmentState, StopMode};
use std::sync::Arc;

use super::EmbeddedWorker;

pub(super) struct EmbeddedFragmentClient {
    worker: Arc<dyn EmbeddedWorker>,
}

impl EmbeddedFragmentClient {
    pub(super) fn new(worker: Arc<dyn EmbeddedWorker>) -> Self {
        Self { worker }
    }
}

impl FragmentClient for EmbeddedFragmentClient {
    async fn register(&self, id: FragmentId, plan: &[u8]) -> Outcome {
        let mut query_plan: SerializableQueryPlan =
            prost::Message::decode(plan).unwrap_or_default();
        query_plan.query_id = Some(SerializableQueryId { id: *id });
        let prepared = prost::Message::encode_to_vec(&query_plan);
        match self.worker.register_fragment(prepared) {
            Ok(()) => Outcome::Transition(FragmentState::Registered),
            Err(err) => Outcome::Failed(err),
        }
    }

    async fn start(&self, id: FragmentId) -> Outcome {
        match self.worker.start_fragment(*id) {
            Ok(()) => Outcome::Transition(FragmentState::Running),
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
