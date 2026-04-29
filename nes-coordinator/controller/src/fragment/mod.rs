mod task;

pub(super) use task::FragmentTask;

use model::identifier::FragmentId;
use model::query::fragment::{
    DesiredFragmentState, FragmentError, FragmentState, FragmentTransition, StopMode,
};
use std::future::Future;
use std::time::Duration;

pub(super) const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub(super) enum Outcome {
    Transition(FragmentTransition),
    Status(FragmentStatus),
    Accepted,
    Failed(FragmentError),
    Retry(FragmentError),
}

#[derive(Debug)]
pub struct FragmentStatus {
    pub state: FragmentState,
    pub start_timestamp: Option<u64>,
    pub stop_timestamp: Option<u64>,
    pub error: Option<FragmentError>,
}

pub(super) trait FragmentClient: Send + Sync {
    fn register(&self, id: FragmentId, plan: &[u8]) -> impl Future<Output = Outcome> + Send;
    fn start(&self, id: FragmentId) -> impl Future<Output = Outcome> + Send;
    fn stop(&self, id: FragmentId, mode: StopMode) -> impl Future<Output = Outcome> + Send;
    fn poll(
        &self,
        id: FragmentId,
        desired_state: DesiredFragmentState,
    ) -> impl Future<Output = Outcome> + Send;
}
