use crate::util::task_map::TaskMap;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, warn};

/// Common pattern for reconciliation-driven control loops.
///
/// Each level of the coordinator hierarchy (Controller → WorkerTask → FragmentTask)
/// follows the same pattern: maintain a TaskMap of child tasks, periodically query
/// the DB for state mismatches, and spawn/abort tasks to drive toward the desired state.
pub(crate) trait Reconciler {
    type Key: Eq + Hash + Clone + Send + Display + 'static;

    fn tasks(&mut self) -> &mut TaskMap<Self::Key>;
    fn reconcile(&mut self) -> impl Future<Output = ()> + Send;
}

/// Drive a reconciliation loop: wait for intents, task completions, or a periodic
/// timer, then reconcile. Returns when the intent sender is dropped.
pub(crate) async fn reconcile_loop(
    reconciler: &mut impl Reconciler,
    intent_rx: &mut watch::Receiver<()>,
    poll_interval: Duration,
) {
    reconciler.reconcile().await;
    loop {
        {
            let tasks = reconciler.tasks();
            select! {
                Some(result) = tasks.join_next() => {
                    match result {
                        Ok(key) => debug!(%key, "task completed"),
                        Err(e) if e.is_cancelled() => {}
                        Err(e) => warn!("task failed: {e:?}"),
                    }
                }
                change = intent_rx.changed() => {
                    if change.is_err() { return; }
                }
                _ = tokio::time::sleep(poll_interval) => {}
            }
        }
        reconciler.reconcile().await;
    }
}
