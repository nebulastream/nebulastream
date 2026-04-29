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

use crate::util::task_map::TaskMap;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, warn};

/// Common pattern for reconciliation-driven control loops. An implementor
/// owns a keyed set of child tasks and provides a step that queries
/// authoritative state, then spawns or aborts children to drive observed
/// state toward desired state. Each level of the hierarchy uses this trait.
pub(crate) trait Reconciler {
    type Key: Eq + Hash + Clone + Send + Display + 'static;

    fn tasks(&mut self) -> &mut TaskMap<Self::Key>;
    fn reconcile(&mut self) -> impl Future<Output = ()> + Send;
}

/// Drive a reconciliation loop. Three wake-up sources: a watch-channel
/// intent signal (something in the DB likely changed), the completion of a
/// child task, and a periodic tick. The watch channel coalesces — a burst
/// of notifications still results in one reconcile, and a slow consumer
/// can never fall behind a fast producer. The periodic tick is the
/// level-triggered backstop: state lives in the DB, so even if every
/// notification were lost, the next poll would catch up. Returns when the
/// intent sender is dropped.
pub(crate) async fn reconcile_loop(
    reconciler: &mut impl Reconciler,
    intent_rx: &mut watch::Receiver<()>,
    poll_interval: Duration,
) {
    reconciler.reconcile().await;
    loop {
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
            () = tokio::time::sleep(poll_interval) => {}
        }
        reconciler.reconcile().await;
    }
}
