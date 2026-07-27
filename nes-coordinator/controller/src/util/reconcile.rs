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

//! Shared control loop of every level of the reconciliation hierarchy.

use crate::util::task_map::TaskMap;
use std::fmt::Display;
use std::hash::Hash;
use std::time::Duration;
use tokio::select;
use tokio::sync::watch;
use tracing::{debug, warn};

/// One level of the reconciliation hierarchy.
/// An implementor owns a keyed set of child tasks and provides a step that queries authoritative state,
/// then spawns or aborts children to move observed state toward desired state.
pub(crate) trait Reconciler {
    type Key: Eq + Hash + Clone + Send + Display + 'static;

    fn tasks(&mut self) -> &mut TaskMap<Self::Key>;
    fn reconcile(&mut self) -> impl Future<Output = ()> + Send;
    fn alive(&mut self) -> impl Future<Output = bool> + Send {
        async { true }
    }
}

pub(crate) enum Exit {
    Shutdown,
    LinkLost,
}

pub(crate) async fn reconcile_loop(
    reconciler: &mut impl Reconciler,
    intent_rx: &mut watch::Receiver<()>,
    poll_interval: Duration,
    heartbeat: Option<Duration>,
) -> Exit {
    let mut poll = tokio::time::interval(poll_interval);
    poll.tick().await;
    let mut heartbeat = heartbeat.map(tokio::time::interval);
    reconciler.reconcile().await;
    loop {
        let mut probe = false;
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
                    if change.is_err() { return Exit::Shutdown; }
                }
                _ = poll.tick() => {}
                () = tick(&mut heartbeat) => { probe = true; }
            }
        }
        if probe {
            if !reconciler.alive().await {
                return Exit::LinkLost;
            }
            continue;
        }
        reconciler.reconcile().await;
    }
}

async fn tick(heartbeat: &mut Option<tokio::time::Interval>) {
    match heartbeat {
        Some(interval) => {
            interval.tick().await;
        }
        None => std::future::pending().await,
    }
}
