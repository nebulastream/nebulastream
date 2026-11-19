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

//! Failure-injection workload that applies a fault to a share of the worker nodes and reverts it later.
//!
//! Each victim draws its own start time and end time, so one schedule covers a correlated outage with staggered recovery
//! (`start = 10`, `end = [11, 15]`), overlapping windows (`start = [10, 25]`, `end = [25, 40]`),
//! and one window for all victims (`start = 12`, `end = 38`).

use crate::harness::Harness;
use crate::workload::pick_victims;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::Instant;
use tracing::info;

#[derive(Clone, Copy, Debug, Deserialize)]
pub enum Action {
    /// Kills the node and restarts it without its fragments.
    Kill,
    /// Pauses the node.
    /// It stays reachable from the network's point of view but makes no progress,
    /// so the controller's RPC and probe timeouts have to fire.
    Pause,
    /// Drops every packet to and from the node, which keeps its fragments.
    Clog,
}

/// Seconds since the trial started: one time, or a range `[lo, hi]` that each victim draws its own time from.
#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(untagged)]
pub enum Seconds {
    At(u64),
    Between(u64, u64),
}

impl Seconds {
    pub const fn lo(self) -> u64 {
        match self {
            Self::At(at) | Self::Between(at, _) => at,
        }
    }

    pub const fn hi(self) -> u64 {
        match self {
            Self::At(at) | Self::Between(_, at) => at,
        }
    }

    fn draw(self, rng: &mut impl Rng) -> Duration {
        let (lo, hi) = (
            Duration::from_secs(self.lo()),
            Duration::from_secs(self.hi()),
        );
        if lo == hi { lo } else { rng.gen_range(lo..hi) }
    }
}

/// Only workers that exist at the earliest start can be victims.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeFaultWorkload {
    pub action: Action,
    pub rate: f64,
    pub start: Seconds,
    pub end: Seconds,
}

#[derive(Clone, Copy)]
enum Event {
    Apply(NodeId),
    Revert(NodeId),
}

impl NodeFaultWorkload {
    const NAME: &str = "NodeFault";

    pub async fn run(&self, harness: &Harness) {
        info!(
            "{} {:?}: start={:?} end={:?} rate={:.0}%",
            Self::NAME,
            self.action,
            self.start,
            self.end,
            self.rate * 100.0,
        );

        let origin = Instant::now();
        tokio::time::sleep_until(origin + Duration::from_secs(self.start.lo())).await;

        let mut rng = thread_rng();
        // Sorted by time, because the loop waits for each event in turn.
        // The sort is stable, so a victim's revert stays after its apply when both have the same time.
        let mut events: Vec<(Duration, Event)> =
            pick_victims(&harness.get_worker_nodes(), self.rate)
                .into_iter()
                .flat_map(|id| {
                    [
                        (self.start.draw(&mut rng), Event::Apply(id)),
                        (self.end.draw(&mut rng), Event::Revert(id)),
                    ]
                })
                .collect();
        events.sort_by_key(|&(at, _)| at);

        for (at, event) in events {
            tokio::time::sleep_until(origin + at).await;
            match event {
                Event::Apply(id) => {
                    info!("{} {:?}: apply to {id}", Self::NAME, self.action);
                    match self.action {
                        Action::Kill => harness.kill(id),
                        Action::Pause => harness.pause(id),
                        Action::Clog => harness.clog(id),
                    }
                }
                Event::Revert(id) => {
                    info!("{} {:?}: revert on {id}", Self::NAME, self.action);
                    match self.action {
                        Action::Kill => harness.restart(id),
                        Action::Pause => harness.resume(id),
                        Action::Clog => harness.unclog(id),
                    }
                }
            }
        }
    }
}
