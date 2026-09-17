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

//! Workloads that make up a trial.
//!
//! A driver workload (`Worker`, `Query`) sends catalog operations,
//! a failure-injection workload (`NodeFault`, `Degradation`) disrupts the network or nodes for a window,
//! and a `Watch` workload waits on queries and checks each reply.
//! A trial selects each workload by its `name` key in TOML.

pub mod degradation;
pub mod node_fault;
pub mod ops;
pub mod watch;

use crate::harness::Harness;
use crate::ledger::{Ledger, Operation};
use anyhow::{Result, ensure};
use degradation::DegradationWorkload;
use madsim::rand::{Rng, thread_rng};
use node_fault::NodeFaultWorkload;
use ops::OpsWorkload;
use serde::Deserialize;
use std::cell::RefCell;
use std::time::Duration;
use tracing::debug;
use watch::WatchWorkload;

/// `Worker` and `Query` share one schema and differ in the operations that they send.
#[derive(Debug, Deserialize)]
#[serde(tag = "name")]
pub enum Workload {
    Worker(OpsWorkload),
    Query(OpsWorkload),
    NodeFault(NodeFaultWorkload),
    Degradation(DegradationWorkload),
    Watch(WatchWorkload),
}

impl Workload {
    /// The time after which the workload does nothing more.
    const fn end(&self) -> u64 {
        match self {
            Self::Worker(ops) | Self::Query(ops) => ops.end,
            Self::NodeFault(fault) => fault.end.hi(),
            Self::Degradation(degradation) => degradation.end,
            // A watch sent at the end of the window replies by the end of its timeout.
            Self::Watch(watch) => watch.end + watch.timeout.hi(),
        }
    }

    const fn rate(&self) -> Option<f64> {
        match self {
            Self::Worker(_) | Self::Query(_) | Self::Watch(_) => None,
            Self::NodeFault(fault) => Some(fault.rate),
            Self::Degradation(degradation) => Some(degradation.loss_rate),
        }
    }

    pub fn validate(&self, title: &str, timeout: u64) -> Result<()> {
        let well_formed = match self {
            Self::Worker(ops) | Self::Query(ops) => ops.begin <= ops.end,
            // Every victim's fault ends after it starts.
            Self::NodeFault(NodeFaultWorkload { start, end, .. }) => {
                [start.lo(), start.hi(), end.lo(), end.hi()].is_sorted() && start.lo() < end.hi()
            }
            Self::Degradation(degradation) => degradation.begin < degradation.end,
            Self::Watch(watch) => {
                watch.begin <= watch.end && watch.timeout.lo() <= watch.timeout.hi()
            }
        };
        ensure!(
            well_formed,
            "trial '{title}': {self:?} has an empty or inverted time range"
        );
        if let Self::Worker(ops) | Self::Query(ops) = self {
            ensure!(
                ops.create_weight
                    .checked_add(ops.drop_weight)
                    .is_some_and(|total| total > 0),
                "trial '{title}': {self:?} has weights that are all zero or sum past u32::MAX"
            );
        }
        ensure!(
            self.rate().is_none_or(|rate| (0.0..=1.0).contains(&rate)),
            "trial '{title}': {self:?} has a rate outside [0, 1]"
        );
        if let Self::Degradation(degradation) = self {
            ensure!(
                degradation.latency_lo_ms < degradation.latency_hi_ms,
                "trial '{title}': {self:?} has an empty latency range"
            );
        }
        ensure!(
            self.end() < timeout,
            "trial '{title}': {self:?} is still active at the timeout of {timeout}s"
        );
        Ok(())
    }

    /// Runs concurrently with the other workloads of the trial.
    pub async fn run(&self, ledger: &RefCell<Ledger>, harness: &Harness) {
        use Operation::{CreateQuery, CreateWorker, DropQuery, DropWorker};
        match self {
            Self::Worker(ops) => {
                ops.run("Worker", CreateWorker, DropWorker, ledger, harness)
                    .await;
            }
            Self::Query(ops) => {
                ops.run("Query", CreateQuery, DropQuery, ledger, harness)
                    .await;
            }
            Self::NodeFault(fault) => fault.run(harness).await,
            Self::Degradation(degradation) => degradation.run(harness).await,
            Self::Watch(watch) => watch.run(ledger, harness).await,
        }
    }
}

/// Sorted, because the driver waits for each offset in turn.
pub fn pick_offsets(num_ops: usize, window_secs: u64) -> Vec<Duration> {
    let window_ms = window_secs
        .checked_mul(1000)
        .expect("the operation window does not fit in milliseconds");
    let mut rng = thread_rng();
    let mut offsets: Vec<Duration> = (0..num_ops)
        .map(|_| Duration::from_millis(rng.gen_range(0..=window_ms)))
        .collect();
    offsets.sort_unstable();

    debug!(offsets = %offsets.iter().map(|d| format!("{}ms", d.as_millis())).collect::<Vec<_>>().join(", "));
    offsets
}

/// A number of seconds: one value, or a range `[lo, hi)` that each draw picks its own value from.
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

    pub fn draw(self, rng: &mut impl Rng) -> Duration {
        let lo = Duration::from_secs(self.lo());
        let hi = Duration::from_secs(self.hi());
        if lo == hi { lo } else { rng.gen_range(lo..hi) }
    }
}
