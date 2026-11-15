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
//! A workload is the unit of behavior that composes a trial.
//! There are two kinds:
//!
//!  - Driver workloads (`ops`) submit catalog operations for queries or workers.
//!  - Failure-injection workloads (`attrition`, `clog`, `stall`, `degradation`)
//!    degrade or stop the network or nodes for a defined duration.
//!
//! A trial selects each workload by its `name` key in TOML.

pub mod attrition;
pub mod clog;
pub mod degradation;
pub mod ops;
pub mod stall;

use crate::harness::Harness;
use crate::model_state::{ModelState, Operation};
use anyhow::{Result, ensure};
use attrition::AttritionWorkload;
use clog::ClogWorkload;
use degradation::DegradationWorkload;
use madsim::rand::seq::SliceRandom;
use madsim::rand::thread_rng;
use madsim::task::NodeId;
use ops::OpsWorkload;
use serde::Deserialize;
use stall::StallWorkload;
use std::cell::RefCell;

#[derive(Debug, Deserialize)]
#[serde(tag = "name")]
pub enum Workload {
    Worker(OpsWorkload),
    Query(OpsWorkload),
    Attrition(AttritionWorkload),
    Clog(ClogWorkload),
    Stall(StallWorkload),
    Degradation(DegradationWorkload),
}

impl Workload {
    /// The time after which the workload does nothing more.
    const fn end(&self) -> u64 {
        match self {
            Self::Worker(ops) | Self::Query(ops) => ops.end,
            Self::Attrition(attrition) => {
                attrition.begin.saturating_add(attrition.restart_delay_hi)
            }
            Self::Clog(clog) => clog.end,
            Self::Stall(stall) => stall.end,
            Self::Degradation(degradation) => degradation.end,
        }
    }

    const fn rate(&self) -> Option<f64> {
        match self {
            Self::Worker(_) | Self::Query(_) => None,
            Self::Attrition(attrition) => Some(attrition.kill_rate),
            Self::Clog(clog) => Some(clog.clog_rate),
            Self::Stall(stall) => Some(stall.pause_rate),
            Self::Degradation(degradation) => Some(degradation.loss_rate),
        }
    }

    pub fn validate(&self, title: &str, timeout: u64) -> Result<()> {
        let well_formed = match self {
            Self::Worker(ops) | Self::Query(ops) => ops.begin <= ops.end,
            Self::Attrition(attrition) => attrition.restart_delay_lo < attrition.restart_delay_hi,
            Self::Clog(clog) => clog.begin < clog.end,
            Self::Stall(stall) => stall.begin < stall.end,
            Self::Degradation(degradation) => degradation.begin < degradation.end,
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
        if let Some(rate) = self.rate() {
            ensure!(
                (0.0..=1.0).contains(&rate),
                "trial '{title}': {self:?} has a rate outside [0, 1]"
            );
        }
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
    pub async fn run(&self, model: &RefCell<ModelState>, harness: &Harness) {
        use Operation::{CreateQuery, CreateWorker, DropQuery, DropWorker};
        match self {
            Self::Worker(ops) => {
                ops.run("Worker", CreateWorker, DropWorker, model, harness)
                    .await;
            }
            Self::Query(ops) => {
                ops.run("Query", CreateQuery, DropQuery, model, harness)
                    .await;
            }
            Self::Attrition(attrition) => attrition.run(harness).await,
            Self::Clog(clog) => clog.run(harness).await,
            Self::Stall(stall) => stall.run(harness).await,
            Self::Degradation(degradation) => degradation.run(harness).await,
        }
    }
}

/// Picks the nodes that a fault hits: the share `rate` of the candidates, and at least one.
/// With no victim the trial would pass without having injected the fault.
pub fn pick_victims(candidates: &[NodeId], rate: f64) -> Vec<NodeId> {
    assert!(
        !candidates.is_empty(),
        "a fault workload started before any node it could hit existed"
    );
    let count = ((candidates.len() as f64 * rate).round() as usize).clamp(1, candidates.len());
    candidates
        .choose_multiple(&mut thread_rng(), count)
        .copied()
        .collect()
}
