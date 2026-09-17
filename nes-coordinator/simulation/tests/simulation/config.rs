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

//! TOML schema for simulation trial configs.
//!
//! A test file in `tests/simulation/configs` holds one or more trial entries.
//! Each entry pins simulator behavior (timeout, buggify)
//! and lists the workloads to run, each selected by its `name` key.

use crate::workload::attrition::AttritionWorkload;
use crate::workload::clog::ClogWorkload;
use crate::workload::degradation::DegradationWorkload;
use crate::workload::ops::OpsConfig;
use crate::workload::stall::StallWorkload;
use serde::Deserialize;
use std::time::Duration;

/// The latency range of the simulated network outside of a degradation window.
pub const SEND_LATENCY_LO: Duration = Duration::from_millis(1);
pub const SEND_LATENCY_HI: Duration = Duration::from_millis(100);

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub test: Vec<TestConfig>,
}

/// One trial. `timeout`, `buggify_until` and every `begin` and `end` are seconds of simulated time since the trial started.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestConfig {
    pub title: String,
    /// When the checks run.
    pub timeout: u64,
    /// Buggify is on from the start until this time. Without the key it stays off.
    pub buggify_until: Option<u64>,
    /// The trial fails if worker drops failed fewer queries than this,
    /// so it cannot pass without having dropped a worker that ran a fragment.
    #[serde(default)]
    pub min_queries_failed_by_worker_drop: usize,
    pub workload: Vec<WorkloadSpec>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "name")]
pub enum WorkloadSpec {
    Worker(OpsConfig),
    Query(OpsConfig),
    Attrition(AttritionWorkload),
    Clog(ClogWorkload),
    Stall(StallWorkload),
    Degradation(DegradationWorkload),
}

impl WorkloadSpec {
    /// The time after which the workload does nothing more.
    fn end(&self) -> u64 {
        match self {
            Self::Worker(ops) | Self::Query(ops) => ops.end,
            Self::Attrition(attrition) => attrition.begin + attrition.restart_delay_hi_secs,
            Self::Clog(clog) => clog.end,
            Self::Stall(stall) => stall.end,
            Self::Degradation(degradation) => degradation.end,
        }
    }
}

impl TestConfig {
    /// The checks run at the timeout, so a workload that is still active then leaves the system no time to recover.
    pub fn validate(&self) {
        for workload in &self.workload {
            let well_formed = match workload {
                WorkloadSpec::Worker(ops) | WorkloadSpec::Query(ops) => ops.begin <= ops.end,
                WorkloadSpec::Attrition(attrition) => {
                    attrition.restart_delay_lo_secs < attrition.restart_delay_hi_secs
                }
                WorkloadSpec::Clog(clog) => clog.begin < clog.end,
                WorkloadSpec::Stall(stall) => stall.begin < stall.end,
                WorkloadSpec::Degradation(degradation) => degradation.begin < degradation.end,
            };
            assert!(
                well_formed,
                "trial '{}': {workload:?} has an empty or inverted time range",
                self.title,
            );
            if let WorkloadSpec::Worker(ops) | WorkloadSpec::Query(ops) = workload {
                assert!(
                    ops.create_weight > 0 || ops.drop_weight > 0,
                    "trial '{}': {workload:?} has no operation with a non-zero weight",
                    self.title,
                );
            }
            assert!(
                workload.end() < self.timeout,
                "trial '{}': {workload:?} is still active at the timeout of {}s",
                self.title,
                self.timeout,
            );
        }
        if let Some(until) = self.buggify_until {
            assert!(
                until < self.timeout,
                "trial '{}': buggify is still on at the timeout of {}s",
                self.title,
                self.timeout,
            );
        }
    }
}
