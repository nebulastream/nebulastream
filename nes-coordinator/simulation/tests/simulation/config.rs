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
//! A config file holds one or more `[[test]]` tables, each with its `[[test.workload]]` tables.
//! TODO #2286: generate trials instead of writing each one by hand.

use crate::workload::Workload;
use anyhow::{Result, ensure};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialFile {
    #[serde(rename = "test")]
    pub trials: Vec<TrialConfig>,
}

/// `timeout`, `buggify_until` and every `begin` and `end` are seconds of simulated time since the trial started.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrialConfig {
    pub title: String,
    /// When the checks run.
    /// The configs leave at least 45 s after the last fault workload ends,
    /// so the coordinator reaches every worker again and every fragment of an active query is Running before the checks.
    pub timeout: u64,
    /// Buggify is on from the start until this time.
    /// Without the key it stays off.
    pub buggify_until: Option<u64>,
    /// The trial fails if fewer queries than this end with a fragment on a removed host and without a drop by the client.
    /// With a value above zero, the trial cannot pass without having dropped a worker that ran a fragment.
    #[serde(default)]
    pub min_failed_queries: usize,
    #[serde(rename = "workload")]
    pub workloads: Vec<Workload>,
}

impl TrialConfig {
    /// A workload or buggify that is still active at the timeout leaves the system no time to recover.
    pub fn validate(&self) -> Result<()> {
        for workload in &self.workloads {
            workload.validate(&self.title, self.timeout)?;
        }
        if let Some(until) = self.buggify_until {
            ensure!(
                until < self.timeout,
                "trial '{}': buggify is still on at the timeout of {}s",
                self.title,
                self.timeout,
            );
        }
        Ok(())
    }
}
