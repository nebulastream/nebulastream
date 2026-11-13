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

//! Failure-injection workload that pauses random nodes for a window and resumes them.
//!
//! A paused node still exists from the network's point of view but makes no progress.
//! This tests whether the controller's RPC and probe timeouts fire and its reconciliation resumes once the node comes back,
//! rather than staying stuck in an intermediate state.

use crate::harness::TestHarness;
use crate::workload::pick_victims;
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StallWorkload {
    pub begin: u64,
    pub end: u64,
    pub pause_rate: f64,
}

impl StallWorkload {
    const NAME: &str = "Stall";

    pub async fn run(&self, harness: &TestHarness) {
        info!(
            "{}: ({}s..{}s) pause_rate={:.0}%",
            Self::NAME,
            self.begin,
            self.end,
            self.pause_rate * 100.0,
        );

        tokio::time::sleep(Duration::from_secs(self.begin)).await;

        let victims = pick_victims(&harness.get_worker_ids(), self.pause_rate);
        for &id in &victims {
            info!("{}: pause {id}", Self::NAME);
            harness.pause(id);
        }

        tokio::time::sleep(Duration::from_secs(self.end - self.begin)).await;
        for &id in &victims {
            harness.resume(id);
            info!("{}: resume {id}", Self::NAME);
        }
    }
}
