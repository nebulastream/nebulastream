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

//! Failure-injection workload that kills nodes and later restarts them.
//!
//! The share `kill_rate` of the workers is picked at random (at least one), and all victims are killed at the same instant.
//! Restarts happen after random delays inside the configured restart window,
//! so the system sees a correlated outage followed by a staggered recovery, which is harder than losing one node at a time.

use crate::harness::TestHarness;
use crate::workload::pick_victims;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttritionWorkload {
    pub begin: u64,
    pub kill_rate: f64,
    pub restart_delay_lo_secs: u64,
    pub restart_delay_hi_secs: u64,
}

impl AttritionWorkload {
    const NAME: &str = "Attrition";

    pub async fn run(&self, harness: &TestHarness) {
        info!(
            "{}: kill_at={}s kill_rate={:.0}% restart_delay={}s..{}s",
            Self::NAME,
            self.begin,
            self.kill_rate * 100.0,
            self.restart_delay_lo_secs,
            self.restart_delay_hi_secs,
        );

        tokio::time::sleep(Duration::from_secs(self.begin)).await;

        let restart_delay = Duration::from_secs(self.restart_delay_lo_secs)
            ..Duration::from_secs(self.restart_delay_hi_secs);
        let mut rng = thread_rng();
        let victims = pick_victims(&harness.get_worker_ids(), self.kill_rate);

        // Sorted by delay so the restart loop below can sleep the difference between
        // consecutive delays instead of tracking each node's deadline separately.
        let mut restarts: Vec<(NodeId, Duration)> = victims
            .iter()
            .map(|&id| (id, rng.gen_range(restart_delay.clone())))
            .collect();
        restarts.sort_by_key(|&(_, delay)| delay);

        // Kill every victim before the first restart sleep, so the outage is correlated.
        for &(id, _) in &restarts {
            info!("{}: kill {id}", Self::NAME);
            harness.kill(id);
        }

        let mut elapsed = Duration::ZERO;
        for &(id, delay) in &restarts {
            if delay > elapsed {
                tokio::time::sleep(delay - elapsed).await;
                elapsed = delay;
            }
            info!("{}: restart {id} (after {delay:?})", Self::NAME);
            harness.restart(id);
        }
    }
}
