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

//! Failure-injection workload that clogs whole nodes (not links) for overlapping windows.
//!
//! A clogged worker refuses every connection but keeps its fragments.
//! The share `clog_rate` of the workers is picked at random (at least one).
//! Each clog starts at a random point in the first half of the workload's window
//! and ends at a random point in the second half,
//! so the outages overlap in a rolling pattern that stresses concurrent reconnects.

use crate::harness::TestHarness;
use crate::workload::pick_victims;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClogWorkload {
    pub begin: u64,
    pub end: u64,
    pub clog_rate: f64,
}

#[derive(Clone, Copy)]
enum Event {
    Clog(NodeId),
    Unclog(NodeId),
}

impl ClogWorkload {
    const NAME: &str = "Clog";

    pub async fn run(&self, harness: &TestHarness) {
        info!(
            "{}: ({}s..{}s) clog_rate={:.0}%",
            Self::NAME,
            self.begin,
            self.end,
            self.clog_rate * 100.0,
        );

        tokio::time::sleep(Duration::from_secs(self.begin)).await;

        let candidates = harness.get_worker_ids();

        let t = Duration::from_secs(self.end - self.begin);
        let half = t / 2;

        let mut rng = thread_rng();
        let mut victims: Vec<(NodeId, Duration, Duration)> = Vec::new();
        for id in pick_victims(&candidates, self.clog_rate) {
            let start = half.mul_f64(rng.gen_range(0.0..1.0));
            let end = half + half.mul_f64(rng.gen_range(0.0..1.0));
            victims.push((id, start, end));
        }

        let mut events: Vec<(Duration, Event)> = Vec::with_capacity(victims.len() * 2);
        for &(id, start, end) in &victims {
            events.push((start, Event::Clog(id)));
            events.push((end, Event::Unclog(id)));
        }
        events.sort_by_key(|(at, _)| *at);

        let mut elapsed = Duration::ZERO;
        for (at, evt) in events {
            if at > elapsed {
                tokio::time::sleep(at - elapsed).await;
                elapsed = at;
            }
            match evt {
                Event::Clog(id) => {
                    info!("{}: clog {id}", Self::NAME);
                    harness.clog_node(id);
                }
                Event::Unclog(id) => {
                    harness.unclog_node(id);
                    info!("{}: unclog {id}", Self::NAME);
                }
            }
        }

        if elapsed < t {
            tokio::time::sleep(t - elapsed).await;
        }
    }
}
