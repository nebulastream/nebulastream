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

//! Failure-injection workload that pauses random nodes for a window
//! and resumes them.
//!
//! A paused node still exists from the network's point of view but
//! makes no progress, which lets us see whether the coordinator's
//! timeouts and the controller's reconciliation actually unblock when
//! the node comes back rather than wedging in some intermediate state.

#![cfg(madsim)]

use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

const DEFAULT_END_SECS: u64 = 30;
const DEFAULT_PAUSE_RATE: f64 = 0.20;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StallConfig {
    begin: u64,
    end: u64,
    pause_rate: f64,
    pause_coordinator: bool,
}

impl Default for StallConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            end: DEFAULT_END_SECS,
            pause_rate: DEFAULT_PAUSE_RATE,
            pause_coordinator: false,
        }
    }
}

pub struct StallWorkload {
    begin: Duration,
    end: Duration,
    pause_rate: f64,
    pause_coordinator: bool,
}

impl StallWorkload {
    pub const NAME: &str = "Stall";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: StallConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            pause_rate: c.pause_rate,
            pause_coordinator: c.pause_coordinator,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for StallWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&mut self, harness: &TestHarness) {
        info!(
            "{}: ({:?}..{:?}) pause_rate={:.0}% pause_coordinator={:?}",
            self.name(),
            self.begin,
            self.end,
            self.pause_rate * 100.0,
            self.pause_coordinator,
        );

        tokio::time::sleep(self.begin).await;

        let all_nodes = if self.pause_coordinator {
            harness.get_all_nodes()
        } else {
            harness.get_worker_ids()
        };

        let mut rng = thread_rng();
        let victims: Vec<NodeId> = all_nodes
            .iter()
            .filter(|_| rng.gen_bool(self.pause_rate))
            .copied()
            .collect();
        for &id in &victims {
            info!("{}: pause {id}", self.name());
            harness.pause(id);
        }

        tokio::time::sleep(self.end - self.begin).await;
        for &id in &victims {
            harness.resume(id);
            info!("{}: resume {id}", self.name());
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: StallWorkload::NAME,
        create: |opts, _model| Box::new(StallWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(StallWorkload::with_defaults()),
    }
}
