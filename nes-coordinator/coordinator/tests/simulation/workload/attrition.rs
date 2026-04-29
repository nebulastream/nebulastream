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

#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;
use tracing::info;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AttritionConfig {
    begin: u64,
    kill_rate: f64,
    restart_delay_lo_secs: u64,
    restart_delay_hi_secs: u64,
    kill_coordinator: bool,
}

impl Default for AttritionConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            kill_rate: 0.20,
            restart_delay_lo_secs: 1,
            restart_delay_hi_secs: 10,
            kill_coordinator: false,
        }
    }
}

pub struct AttritionWorkload {
    begin: Duration,
    kill_rate: f64,
    restart_delay: Range<Duration>,
    kill_coordinator: bool,
}

impl AttritionWorkload {
    pub const NAME: &str = "Attrition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let cfg: AttritionConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(cfg.begin),
            kill_rate: cfg.kill_rate,
            restart_delay: Duration::from_secs(cfg.restart_delay_lo_secs)
                ..Duration::from_secs(cfg.restart_delay_hi_secs),
            kill_coordinator: cfg.kill_coordinator,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for AttritionWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&mut self, harness: &TestHarness) {
        info!(
            "{}: kill_at={:?} kill_rate={:.0}% restart_delay={:?}..{:?} kill_coordinator={:?}",
            self.name(),
            self.begin,
            self.kill_rate * 100.0,
            self.restart_delay.start,
            self.restart_delay.end,
            self.kill_coordinator
        );

        tokio::time::sleep(self.begin).await;

        let mut rng = thread_rng();
        let all_nodes = if self.kill_coordinator {
            harness.get_all_nodes()
        } else {
            harness.get_worker_ids()
        };
        let victims: Vec<NodeId> = all_nodes
            .iter()
            .filter(|_| rng.gen_bool(self.kill_rate))
            .copied()
            .collect();

        // each victim gets its own random restart delay
        let mut restarts: Vec<(NodeId, Duration)> = victims
            .iter()
            .map(|&id| (id, rng.gen_range(self.restart_delay.clone())))
            .collect();
        restarts.sort_by_key(|&(_, delay)| delay);

        // all nodes are killed at the same instant
        for &(id, _) in &restarts {
            info!("{}: kill {id}", self.name());
            harness.kill(&id);
        }

        let mut elapsed = Duration::ZERO;
        for &(id, delay) in &restarts {
            if delay > elapsed {
                tokio::time::sleep(delay - elapsed).await;
                elapsed = delay;
            }
            info!("{}: restart {id} (after {delay:?})", self.name());
            harness.restart(&id);
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: AttritionWorkload::NAME,
        create: |opts, _model| Box::new(AttritionWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(AttritionWorkload::with_defaults()),
    }
}
