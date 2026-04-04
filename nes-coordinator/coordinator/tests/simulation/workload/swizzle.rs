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
use std::time::Duration;
use tracing::info;

const DEFAULT_END_SECS: u64 = 30;
const DEFAULT_SWIZZLE_RATE: f64 = 0.5;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SwizzleClogConfig {
    begin: u64,
    end: u64,
    swizzle_rate: f64,
    include_coordinator: bool,
}

impl Default for SwizzleClogConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            end: DEFAULT_END_SECS,
            swizzle_rate: DEFAULT_SWIZZLE_RATE,
            include_coordinator: false,
        }
    }
}

pub struct SwizzleClogWorkload {
    begin: Duration,
    end: Duration,
    swizzle_rate: f64,
    include_coordinator: bool,
}

impl SwizzleClogWorkload {
    pub const NAME: &str = "SwizzleClog";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: SwizzleClogConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            swizzle_rate: c.swizzle_rate,
            include_coordinator: c.include_coordinator,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[derive(Clone, Copy)]
enum Event {
    Clog(NodeId),
    Unclog(NodeId),
}

#[async_trait(?Send)]
impl Workload for SwizzleClogWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&mut self, harness: &TestHarness) {
        info!(
            "{}: ({:?}..{:?}) swizzle_rate={:.0}% include_coordinator={:?}",
            self.name(),
            self.begin,
            self.end,
            self.swizzle_rate * 100.0,
            self.include_coordinator,
        );

        tokio::time::sleep(self.begin).await;

        let candidates = if self.include_coordinator {
            harness.get_all_nodes()
        } else {
            harness.get_workers()
        };

        let t = self.end - self.begin;
        let half = t / 2;

        let mut rng = thread_rng();
        let mut victims: Vec<(NodeId, Duration, Duration)> = Vec::new();
        for id in candidates {
            if !rng.gen_bool(self.swizzle_rate) {
                continue;
            }
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
                    info!("{}: clog {id}", self.name());
                    harness.clog_node(id);
                }
                Event::Unclog(id) => {
                    harness.unclog_node(id);
                    info!("{}: unclog {id}", self.name());
                }
            }
        }

        if elapsed < t {
            tokio::time::sleep(t - elapsed).await;
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: SwizzleClogWorkload::NAME,
        create: |opts, _model| Box::new(SwizzleClogWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(SwizzleClogWorkload::with_defaults()),
    }
}
