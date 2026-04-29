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
use std::time::{Duration, Instant};
use tracing::info;

const DEFAULT_END_SECS: u64 = 30;
const DEFAULT_PARTITION_RATE: f64 = 0.15;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PartitionConfig {
    begin: u64,
    end: u64,
    partition_rate: f64,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            end: DEFAULT_END_SECS,
            partition_rate: DEFAULT_PARTITION_RATE,
        }
    }
}

pub struct PartitionWorkload {
    begin: Duration,
    end: Duration,
    partition_rate: f64,
}

impl PartitionWorkload {
    pub const NAME: &str = "NetworkPartition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: PartitionConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            partition_rate: c.partition_rate,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for PartitionWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&mut self, harness: &TestHarness) {
        info!(
            "{}: ({:?}..{:?}) rate={:.0}%",
            self.name(),
            self.begin,
            self.end,
            self.partition_rate * 100.0,
        );

        tokio::time::sleep(self.begin).await;

        let all_nodes = harness.get_all_nodes();
        let mut rng = thread_rng();
        let mut clogged: Vec<(NodeId, NodeId)> = Vec::new();
        for (i, &src) in all_nodes.iter().enumerate() {
            for &dst in &all_nodes[i + 1..] {
                if rng.gen_bool(self.partition_rate) {
                    clogged.push((src, dst));
                }
            }
        }
        for &(src, dst) in &clogged {
            info!("partition: clog {src} <-> {dst}");
            harness.clog_link(src, dst);
            harness.clog_link(dst, src);
        }

        tokio::time::sleep(self.end - self.begin).await;
        for &(src, dst) in &clogged {
            harness.unclog_link(src, dst);
            harness.unclog_link(dst, src);
            info!("partition: heal {src} <-> {dst}");
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: PartitionWorkload::NAME,
        create: |opts, _model| Box::new(PartitionWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(PartitionWorkload::with_defaults()),
    }
}
