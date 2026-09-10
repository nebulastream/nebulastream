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

//! Failure-injection workload that clogs random directed links over the
//! simulated network for a windowed period.
//!
//! This is looser than a textbook partition, which splits the nodes into
//! groups with no links between them. Here each directed link is clogged on
//! its own with probability `partition_rate`, so the broken links form a
//! random, usually asymmetric set: A may be unable to reach B while B can
//! still reach A. That covers clean partitions and also the messier
//! reachability failures that reconnect logic has to handle in practice.

#![cfg(madsim)]
use crate::config;
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_required};
use async_trait::async_trait;
use madsim::rand::{Rng, thread_rng};
use madsim::task::NodeId;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

fn default_partition_rate() -> f64 {
    config::PARTITION_RATE
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PartitionConfig {
    begin: u64,
    end: u64,
    #[serde(default = "default_partition_rate")]
    partition_rate: f64,
}

pub struct PartitionWorkload {
    begin: Duration,
    end: Duration,
    partition_rate: f64,
}

impl PartitionWorkload {
    pub const NAME: &str = "NetworkPartition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: PartitionConfig = parse_required(options);
        Self {
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            partition_rate: c.partition_rate,
        }
    }

    pub fn for_injection() -> Self {
        Self {
            begin: Duration::from_secs(0),
            end: Duration::from_secs(config::INJECTION_WINDOW_SECS),
            partition_rate: config::PARTITION_RATE,
        }
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
        create: || Box::new(PartitionWorkload::for_injection()),
    }
}
