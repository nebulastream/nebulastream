#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use madsim::net::NetSim;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use serde::Deserialize;
use std::collections::HashMap;
use std::iter::once;
use std::time::Duration;
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

    fn min_workers(&self) -> u8 {
        2
    }

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: ({:?}..{:?}) rate={:.0}%",
            self.name(),
            self.begin,
            self.end,
            self.partition_rate * 100.0,
        );

        let all_names: Vec<String> = once("coordinator".to_string())
            .chain((0..harness.num_workers()).map(|i| harness.worker_name(i)))
            .collect();

        let min_nodes_for_partition: usize = 2;
        if all_names.len() < min_nodes_for_partition {
            return;
        }

        let net = NetSim::current();
        let rt = Handle::current();

        let mut rng = thread_rng();
        let mut clogged = Vec::new();
        for (i, src_name) in all_names.iter().enumerate() {
            for dst_name in &all_names[i + 1..] {
                if !rng.gen_bool(self.partition_rate) {
                    continue;
                }
                let (src_id, dst_id) = match (rt.get_node(src_name), rt.get_node(dst_name)) {
                    (Some(s), Some(d)) => (s.id(), d.id()),
                    _ => continue,
                };
                clogged.push((src_name.clone(), dst_name.clone(), src_id, dst_id));
            }
        }

        if clogged.is_empty() {
            return;
        }

        tokio::time::sleep(self.begin).await;
        for (src_name, dst_name, src_id, dst_id) in &clogged {
            info!("partition: clog {src_name} <-> {dst_name}");
            net.clog_link(*src_id, *dst_id);
            net.clog_link(*dst_id, *src_id);
        }

        tokio::time::sleep(self.end - self.begin).await;
        for (src_name, dst_name, src_id, dst_id) in &clogged {
            net.unclog_link(*src_id, *dst_id);
            net.unclog_link(*dst_id, *src_id);
            info!("partition: heal {src_name} <-> {dst_name}");
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: PartitionWorkload::NAME,
        create: |opts| Box::new(PartitionWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(PartitionWorkload::with_defaults()),
    }
}
