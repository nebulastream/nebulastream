#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use madsim::net::NetSim;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

const DEFAULT_END_SECS: u64 = 30;
const DEFAULT_LATENCY_LO_MS: u64 = 50;
const DEFAULT_LATENCY_HI_MS: u64 = 500;
const DEFAULT_LOSS_RATE: f64 = 0.1;
const RESTORE_LATENCY_LO_MS: u64 = 1;
const RESTORE_LATENCY_HI_MS: u64 = 100;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DegradationConfig {
    begin: u64,
    end: u64,
    latency_lo_ms: u64,
    latency_hi_ms: u64,
    loss_rate: f64,
}

impl Default for DegradationConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            end: DEFAULT_END_SECS,
            latency_lo_ms: DEFAULT_LATENCY_LO_MS,
            latency_hi_ms: DEFAULT_LATENCY_HI_MS,
            loss_rate: DEFAULT_LOSS_RATE,
        }
    }
}

pub struct DegradationWorkload {
    begin: Duration,
    end: Duration,
    latency_lo: Duration,
    latency_hi: Duration,
    loss_rate: f64,
}

impl DegradationWorkload {
    pub const NAME: &str = "Degradation";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: DegradationConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            latency_lo: Duration::from_millis(c.latency_lo_ms),
            latency_hi: Duration::from_millis(c.latency_hi_ms),
            loss_rate: c.loss_rate,
        }
    }

    pub fn with_defaults() -> Self {
        Self::from_options(&HashMap::new())
    }
}

#[async_trait(?Send)]
impl Workload for DegradationWorkload {
    fn name(&self) -> &str {
        Self::NAME
    }

    async fn start(&self, _harness: &TestHarness) {
        info!(
            "{}: ({:?}..{:?}) latency={:?}..{:?} loss={:.0}%",
            self.name(),
            self.begin,
            self.end,
            self.latency_lo,
            self.latency_hi,
            self.loss_rate * 100.0,
        );

        let net = NetSim::current();

        tokio::time::sleep(self.begin).await;
        info!("degradation: increasing latency and loss");
        net.update_config(|cfg| {
            cfg.send_latency = self.latency_lo..self.latency_hi;
            cfg.packet_loss_rate = self.loss_rate;
        });

        tokio::time::sleep(self.end - self.begin).await;
        net.update_config(|cfg| {
            cfg.send_latency = Duration::from_millis(RESTORE_LATENCY_LO_MS)
                ..Duration::from_millis(RESTORE_LATENCY_HI_MS);
            cfg.packet_loss_rate = 0.0;
        });
        info!("degradation: restored network defaults");
    }
}

inventory::submit! {
    WorkloadFactory {
        name: DegradationWorkload::NAME,
        create: |opts| Box::new(DegradationWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(DegradationWorkload::with_defaults()),
    }
}
