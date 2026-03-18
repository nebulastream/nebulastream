#![cfg(madsim)]
use crate::harness::TestHarness;
use crate::workload::{FailureInjectorFactory, Workload, WorkloadFactory, parse_options};
use async_trait::async_trait;
use madsim::rand::{Rng, thread_rng};
use madsim::runtime::Handle;
use serde::Deserialize;
use std::collections::HashMap;
use std::ops::Range;
use std::time::Duration;
use tracing::info;

const DEFAULT_END_SECS: u64 = 30;

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AttritionConfig {
    begin: u64,
    end: u64,
    kill_rate: f64,
    restart_delay_lo_secs: u64,
    restart_delay_hi_secs: u64,
    kill_coordinator: bool,
}

impl Default for AttritionConfig {
    fn default() -> Self {
        Self {
            begin: 0,
            end: DEFAULT_END_SECS,
            kill_rate: 0.20,
            restart_delay_lo_secs: 1,
            restart_delay_hi_secs: 10,
            kill_coordinator: false,
        }
    }
}

pub struct AttritionWorkload {
    begin: Duration,
    end: Duration,
    kill_rate: f64,
    restart_delay: Range<Duration>,
    kill_coordinator: bool,
}

impl AttritionWorkload {
    pub const NAME: &str = "Attrition";

    pub fn from_options(options: &HashMap<String, toml::Value>) -> Self {
        let c: AttritionConfig = parse_options(options);
        Self {
            begin: Duration::from_secs(c.begin),
            end: Duration::from_secs(c.end),
            kill_rate: c.kill_rate,
            restart_delay: Duration::from_secs(c.restart_delay_lo_secs)
                ..Duration::from_secs(c.restart_delay_hi_secs),
            kill_coordinator: c.kill_coordinator,
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

    async fn start(&self, harness: &TestHarness) {
        info!(
            "{}: ({:?}..{:?}) kill_rate={:.0}% restart_delay={:?}..{:?}",
            self.name(),
            self.begin,
            self.end,
            self.kill_rate * 100.0,
            self.restart_delay.start,
            self.restart_delay.end,
        );

        tokio::time::sleep(self.begin).await;

        let mut node_names: Vec<String> = (0..harness.num_workers())
            .map(|i| harness.worker_name(i))
            .collect();
        if self.kill_coordinator {
            node_names.push(harness.coordinator_name().to_string());
        }

        let mut rng = thread_rng();
        let victims: Vec<String> = node_names
            .iter()
            .filter(|_| rng.gen_bool(self.kill_rate))
            .cloned()
            .collect();

        if victims.is_empty() {
            return;
        }

        for name in &victims {
            info!("attrition: kill {name}");
            Handle::current().kill(name);
        }

        let restart_at = self.begin + rng.gen_range(self.restart_delay.clone());
        let sleep_until = restart_at.min(self.end);
        tokio::time::sleep(sleep_until - self.begin).await;

        for name in &victims {
            info!("attrition: restart {name}");
            Handle::current().restart(name);
        }
    }
}

inventory::submit! {
    WorkloadFactory {
        name: AttritionWorkload::NAME,
        create: |opts| Box::new(AttritionWorkload::from_options(opts)),
    }
}

inventory::submit! {
    FailureInjectorFactory {
        should_inject: |already_added| already_added == 0,
        create: || Box::new(AttritionWorkload::with_defaults()),
    }
}
