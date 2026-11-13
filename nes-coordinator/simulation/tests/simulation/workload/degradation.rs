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

//! Failure-injection workload that raises network latency and packet loss for a window, then restores a baseline.
//!
//! Targets the parts of the system that assume a mostly working network:
//! RPC retries and deadlines.
//! The restored latency is small but non-zero so the test never relies on instant delivery.

use crate::config;
use crate::harness::TestHarness;
use madsim::net::NetSim;
use serde::Deserialize;
use std::time::Duration;
use tracing::info;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DegradationWorkload {
    pub begin: u64,
    pub end: u64,
    pub latency_lo_ms: u64,
    pub latency_hi_ms: u64,
    pub loss_rate: f64,
}

impl DegradationWorkload {
    const NAME: &str = "Degradation";

    pub async fn run(&self, _harness: &TestHarness) {
        info!(
            "{}: ({}s..{}s) latency={}ms..{}ms loss={:.0}%",
            Self::NAME,
            self.begin,
            self.end,
            self.latency_lo_ms,
            self.latency_hi_ms,
            self.loss_rate * 100.0,
        );

        let net = NetSim::current();

        tokio::time::sleep(Duration::from_secs(self.begin)).await;
        info!("{}: increasing latency and packet loss", Self::NAME);
        net.update_config(|cfg| {
            cfg.send_latency = Duration::from_millis(self.latency_lo_ms)
                ..Duration::from_millis(self.latency_hi_ms);
            cfg.packet_loss_rate = self.loss_rate;
        });

        tokio::time::sleep(Duration::from_secs(self.end - self.begin)).await;
        net.update_config(|cfg| {
            cfg.send_latency = config::SEND_LATENCY_LO..config::SEND_LATENCY_HI;
            cfg.packet_loss_rate = 0.0;
        });
        info!("{}: restored network defaults", Self::NAME);
    }
}
