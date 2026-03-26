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
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

const DEFAULT_TIMEOUT_SECS: u64 = 600;
const DEFAULT_BUGGIFY_PROBABILITY: f64 = 0.25;

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub test: Vec<TestSpec>,
}

#[derive(Debug, Deserialize)]
pub struct TestSpec {
    pub title: Option<String>,
    pub timeout: Option<u64>,
    pub buggify: Option<bool>,
    pub run_failure_workloads: Option<bool>,
    pub network: Option<NetworkConfig>,
    pub workload: Option<Vec<WorkloadOptions>>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(try_from = "RawNetworkConfig")]
pub struct NetworkConfig {
    pub send_latency_lo_ms: Option<u64>,
    pub send_latency_hi_ms: Option<u64>,
    pub packet_loss_rate: Option<f64>,
}

#[derive(Deserialize)]
struct RawNetworkConfig {
    send_latency_lo_ms: Option<u64>,
    send_latency_hi_ms: Option<u64>,
    packet_loss_rate: Option<f64>,
}

impl TryFrom<RawNetworkConfig> for NetworkConfig {
    type Error = String;

    fn try_from(raw: RawNetworkConfig) -> Result<Self, Self::Error> {
        if let (Some(lo), Some(hi)) = (raw.send_latency_lo_ms, raw.send_latency_hi_ms) {
            if lo > hi {
                return Err(format!(
                    "send_latency_lo_ms ({lo}) must be <= send_latency_hi_ms ({hi})"
                ));
            }
        }
        if let Some(rate) = raw.packet_loss_rate {
            if !(0.0..=1.0).contains(&rate) {
                return Err(format!(
                    "packet_loss_rate ({rate}) must be in [0.0, 1.0]"
                ));
            }
        }
        Ok(NetworkConfig {
            send_latency_lo_ms: raw.send_latency_lo_ms,
            send_latency_hi_ms: raw.send_latency_hi_ms,
            packet_loss_rate: raw.packet_loss_rate,
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct WorkloadOptions {
    pub name: String,
    #[serde(flatten)]
    pub options: HashMap<String, toml::Value>,
}

impl TestSpec {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT_SECS))
    }

    pub fn run_failure_workloads(&self) -> bool {
        self.run_failure_workloads.unwrap_or(true)
    }

    pub fn buggify_probability() -> f64 {
        DEFAULT_BUGGIFY_PROBABILITY
    }
}


