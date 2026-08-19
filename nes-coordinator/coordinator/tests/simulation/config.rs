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

//! TOML schema for simulation trial configs.
//!
//! A test file in `tests/simulation/configs` holds one or more trial
//! entries. Each entry pins simulator behavior (timeout, buggify, network
//! noise) and lists the workloads to run, by name plus a free-form
//! options table. The runner uses these types to turn the TOML into a
//! plan for a single madsim run.

#![cfg(madsim)]
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tracing::warn;

const DEFAULT_TIMEOUT_SECS: u64 = 600;
const DEFAULT_BUGGIFY_PROBABILITY: f64 = 0.25;

/// Network conditions the harness applies to the simulated network.
pub const SEND_LATENCY_LO: Duration = Duration::from_millis(1);
pub const SEND_LATENCY_HI: Duration = Duration::from_millis(100);
pub const SEND_TIMEOUT: Duration = Duration::from_secs(30);

/// Window a failure workload runs over when it is auto-injected, and so has
/// no config to read a window from.
pub const INJECTION_WINDOW_SECS: u64 = 30;

/// Rates a failure workload falls back to when its config leaves them unset.
pub const PARTITION_RATE: f64 = 0.15;
pub const PAUSE_RATE: f64 = 0.20;
pub const SWIZZLE_RATE: f64 = 0.5;
pub const DEGRADATION_LATENCY_LO_MS: u64 = 50;
pub const DEGRADATION_LATENCY_HI_MS: u64 = 1000;
pub const DEGRADATION_LOSS_RATE: f64 = 0.05;

#[derive(Debug, Deserialize)]
pub struct TestFile {
    pub test: Vec<TestConfig>,
}

#[derive(Debug, Deserialize)]
pub struct TestConfig {
    pub title: Option<String>,
    pub timeout: Option<u64>,
    pub buggify: Option<bool>,
    pub buggify_end: Option<u64>,
    pub run_failure_workloads: Option<bool>,
    pub network: Option<NetworkConfig>,
    pub workload: Option<Vec<WorkloadOptions>>,
}

/// Validated network-noise settings applied to the simulated network for
/// the entire trial. Validation happens during deserialization so a bad
/// config fails at trial discovery rather than during the run.
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
        if let (Some(lo), Some(hi)) = (raw.send_latency_lo_ms, raw.send_latency_hi_ms)
            && lo > hi
        {
            return Err(format!(
                "send_latency_lo_ms ({lo}) must be <= send_latency_hi_ms ({hi})"
            ));
        }
        if let Some(rate) = raw.packet_loss_rate
            && !(0.0..=1.0).contains(&rate)
        {
            return Err(format!("packet_loss_rate ({rate}) must be in [0.0, 1.0]"));
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

impl TestConfig {
    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout.unwrap_or(DEFAULT_TIMEOUT_SECS))
    }

    pub fn run_failure_workloads(&self) -> bool {
        self.run_failure_workloads.unwrap_or(true)
    }

    pub fn buggify_probability() -> f64 {
        DEFAULT_BUGGIFY_PROBABILITY
    }

    /// Warn about workload windows that run past the test timeout, since the
    /// test ends before they do and their tail is cut off.
    pub fn warn_on_windows_past_timeout(&self) {
        let timeout = self.timeout().as_secs();
        for w in self.workload.iter().flatten() {
            for key in ["begin", "end"] {
                if let Some(secs) = w.options.get(key).and_then(toml::Value::as_integer)
                    && secs as u64 > timeout
                {
                    warn!(
                        "workload '{}': {key} = {secs}s is past the test timeout of {timeout}s",
                        w.name
                    );
                }
            }
        }
    }
}
