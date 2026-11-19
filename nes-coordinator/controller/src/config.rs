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

//! Timing and retry values for the reconciliation loops and the worker RPC layer.

use std::time::Duration;

/// How long the controller waits between catalog reads when no wake-up arrives.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How long a worker loop waits between status reports of a remote worker.
/// A shorter interval detects state changes sooner and adds network traffic.
pub(crate) const REMOTE_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// The same wait for an in-process worker.
/// The report is an FFI call, so it costs almost nothing.
pub(crate) const EMBEDDED_REPORT_INTERVAL: Duration = Duration::from_millis(50);

/// Minimum time between two catalog writes that only mark running fragments as observed.
pub(crate) const OBSERVATION_WRITE_INTERVAL: Duration = Duration::from_secs(1);

/// Failed starts in a row at which a fragment fails.
/// A failed start is repeated at the next report, so a remote fragment fails after a few minutes of failing starts.
pub(crate) const MAX_CONSECUTIVE_FAILURES: u32 = 40;

/// Missed reports in a row at which a worker counts as unreachable, and a removed worker is given up.
pub(crate) const HEALTH_CHECK_MAX_MISSES: u32 = 3;

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const VERSION_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(2);
