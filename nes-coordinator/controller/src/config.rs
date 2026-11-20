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

/// How long a worker loop waits between status requests to a remote worker.
pub(crate) const REMOTE_STATUS_INTERVAL: Duration = Duration::from_secs(5);

/// The same wait for an in-process worker.
/// The status request is an FFI call, so a round costs little more than its two catalog reads.
pub(crate) const EMBEDDED_STATUS_INTERVAL: Duration = Duration::from_millis(50);

/// Minimum time between two catalog writes that only mark running fragments as observed.
/// Without the bound, every round of an in-process worker with a running fragment writes to the catalog.
pub(crate) const OBSERVATION_WRITE_INTERVAL: Duration = Duration::from_secs(1);

/// Failed starts in a row at which a fragment fails.
/// A failed start is repeated after one status interval, so a remote fragment fails after a few minutes of failing starts.
pub(crate) const MAX_FAILED_STARTS: usize = 40;

/// Failed status requests in a row after which a worker is considered unreachable, and a removed worker is given up.
pub(crate) const MAX_FAILED_STATUS_REQUESTS: usize = 3;

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const VERSION_TIMEOUT: Duration = Duration::from_secs(3);

/// Bounds every call to a remote worker, including a start, which the worker answers only after it has compiled the fragment.
/// A start that exceeds it goes on at the worker, so its repeat finds the fragment already accepted.
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(2);
