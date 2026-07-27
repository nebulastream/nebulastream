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
//! These are compile-time constants for now, making them configurable is a follow-up.

use std::time::Duration;

/// How long a reconciliation loop waits between database reads when no wake-up arrives.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How often a worker's active fragments are compared with the catalog.
/// A fragment that the catalog does not want can run for about this long before it is stopped.
pub(crate) const ORPHAN_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// How long a query_fragment lifecycle waits between status reads for a remote worker.
/// The interval trades off stale information/latency against network load.
pub(crate) const REMOTE_FRAGMENT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The same wait for an in-process worker.
/// The read is an FFI call, so it costs almost nothing.
pub(crate) const EMBEDDED_FRAGMENT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Delay before retrying a query_fragment step after a retryable failure.
pub(crate) const RETRY_INTERVAL: Duration = Duration::from_secs(2);

pub(crate) const TASK_RESPAWN_DELAY: Duration = Duration::from_secs(2);

pub(crate) const REMOVED_WORKER_STOP_DEADLINE: Duration = Duration::from_secs(5);

pub(crate) const OBSERVATION_WRITE_INTERVAL: Duration = Duration::from_secs(1);

/// Retryable failures in a row at which a query_fragment fails.
/// Every failure is one call that took at most the RPC deadline, followed by the retry delay,
/// so a fragment is given up after about two and a half minutes of a worker not answering.
pub(crate) const MAX_CONSECUTIVE_FAILURES: u32 = 40;

/// How often a connected worker is health-checked.
pub(crate) const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) const HEALTH_CHECK_MAX_MISSES: u32 = 3;

/// Deadline for a single connection attempt.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Deadline for asking a worker for its version.
pub(crate) const VERSION_TIMEOUT: Duration = Duration::from_secs(3);

/// Wait before reconnecting after every connection attempt failed.
pub(crate) const RECONNECT_INTERVAL: Duration = Duration::from_secs(5);

/// Deadline for a single RPC attempt.
pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(2);
