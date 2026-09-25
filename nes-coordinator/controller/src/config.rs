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

/// How long a query_fragment lifecycle waits between status reads for a remote worker.
/// The interval trades off stale information/latency against network load.
pub(crate) const REMOTE_FRAGMENT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The same wait for an in-process worker.
/// The read is an FFI call, so it costs almost nothing.
pub(crate) const EMBEDDED_FRAGMENT_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Delay before retrying a query_fragment step after a retryable failure.
pub(crate) const RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Retryable failures in a row that a query_fragment tolerates before it fails.
pub(crate) const MAX_CONSECUTIVE_FAILURES: u32 = 10;

/// How often a connected worker is health-checked.
pub(crate) const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Deadline for a single health check probe.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Deadline for a single connection attempt.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Deadline for asking a worker for its version.
pub(crate) const VERSION_TIMEOUT: Duration = Duration::from_secs(3);

/// Connection attempts before a worker is marked unreachable.
pub(crate) const CONNECT_MAX_RETRIES: usize = 8;

/// Upper bound on the backoff between connection attempts.
pub(crate) const CONNECT_MAX_DELAY: Duration = Duration::from_secs(5);

/// Wait before reconnecting after every connection attempt failed.
pub(crate) const RECONNECT_INTERVAL: Duration = Duration::from_secs(30);

/// Keep-alive ping interval on a worker connection.
pub(crate) const ENDPOINT_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// Wait for a keep-alive ping before dropping the connection.
pub(crate) const ENDPOINT_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(60);

/// Deadline for a single RPC attempt.
pub(crate) const RPC_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(2);

/// Deadline covering all retries of one single RPC.
pub(crate) const RPC_TOTAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Growth rate of the retry backoff: each retry waits this many times longer than the last.
pub(crate) const RETRY_BACKOFF_BASE: u64 = 2;

/// Multiplier that scales the backoff into milliseconds.
/// With the base above the delays are 50, 100, 200, 400, ... ms.
pub(crate) const RETRY_BACKOFF_FACTOR_MS: u64 = 25;

/// Retry attempts for a single RPC.
pub(crate) const RPC_MAX_RETRIES: usize = 5;
