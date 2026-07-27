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

//! Timing and retry values for the reconciliation loops and the worker RPC
//! layer, collected in one place. These are compile-time constants for now;
//! making them settable from operator configuration is a follow-up.

use std::time::Duration;

/// How long a reconciliation loop waits between database reads when no wake-up
/// arrives. Applies to the controller and the per-worker loops.
pub(crate) const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// How long a fragment lifecycle waits between status reads of a worker it reaches
/// over the network. Each read is a round trip, so the interval trades how long a
/// finished fragment goes unnoticed against the load of asking.
pub(crate) const REMOTE_FRAGMENT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The same wait for a worker in the coordinator's own process. The read is a
/// function call rather than a round trip, so it costs almost nothing and the
/// interval is set by how promptly a finished fragment should be noticed.
pub(crate) const EMBEDDED_FRAGMENT_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Delay before retrying a fragment step after a retryable failure.
pub(crate) const RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Retryable failures in a row a fragment tolerates before it fails.
pub(crate) const MAX_CONSECUTIVE_FAILURES: u32 = 10;

/// How often a connected worker is health-checked.
pub(crate) const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// Deadline for a single health check probe.
pub(crate) const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Deadline for a single connection attempt.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// Deadline for reaching one worker and reading the version it runs.
/// Short and tried once, because a caller asked a question about every worker and would rather be
/// told promptly that one is unreachable than wait out the retries that starting a query is worth.
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

/// Deadline covering all retries of one RPC.
pub(crate) const RPC_TOTAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Initial backoff for the RPC retry schedule, in milliseconds.
pub(crate) const RPC_RETRY_INIT: u64 = 50;

/// Retry attempts for a single RPC.
pub(crate) const RPC_MAX_RETRIES: usize = 5;
