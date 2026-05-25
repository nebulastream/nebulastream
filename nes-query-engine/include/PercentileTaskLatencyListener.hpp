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

#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{

/// @brief Computes per-task execution latency percentiles (p50/p95/p99) over a query's lifetime
///        and emits a single, grep-parseable NES_INFO line at QueryStop / QueryFail.
///
/// CAVEAT (honesty): latency is measured at the *task* granularity, not the *window* granularity.
/// The engine's public event API fires TaskExecutionStart / TaskExecutionComplete per task buffer;
/// per-window latency would require an additional engine hook (not available here).
/// The emitted line carries the suffix "(per-task)" to make this distinction visible to the harness.
///
/// Timestamp source: EventBase::timestamp (std::chrono::system_clock::time_point), set at event
/// construction time inside the engine thread — i.e. the time the engine decides the task
/// started / completed.  No separate clock call is needed in the listener.
///
/// Thread-safety: onEvent() is called from engine worker threads.  A single std::mutex guards
/// the pending-start map and the duration accumulator.  The critical section is a cheap map
/// lookup + optional vector push_back, so contention is negligible compared to task execution.
/// Percentile computation happens exactly once, at stop, outside any lock and off the eviction path.
///
/// Percentile method: nearest-rank (floor-based).  For a sorted vector of n samples the rank for
/// percentile P is  idx = floor(P/100 * n) - 1  (0-indexed, clamped to [0, n-1]).
/// This is the standard nearest-rank formula; it agrees with RocksDB's own percentile reporting.
class PercentileTaskLatencyListener final : public QueryEngineStatisticListener
{
public:
    PercentileTaskLatencyListener() = default;
    ~PercentileTaskLatencyListener() override = default;

    /// Called from engine worker threads — must not block meaningfully.
    /// Records task start timestamps and computes / accumulates durations on completion.
    /// Emits the summary log line on QueryStop or QueryFail.
    void onEvent(Event event) override;

    /// @brief Compute and return percentile statistics from the collected durations.
    ///        Exposed for unit testing via a seam — production code calls this internally at stop.
    struct LatencyStats
    {
        int64_t p50Us = 0;
        int64_t p95Us = 0;
        int64_t p99Us = 0;
        int64_t maxUs = 0;
        size_t n = 0;
    };

    /// Compute percentiles from a sorted (ascending) copy of the accumulated durations.
    /// @param durations  Vector of per-task durations in microseconds (will be sorted in-place).
    /// @return           Computed statistics; all zero if durations is empty.
    static LatencyStats computePercentiles(std::vector<int64_t> durations);

private:
    /// Key: (pipelineId, taskId) — identifies an in-flight task uniquely across the engine lifetime
    /// (taskId comes from a ThreadPool-monotonic counter that never resets between queries).
    using TaskKey = std::pair<PipelineId, TaskId>;

    struct TaskKeyHash
    {
        size_t operator()(const TaskKey& k) const noexcept
        {
            const size_t h1 = std::hash<PipelineId>{}(k.first);
            const size_t h2 = std::hash<TaskId>{}(k.second);
            /// Combine hashes using the golden-ratio mix (same pattern as boost::hash_combine)
            return h1 ^ (h2 + 0x9e3779b9ULL + (h1 << 6) + (h1 >> 2));
        }
    };

    mutable std::mutex mutex;

    /// Maps in-flight (pipelineId, taskId) → start timestamp.
    /// Populated on TaskExecutionStart; erased on TaskExecutionComplete / TaskExpired.
    /// `mutable`: cleared by the const logAndEmit() at query stop so the next query starts fresh (guarded by `mutex`).
    mutable std::unordered_map<TaskKey, ChronoClock::time_point, TaskKeyHash> pendingStarts;

    /// Collected per-task durations in microseconds, appended on each TaskExecutionComplete.
    /// `mutable`: snapshotted + cleared by the const logAndEmit() at query stop (guarded by `mutex`).
    mutable std::vector<int64_t> durationsUs;

    /// Emit the NES_INFO summary line, then reset the accumulators for the next query.  Takes `mutex` internally.
    void logAndEmit() const;
};

} // namespace NES
