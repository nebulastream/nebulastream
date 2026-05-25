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

#include <PercentileTaskLatencyListener.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <variant>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <QueryEngineStatisticListener.hpp>

namespace NES
{

namespace
{
/// Convert a chrono duration to microseconds as a signed 64-bit integer.
int64_t toMicroseconds(const ChronoClock::duration& d)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}
} // anonymous namespace

void PercentileTaskLatencyListener::onEvent(Event event)
{
    std::visit(
        Overloaded{
            [this](const TaskExecutionStart& start)
            {
                NES_TRACE("PercentileTaskLatencyListener: TaskExecutionStart pipeline={} task={}", start.pipelineId, start.taskId);
                const TaskKey key{start.pipelineId, start.taskId};
                const std::lock_guard lock(mutex);
                /// Overwrite silently if a duplicate key arrives (defensive; should not happen in practice).
                pendingStarts.insert_or_assign(key, start.timestamp);
            },
            [this](const TaskExecutionComplete& complete)
            {
                NES_TRACE("PercentileTaskLatencyListener: TaskExecutionComplete pipeline={} task={}", complete.pipelineId, complete.taskId);
                const TaskKey key{complete.pipelineId, complete.taskId};
                const std::lock_guard lock(mutex);
                if (const auto it = pendingStarts.find(key); it != pendingStarts.end())
                {
                    const int64_t durationUs = toMicroseconds(complete.timestamp - it->second);
                    durationsUs.push_back(durationUs);
                    pendingStarts.erase(it);
                }
                else
                {
                    /// Unmatched complete (e.g. listener registered after task started): skip silently.
                    NES_DEBUG("PercentileTaskLatencyListener: unmatched TaskExecutionComplete pipeline={} task={} — skipped",
                              complete.pipelineId,
                              complete.taskId);
                }
            },
            [this](const TaskExpired& expired)
            {
                /// Drop the pending start if the task expired without completing.
                const TaskKey key{expired.pipelineId, expired.taskId};
                const std::lock_guard lock(mutex);
                pendingStarts.erase(key);
            },
            [this](const QueryStop& /*stop*/)
            {
                NES_DEBUG("PercentileTaskLatencyListener: QueryStop — emitting task latency summary");
                logAndEmit();
            },
            [this](const QueryFail& /*fail*/)
            {
                NES_DEBUG("PercentileTaskLatencyListener: QueryFail — emitting task latency summary");
                logAndEmit();
            },
            [](const auto& /*ignored*/) { /* QueryStart, QueryStopRequest, PipelineStart, PipelineStop, TaskEmit */ }},
        event);
}

PercentileTaskLatencyListener::LatencyStats PercentileTaskLatencyListener::computePercentiles(std::vector<int64_t> durations)
{
    if (durations.empty())
    {
        return LatencyStats{};
    }

    std::sort(durations.begin(), durations.end());

    const size_t n = durations.size();

    /// Nearest-rank (floor-based) percentile: for percentile P and n samples
    ///   rank = floor(P / 100.0 * n)  — 1-indexed, so 0-indexed position = rank - 1
    ///   clamped to [0, n-1].
    /// Example: n=100, P=50  → rank=50 → index=49 → durations[49]
    ///          n=100, P=95  → rank=95 → index=94 → durations[94]
    ///          n=100, P=99  → rank=99 → index=98 → durations[98]
    auto nearestRankIdx = [n](double p) -> size_t
    {
        /// rank (1-indexed) = ceil(P/100 * n), but we use floor for the standard nearest-rank lower bound.
        const auto rank = static_cast<size_t>(p / 100.0 * static_cast<double>(n));
        /// rank can be 0 when n=1 and p<100, clamp to at least index 0.
        return rank == 0 ? 0 : std::min(rank - 1, n - 1);
    };

    LatencyStats stats;
    stats.n = n;
    stats.p50Us = durations[nearestRankIdx(50.0)];
    stats.p95Us = durations[nearestRankIdx(95.0)];
    stats.p99Us = durations[nearestRankIdx(99.0)];
    stats.maxUs = durations[n - 1]; // sorted ascending, last element is max

    return stats;
}

void PercentileTaskLatencyListener::logAndEmit() const
{
    /// Take snapshot and clear BOTH accumulators under ONE lock acquisition so the emitted values
    /// reflect ONLY the just-finished query and the next query starts fresh.
    std::vector<int64_t> snapshot;
    {
        const std::lock_guard lock(mutex);
        snapshot = durationsUs;
        durationsUs.clear();
        pendingStarts.clear();
    }

    if (snapshot.empty())
    {
        NES_INFO("Task latency us: p50=0 p95=0 p99=0 max=0 n=0 (per-task)");
        return;
    }

    const LatencyStats stats = computePercentiles(std::move(snapshot));

    /// Stable, grep-parseable one-liner — same grammar family as "Spill metrics:" line.
    /// Tag: "Task latency us:" — stable for shell harness grep.
    /// Caveat suffix "(per-task)": documents that the unit is per engine task, NOT per window.
    /// Per-window latency would require an additional engine hook not currently available.
    NES_INFO("Task latency us: p50={} p95={} p99={} max={} n={} (per-task)",
             stats.p50Us,
             stats.p95Us,
             stats.p99Us,
             stats.maxUs,
             stats.n);
}

} // namespace NES
