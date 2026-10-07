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

#include <SemanticLatencyStats.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

namespace NES
{

namespace
{
/// Nearest-rank on a sorted copy. Exact enough for a benchmark and free of interpolation questions.
double quantile(const std::vector<double>& sorted, const double fraction)
{
    if (sorted.empty())
    {
        return 0;
    }
    const auto index = static_cast<size_t>(fraction * static_cast<double>(sorted.size() - 1) + 0.5);
    return sorted[std::min(index, sorted.size() - 1)];
}
}

SemanticLatencyStats& SemanticLatencyStats::instance()
{
    static SemanticLatencyStats stats;
    return stats;
}

void SemanticLatencyStats::record(const std::chrono::nanoseconds duration, const bool failed)
{
    const auto milliseconds = std::chrono::duration<double, std::milli>(duration).count();
    const auto now = std::chrono::steady_clock::now();

    const std::lock_guard lock(mutex);
    calls += 1;
    failures += failed ? 1 : 0;
    totalMs += milliseconds;
    maxMs = std::max(maxMs, milliseconds);
    if (samplesMs.size() < SampleCap)
    {
        samplesMs.push_back(milliseconds);
    }
    if (!firstCall.has_value())
    {
        firstCall = now;
    }
    lastCall = now;
}

std::optional<SemanticLatencyStats::Summary> SemanticLatencyStats::summarize() const
{
    const std::lock_guard lock(mutex);
    if (calls == 0)
    {
        return std::nullopt;
    }

    auto sorted = samplesMs;
    std::ranges::sort(sorted);

    /// The window from the first to the last call, so this is the rate actually achieved rather
    /// than one derived from the mean.
    const auto windowSeconds = std::chrono::duration<double>(lastCall - firstCall.value()).count();
    return Summary{
        .calls = calls,
        .failures = failures,
        .meanMs = totalMs / static_cast<double>(calls),
        .p50Ms = quantile(sorted, 0.50),
        .p95Ms = quantile(sorted, 0.95),
        .maxMs = maxMs,
        .callsPerSecond = windowSeconds > 0 ? static_cast<double>(calls) / windowSeconds : 0};
}

void SemanticLatencyStats::logAndReset(const std::string_view context)
{
    if (const auto summary = summarize())
    {
        /// A release build compiles with NES_LOGLEVEL_WARN, which removes NES_INFO entirely -- and
        /// a release build is exactly where a benchmark runs. This is a measurement, not a
        /// diagnostic, so it must not depend on how the logger was compiled. Opt in with
        /// NES_SEMANTIC_LATENCY_REPORT=1 and it goes to stderr as well, which keeps an ordinary
        /// deployment quiet.
        if (std::getenv("NES_SEMANTIC_LATENCY_REPORT") != nullptr)
        {
            fmt::print(
                stderr,
                "Semantic model latency [{}]: {} calls ({} failed), mean {:.0f} ms, p50 {:.0f} ms, p95 {:.0f} ms, max {:.0f} ms, "
                "{:.1f} calls/s\n",
                context,
                summary->calls,
                summary->failures,
                summary->meanMs,
                summary->p50Ms,
                summary->p95Ms,
                summary->maxMs,
                summary->callsPerSecond);
        }
        NES_INFO(
            "Semantic model latency [{}]: {} calls ({} failed), mean {:.0f} ms, p50 {:.0f} ms, p95 {:.0f} ms, max {:.0f} ms, "
            "{:.1f} calls/s",
            context,
            summary->calls,
            summary->failures,
            summary->meanMs,
            summary->p50Ms,
            summary->p95Ms,
            summary->maxMs,
            summary->callsPerSecond);
    }

    const std::lock_guard lock(mutex);
    samplesMs.clear();
    calls = 0;
    failures = 0;
    totalMs = 0;
    maxMs = 0;
    firstCall.reset();
}

}
