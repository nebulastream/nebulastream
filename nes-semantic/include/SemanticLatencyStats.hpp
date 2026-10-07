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

#include <chrono>
#include <cstddef>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

namespace NES
{

/// Latency of the model round trips, which is the number a throughput figure has to be read
/// against: throughput derived from wall time alone cannot tell waiting apart from service time.
///
/// Recorded in `HttpSemanticBackend`, the one place both execution modes pass through, so the
/// synchronous operator and the asynchronous executor are measured identically. Process-wide,
/// because the synchronous operator keeps one backend per worker thread and the asynchronous one
/// per calling thread, and a per-instance number would say nothing.
class SemanticLatencyStats
{
public:
    static SemanticLatencyStats& instance();

    /// One finished round trip, retries included: what the caller actually waited for.
    void record(std::chrono::nanoseconds duration, bool failed);

    struct Summary
    {
        size_t calls;
        size_t failures;
        double meanMs;
        double p50Ms;
        double p95Ms;
        double maxMs;
        /// Calls per second the endpoint sustained, i.e. concurrency achieved divided by mean
        /// latency. Compare it against the throughput of the query as a whole.
        double callsPerSecond;
    };

    [[nodiscard]] std::optional<Summary> summarize() const;

    /// Writes the summary at info level and starts over, so the log carries one line per query
    /// rather than one per process. Called when an operator finishes.
    void logAndReset(std::string_view context);

private:
    SemanticLatencyStats() = default;

    /// Samples are kept to compute quantiles, which a running mean cannot give. Capped, because a
    /// long run would otherwise grow without bound; beyond the cap the count, the mean and the
    /// maximum stay exact while the quantiles describe the first samples.
    static constexpr size_t SampleCap = 200'000;

    mutable std::mutex mutex;
    std::vector<double> samplesMs;
    size_t calls = 0;
    size_t failures = 0;
    double totalMs = 0;
    double maxMs = 0;
    std::optional<std::chrono::steady_clock::time_point> firstCall;
    std::chrono::steady_clock::time_point lastCall;
};

}
