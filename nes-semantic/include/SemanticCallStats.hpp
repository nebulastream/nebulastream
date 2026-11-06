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
#include <cstdint>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

namespace NES
{

/// What the model calls of `SEM_CALL` took and cost, process-wide: the round trip of every exchange and the tokens of
/// every chat-completion `usage` block. Reported as one line, in the wording the benchmark scripts grep for
/// (scripts/summarize_sem_map_comparison.py, docs/design/20261006_async_operator_measurements.md), with
/// NES_SEMANTIC_LATENCY_REPORT=1.
class SemanticCallStats
{
public:
    static SemanticCallStats& instance();

    void recordCall(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end, bool failed);
    /// `hasUsage` is false when the envelope carried no `usage` block; `cutOff` is `finish_reason == "length"`.
    void recordUsage(bool hasUsage, uint64_t promptTokens, uint64_t completionTokens, bool cutOff);
    /// A batched request of `rows` rows whose answer left out `missing` of them.
    void recordBatch(size_t rows, size_t missing);
    /// A response with a status that `isTransientStatus` lets the Async source retry.
    void recordTransientStatus(unsigned status);
    /// The Async source waits `delay` before it repeats a failed call.
    void recordRetry(std::chrono::milliseconds delay);

    /// Writes the line if anything was recorded, then starts over. `context` names the path, e.g. "SEM_CALL".
    void reportAndReset(std::string_view context);

private:
    SemanticCallStats() = default;

    std::mutex mutex;
    std::vector<double> latenciesMs;
    size_t failures = 0;
    std::optional<std::chrono::steady_clock::time_point> firstStart;
    std::optional<std::chrono::steady_clock::time_point> lastEnd;
    size_t responses = 0;
    size_t responsesWithoutUsage = 0;
    uint64_t promptTokens = 0;
    uint64_t completionTokens = 0;
    size_t cutOff = 0;
    size_t batchedRows = 0;
    size_t missingRows = 0;
    size_t tooManyRequests = 0;
    size_t serverErrors = 0;
    size_t retries = 0;
    std::chrono::milliseconds backedOff{0};
};

}
