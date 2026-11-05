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

#include <SemanticCallStats.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <numeric>
#include <string_view>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>

namespace NES
{

SemanticCallStats& SemanticCallStats::instance()
{
    static SemanticCallStats stats;
    return stats;
}

void SemanticCallStats::recordCall(
    const std::chrono::steady_clock::time_point start, const std::chrono::steady_clock::time_point end, const bool failed)
{
    const std::scoped_lock lock{mutex};
    latenciesMs.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    failures += failed ? 1 : 0;
    firstStart = firstStart ? std::min(*firstStart, start) : start;
    lastEnd = lastEnd ? std::max(*lastEnd, end) : end;
}

void SemanticCallStats::recordUsage(const bool hasUsage, const uint64_t prompt, const uint64_t completion, const bool wasCutOff)
{
    const std::scoped_lock lock{mutex};
    ++responses;
    responsesWithoutUsage += hasUsage ? 0 : 1;
    promptTokens += prompt;
    completionTokens += completion;
    cutOff += wasCutOff ? 1 : 0;
}

void SemanticCallStats::recordBatch(const size_t rows, const size_t missing)
{
    const std::scoped_lock lock{mutex};
    batchedRows += rows;
    missingRows += missing;
}

void SemanticCallStats::reportAndReset(const std::string_view context)
{
    const std::scoped_lock lock{mutex};
    if (!latenciesMs.empty())
    {
        auto sorted = latenciesMs;
        std::ranges::sort(sorted);
        const auto at = [&sorted](const double quantile)
        { return sorted[std::min(sorted.size() - 1, static_cast<size_t>(quantile * static_cast<double>(sorted.size())))]; };
        const double mean = std::accumulate(sorted.begin(), sorted.end(), 0.0) / static_cast<double>(sorted.size());
        const double span = std::chrono::duration<double>(*lastEnd - *firstStart).count();
        const auto line = fmt::format(
            "Semantic model latency [{}]: {} calls ({} failed), mean {:.0f} ms, p50 {:.0f} ms, p95 {:.0f} ms, p99 {:.0f} ms, max {:.0f} "
            "ms, "
            "{:.1f} calls/s, span {:.1f} s, prompt tokens {}, completion tokens {} ({} responses, {} without usage, {} cut off at "
            "max_tokens), {} missing of {} batched rows",
            context,
            sorted.size(),
            failures,
            mean,
            at(0.5),
            at(0.95),
            at(0.99),
            sorted.back(),
            span > 0 ? static_cast<double>(sorted.size()) / span : 0.0,
            span,
            promptTokens,
            completionTokens,
            responses,
            responsesWithoutUsage,
            cutOff,
            missingRows,
            batchedRows);
        /// A measurement, not a diagnostic: it must not depend on the compiled log level.
        if (std::getenv("NES_SEMANTIC_LATENCY_REPORT") != nullptr) /// NOLINT(concurrency-mt-unsafe)
        {
            fmt::print(stderr, "{}\n", line);
        }
        NES_INFO("{}", line);
    }
    latenciesMs.clear();
    failures = 0;
    firstStart.reset();
    lastEnd.reset();
    responses = responsesWithoutUsage = cutOff = batchedRows = missingRows = 0;
    promptTokens = completionTokens = 0;
}

}
