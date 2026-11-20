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
#include <fstream>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace NES
{

namespace
{
std::optional<std::string> callLogPathFromEnvironment()
{
    const char* const path = std::getenv("NES_SEMANTIC_CALL_LOG"); /// NOLINT(concurrency-mt-unsafe) read once, at first use
    return path != nullptr && *path != '\0' ? std::optional<std::string>{path} : std::nullopt;
}
}

SemanticCallStats::SemanticCallStats() : callLogPath(callLogPathFromEnvironment())
{
}

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

void SemanticCallStats::recordTransientStatus(const unsigned status)
{
    constexpr unsigned tooManyRequestsStatus = 429;
    const std::scoped_lock lock{mutex};
    ++(status == tooManyRequestsStatus ? tooManyRequests : serverErrors);
}

void SemanticCallStats::recordRetry(const std::chrono::milliseconds delay)
{
    const std::scoped_lock lock{mutex};
    ++retries;
    backedOff += delay;
}

void SemanticCallStats::recordUnusableImage()
{
    const std::scoped_lock lock{mutex};
    ++unusableImages;
}

void SemanticCallStats::recordCascadeRow(const std::string_view route, const bool auditDisagreed)
{
    const std::scoped_lock lock{mutex};
    if (route == "student")
    {
        ++cascadeStudent;
    }
    else if (route == "audit")
    {
        ++cascadeAudited;
        cascadeAuditDisagreed += auditDisagreed ? 1 : 0;
    }
    else
    {
        ++cascadeTeacher;
    }
}

void SemanticCallStats::recordExchange(SemanticCallRecord record)
{
    const std::scoped_lock lock{mutex};
    if (callLogPath)
    {
        calls.push_back(std::move(record));
    }
}

void SemanticCallStats::setCallLogPath(std::optional<std::string> path)
{
    const std::scoped_lock lock{mutex};
    callLogPath = std::move(path);
    if (!callLogPath)
    {
        calls.clear();
    }
}

std::string SemanticCallStats::formatCallRecord(const SemanticCallRecord& record, const std::string_view context)
{
    const auto optionalNumber
        = [](const std::optional<uint64_t> value) { return value ? nlohmann::json(*value) : nlohmann::json(nullptr); };
    nlohmann::ordered_json line;
    line["context"] = std::string{context};
    line["start_ns"] = record.startNs;
    line["end_ns"] = record.endNs;
    line["latency_ms"] = static_cast<double>(record.endNs - record.startNs) / 1e6;
    line["status"] = record.status == 0 ? nlohmann::json(nullptr) : nlohmann::json(record.status);
    line["prompt_tokens"] = optionalNumber(record.promptTokens);
    line["completion_tokens"] = optionalNumber(record.completionTokens);
    line["rows"] = record.rows;
    line["model"] = record.model;
    line["role"] = record.role.empty() ? nlohmann::json(nullptr) : nlohmann::json(record.role);
    return line.dump(-1, ' ', false, nlohmann::json::error_handler_t::replace);
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
        auto line = fmt::format(
            "Semantic model latency [{}]: {} calls ({} failed), mean {:.0f} ms, p50 {:.0f} ms, p95 {:.0f} ms, p99 {:.0f} ms, max {:.0f} "
            "ms, "
            "{:.1f} calls/s, span {:.1f} s, prompt tokens {}, completion tokens {} ({} responses, {} without usage, {} cut off at "
            "max_tokens), {} missing of {} batched rows, {} answered 429, {} answered 5xx, {} retries after {:.1f} s of backoff",
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
            batchedRows,
            tooManyRequests,
            serverErrors,
            retries,
            std::chrono::duration<double>(backedOff).count());
        /// Only with image inputs, so the line of every text run stays as the scripts know it.
        if (unusableImages > 0)
        {
            line += fmt::format(", {} rows with an unusable image", unusableImages);
        }
        /// Only with a cascade, for the same reason.
        if (cascadeStudent + cascadeTeacher + cascadeAudited > 0)
        {
            line += fmt::format(
                ", cascade: {} rows, {} answered by the student alone, {} unsure to the teacher, {} audited ({} disagreed)",
                cascadeStudent + cascadeTeacher + cascadeAudited,
                cascadeStudent,
                cascadeTeacher,
                cascadeAudited,
                cascadeAuditDisagreed);
        }
        /// A measurement, not a diagnostic: it must not depend on the compiled log level.
        if (std::getenv("NES_SEMANTIC_LATENCY_REPORT") != nullptr) /// NOLINT(concurrency-mt-unsafe)
        {
            fmt::print(stderr, "{}\n", line);
        }
        NES_INFO("{}", line);
    }
    /// The per-call log: written once, at the end of the run, so the query itself does no file I/O for it.
    if (callLogPath && !calls.empty())
    {
        std::ofstream log{*callLogPath, std::ios::app};
        for (const auto& record : calls)
        {
            log << formatCallRecord(record, context) << '\n';
        }
        log.flush();
        if (!log)
        {
            NES_WARNING("Semantic model call log: could not append {} records to {}", calls.size(), *callLogPath);
        }
    }
    calls.clear();
    latenciesMs.clear();
    failures = 0;
    firstStart.reset();
    lastEnd.reset();
    responses = responsesWithoutUsage = cutOff = batchedRows = missingRows = tooManyRequests = serverErrors = retries = 0;
    backedOff = std::chrono::milliseconds{0};
    unusableImages = 0;
    cascadeStudent = cascadeTeacher = cascadeAudited = cascadeAuditDisagreed = 0;
    promptTokens = completionTokens = 0;
}

}
