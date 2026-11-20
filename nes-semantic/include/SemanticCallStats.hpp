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
#include <string>
#include <string_view>
#include <vector>

namespace NES
{

/// One HTTP exchange with a model, for the per-call log (`NES_SEMANTIC_CALL_LOG`).
struct SemanticCallRecord
{
    /// system_clock, ns since the epoch: comparable with DataGen's t0 and the benchmark tracker's arrival times.
    int64_t startNs = 0;
    int64_t endNs = 0;
    /// The HTTP status; 0 when no response came (connection failure, timeout).
    unsigned status = 0;
    /// From the envelope's `usage` block; empty without one (and for a non-2xx answer).
    std::optional<uint64_t> promptTokens;
    std::optional<uint64_t> completionTokens;
    /// Rows (or join pairs) the request carries: BATCH_SIZE for a batched request, else 1.
    size_t rows = 1;
    std::string model;
    /// A cascade's call: "student", "teacher" or "audit"; empty outside a cascade.
    std::string role;

    bool operator==(const SemanticCallRecord&) const = default;
};

/// What the model calls of `SEM_CALL` took and cost, process-wide: the round trip of every exchange and the tokens of
/// every chat-completion `usage` block. Reported as one line, in the wording the benchmark scripts grep for
/// (scripts/summarize_sem_map_comparison.py, docs/design/20261006_async_operator_measurements.md), with
/// NES_SEMANTIC_LATENCY_REPORT=1.
///
/// With NES_SEMANTIC_CALL_LOG=<path>, every HTTP exchange is also kept as a `SemanticCallRecord` (in memory, under the
/// same lock, no file I/O while the query runs) and `reportAndReset` appends them to <path> as JSON lines, one per
/// exchange (`formatCallRecord`). Without it nothing is kept and the behaviour is as before.
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
    /// A row whose image (`LLM.IMAGE_FIELDS`) could not be used, so it got the default without a request.
    void recordUnusableImage();
    /// A row at a cascade's teacher call (LLM.PROXY_MODEL): the student's route for it and, for an audited row, whether
    /// the teacher's verdict differed from the student's.
    void recordCascadeRow(std::string_view route, bool auditDisagreed);
    /// Keeps `record` for the per-call log; does nothing without a log path.
    void recordExchange(SemanticCallRecord record);
    /// Sets (or with nullopt clears) the per-call log's path, which comes from NES_SEMANTIC_CALL_LOG at start.
    void setCallLogPath(std::optional<std::string> path);
    /// One record as a JSON line (no newline): context, start_ns, end_ns, latency_ms, status (null: no response),
    /// prompt_tokens and completion_tokens (null: no usage block), rows, model, role (null outside a cascade).
    static std::string formatCallRecord(const SemanticCallRecord& record, std::string_view context);

    /// Writes the line if anything was recorded, appends the per-call records to the log if one is set, then starts over.
    /// `context` names the path, e.g. "SEM_CALL".
    void reportAndReset(std::string_view context);

private:
    SemanticCallStats();

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
    size_t unusableImages = 0;
    size_t cascadeStudent = 0;
    size_t cascadeTeacher = 0;
    size_t cascadeAudited = 0;
    size_t cascadeAuditDisagreed = 0;
    std::optional<std::string> callLogPath;
    std::vector<SemanticCallRecord> calls;
};

}
