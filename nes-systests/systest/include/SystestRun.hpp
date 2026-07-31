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
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <SystestQueryModel.hpp>

namespace NES::Systest
{

struct IntentionalCaseSkip
{
    TestCaseId id;
    std::string reason;
};

struct TestSelection
{
    bool includeAll = true;
    std::vector<TestCaseId> cases;
    std::vector<IntentionalCaseSkip> intentionalSkips;

    [[nodiscard]] bool contains(const TestCaseId& id) const;
    [[nodiscard]] std::optional<std::string_view> skipReason(const TestCaseId& id) const;
};

enum class OrderingKind : uint8_t
{
    SourceOrder,
    Shuffled
};

struct OrderingPolicy
{
    OrderingKind kind = OrderingKind::SourceOrder;
    std::optional<uint64_t> seed;
};

struct ConcurrencyPolicy
{
    size_t maximumActiveCases = 1;
};

struct Once
{
};

struct FixedRepetitions
{
    size_t count = 1;
};

struct UntilCancelled
{
};

using RepetitionPolicy = std::variant<Once, FixedRepetitions, UntilCancelled>;

enum class IndependentFailurePolicy : uint8_t
{
    Continue,
    FailFast
};

struct DeadlinePolicy
{
    std::chrono::milliseconds caseTimeout = std::chrono::milliseconds::max();
    std::optional<std::chrono::milliseconds> runTimeout;
    std::chrono::milliseconds cancellationGracePeriod = std::chrono::seconds(5);
};

struct ValidationPolicy
{
    bool enabled = true;
};

struct MetricsPolicy
{
    bool collect = false;
    bool report = false;
};

struct RunSetup
{
    TestSelection selection;
    OrderingPolicy ordering;
    ConcurrencyPolicy concurrency;
    RepetitionPolicy repetition = Once{};
    IndependentFailurePolicy failurePolicy = IndependentFailurePolicy::Continue;
    DeadlinePolicy deadlines;
    ValidationPolicy validation;
    MetricsPolicy metrics;
};

struct RunSummary
{
    RunSetup setup;
    std::vector<ValidatedResult> results;
    size_t passed = 0;
    size_t failed = 0;
    size_t skipped = 0;
    std::chrono::milliseconds elapsed{};
    bool cancelled = false;
    std::vector<Diagnostic> diagnostics;
};

struct RunStarted
{
    RunSetup plan;
};

struct CaseFinished
{
    ValidatedResult result;
};

struct RunFinished
{
    RunSummary summary;
};

using RunEvent = std::variant<RunStarted, CaseFinished, RunFinished>;

struct ReportingDiagnostic
{
    std::string message;
};

class RunReporter
{
public:
    virtual ~RunReporter() = default;

    virtual std::expected<void, ReportingDiagnostic> publish(const RunEvent&) = 0;
};

}
