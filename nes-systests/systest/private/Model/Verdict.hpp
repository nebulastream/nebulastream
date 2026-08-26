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
#include <expected>
#include <string>
#include <variant>
#include <vector>

#include <Model/TestCaseId.hpp>

namespace NES
{

/// Why a check failed, as text describing how the actual output differs from the expected one.
/// A struct, so it can later store more structured mismatch information without touching call sites.
struct Mismatch
{
    std::string detail;
};

struct Success
{
};

/// The outcome of one check.
using Verdict = std::expected<Success, Mismatch>;

/// A test case that never ran, e.g., because a prerequisite failed.
struct Skipped
{
    std::string reason;
};

/// Reported state of one test case: a check's verdict, or a skip when no check ran.
using CaseOutcome = std::variant<Verdict, Skipped>;

/// Only a verdict in the test case's favor counts as passed; a skip did not pass either.
[[nodiscard]] inline bool hasPassed(const CaseOutcome& outcome)
{
    const auto* verdict = std::get_if<Verdict>(&outcome);
    return verdict != nullptr and verdict->has_value();
}

/// How long one statement took.
struct QueryTiming
{
    /// Total wall clock time elapsed, including planning and queueing behind other queries.
    std::chrono::steady_clock::duration submission{};
    /// Span between the query running and stopping.
    std::chrono::milliseconds execution{};
};

/// One line of the report: a test case's verdict, a skipped test case, or a file that failed before it had test cases.
struct ReportEntry
{
    TestCaseId id;
    CaseOutcome outcome;
    /// One entry per submitted statement, in submission order.
    /// Empty when the test case never ran.
    std::vector<QueryTiming> timings;
};

}
