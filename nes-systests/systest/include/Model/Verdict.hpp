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
#include <utility>
#include <variant>
#include <vector>

#include <Model/TestCaseId.hpp>

namespace NES
{

/// Why a check failed, as text, describing how the actual output differs from the expected.
/// A struct rather than a bare string, so it can later carry a kind such as schema or result without touching call sites.
struct Mismatch
{
    std::string detail;
};

/// The outcome of one check: the empty value when it passed, and a mismatch describing the failure otherwise.
using Verdict = std::expected<void, Mismatch>;

/// A case that ran and returned what the test expects.
struct Passed
{
};

/// A case that never ran because something it needed failed, and the reason why.
/// Reported for observability rather than dropped.
struct Skipped
{
    std::string reason;
};

/// The reported state of one case: it passed, it failed with a mismatch, or it never ran.
using CaseOutcome = std::variant<Passed, Mismatch, Skipped>;

/// Transforms a checker's verdict into the reported state of a case.
inline CaseOutcome asOutcome(Verdict verdict)
{
    if (verdict.has_value())
    {
        return Passed{};
    }
    return std::move(verdict).error();
}

/// How long one statement took, measured two ways:
/// - The submission is the wall time this process waited, which includes planning and the queueing behind other queries.
/// - The execution is the span between the query starting and stopping.
struct QueryTiming
{
    std::chrono::steady_clock::duration submission{};
    std::chrono::milliseconds execution{};
};

/// One checked case, identified so a report line points back at the test file and query it came from.
struct CheckedQuery
{
    TestCaseId id;
    CaseOutcome outcome;
    /// One entry per statement the case submitted, in submission order, and empty when the case never ran.
    std::vector<QueryTiming> timings;
};

}
