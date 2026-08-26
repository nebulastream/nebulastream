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

#include <ResultChecker/OutcomeChecker.hpp>

#include <algorithm>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/Check.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/QueryResultChecker.hpp>
#include <Rewriter/NamePrefixer.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Only a stopped query leaves a complete result file.
bool hasStopped(const StatementOutcome& outcome)
{
    return outcome.reached.has_value() and outcome.reached->getGlobalQueryStatus() == DistributedQueryStatus::Stopped;
}

/// Flattened over workers: a test states the expected code, not where it arose.
struct Failure
{
    std::vector<Exception> errors;
    std::string description;
};

Failure extractFailureOf(const StatementOutcome& outcome)
{
    if (not outcome.reached.has_value())
    {
        return Failure{.errors = {outcome.reached.error()}, .description = outcome.reached.error().what()};
    }

    const auto coalesced = outcome.reached->coalesceException();
    return Failure{
        .errors = outcome.reached->getExceptions() | std::views::values | std::views::join | std::ranges::to<std::vector>(),
        .description = coalesced.has_value() ? coalesced->what() : ""};
}

/// Ignores extra errors: a failing pipeline also fails the pipelines connected to it.
Verdict checkFailed(const Failure& actual, const Expectation& expectation)
{
    if (actual.errors.empty())
    {
        return std::unexpected{Mismatch{"the query failed without reporting an error"}};
    }

    const auto* expectedError = std::get_if<ExpectedError>(&expectation);
    if (expectedError == nullptr)
    {
        return std::unexpected{Mismatch{fmt::format("the query failed with an unexpected error: {}", actual.description)}};
    }

    const auto occurred = std::ranges::any_of(
        actual.errors,
        [&](const Exception& error)
        {
            return error.code() == expectedError->code
                and (not expectedError->message.has_value()
                     or std::string_view{error.what()}.find(*expectedError->message) != std::string_view::npos);
        });
    if (not occurred)
    {
        return std::unexpected{Mismatch{fmt::format(
            "expected the error \"{}({})\" to occur, but it did not. Actual: {}",
            expectedError->message.value_or(""),
            expectedError->code,
            actual.description)}};
    }
    return Success{};
}

/// Restores prefixed names first, e.g., `FILTER_STREAM` back to `stream`.
Verdict checkExplain(const StatementOutcome& outcome, const ExpectedPlan& expected, const OriginalNames& originalNames)
{
    if (not outcome.explained.has_value())
    {
        return checkFailed(extractFailureOf(outcome), Expectation{expected});
    }
    auto actual = restoreNames(*outcome.explained, originalNames);
    if (hasExplainRegexTags(expected.lines))
    {
        return runCheck(ExplainRegexCheck{.expected = expected.lines, .actual = std::move(actual)});
    }
    return runCheck(ExplainLinesCheck{.expected = expected.lines, .actual = std::move(actual)});
}

Verdict checkRows(const StatementOutcome& outcome, const RewrittenQuery& query, const ExpectedRows& expected)
{
    if (not query.resultFile.has_value())
    {
        if (not expected.rows.empty())
        {
            return std::unexpected{Mismatch{"the test expects rows, but the query writes no result file to compare them with"}};
        }
        NES_INFO("Skipping the result check for {} because it writes no result file.", query.id);
        return Success{};
    }
    INVARIANT(outcome.sinkOutputSchema.has_value(), "a query that ran has a bound plan and so a sink schema");
    return runCheck(
        QueryResultCheck{.resultFile = *query.resultFile, .expectedSchema = *outcome.sinkOutputSchema, .expectedTuples = expected.rows});
}

Verdict checkQuery(const StatementOutcome& outcome, const RewrittenQuery& query)
{
    if (not hasStopped(outcome))
    {
        return checkFailed(extractFailureOf(outcome), query.expected);
    }
    if (const auto* expectedError = std::get_if<ExpectedError>(&query.expected))
    {
        return std::unexpected{Mismatch{fmt::format("expected the error {} but the query succeeded", expectedError->code)}};
    }
    const auto* expectedRows = std::get_if<ExpectedRows>(&query.expected);
    INVARIANT(expectedRows != nullptr, "a query that is neither an EXPLAIN nor an expected error states its rows");
    return checkRows(outcome, query, *expectedRows);
}

/// A half that did not stop leaves an incomplete result file, so the block reports that failure instead of comparing.
Verdict checkDifferential(const std::span<const StatementOutcome> outcomes, const RewrittenDifferential& block)
{
    if (const auto failedHalf = std::ranges::find_if_not(outcomes, hasStopped); failedHalf != outcomes.end())
    {
        return checkFailed(extractFailureOf(*failedHalf), Expectation{ExpectedRows{}});
    }
    if (outcomes.size() < 2)
    {
        return std::unexpected{Mismatch{"the second half of the differential block never ran"}};
    }
    return runCheck(DifferentialCheck{.firstResultFile = block.firstResultFile, .secondResultFile = block.secondResultFile});
}

}

Verdict
checkTestCase(const std::span<const StatementOutcome> outcomes, const RewrittenTestCase& testCase, const OriginalNames& originalNames)
{
    INVARIANT(not outcomes.empty(), "a checked test case submitted at least one statement");
    return std::visit(
        Overloaded{
            [&](const RewrittenQuery& query) { return checkQuery(outcomes.front(), query); },
            [&](const RewrittenDifferential& block) { return checkDifferential(outcomes, block); },
            [&](const RewrittenExplain& explain) { return checkExplain(outcomes.front(), explain.expected, originalNames); }},
        testCase.action);
}

}
