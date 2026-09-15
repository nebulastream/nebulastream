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
#include <cstdint>
#include <expected>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <utility>
#include <variant>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <BridgeError.hpp>

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
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

bool hasSucceeded(const Bridge::StatementOutcome& outcome)
{
    return Bridge::isOk(outcome.error);
}

/// Only a completed query leaves a complete result file.
bool hasCompleted(const Bridge::StatementOutcome& outcome)
{
    return hasSucceeded(outcome) and outcome.state == Bridge::QueryState::Completed;
}

Verdict checkFailed(const Bridge::StatementOutcome& outcome, const Expectation& expectation)
{
    if (outcome.errors.empty())
    {
        return std::unexpected{Mismatch{"the query did not complete and reported no error"}};
    }
    const auto description = fmt::format(
        "{}",
        fmt::join(
            outcome.errors
                | std::views::transform([](const Bridge::BridgeError& error)
                                        { return fmt::format("{}({})", std::string{error.msg}, error.code); }),
            "; "));
    const auto* expectedError = std::get_if<ExpectedError>(&expectation);
    if (expectedError == nullptr)
    {
        return std::unexpected{Mismatch{fmt::format("the query failed with an unexpected error: {}", description)}};
    }

    const auto expectedCode = static_cast<uint16_t>(expectedError->code);
    const auto occurred = std::ranges::any_of(
        outcome.errors,
        [&](const Bridge::BridgeError& error)
        {
            return error.code == expectedCode
                and (not expectedError->message.has_value() or std::string{error.msg}.contains(*expectedError->message));
        });
    if (not occurred)
    {
        return std::unexpected{Mismatch{fmt::format(
            "expected the error \"{}({})\" to occur, but it did not. Actual: {}",
            expectedError->message.value_or(""),
            expectedCode,
            description)}};
    }
    return Success{};
}

/// Restores prefixed names first, e.g., `FILTER_STREAM` back to `stream`.
Verdict checkExplain(const Bridge::StatementOutcome& outcome, const ExpectedPlan& expected, const OriginalNames& originalNames)
{
    if (not hasSucceeded(outcome))
    {
        return checkFailed(outcome, Expectation{expected});
    }
    auto actual = restoreNames(std::string{outcome.result}, originalNames);
    if (hasExplainRegexTags(expected.lines))
    {
        return runCheck(ExplainRegexCheck{.expected = expected.lines, .actual = std::move(actual)});
    }
    return runCheck(ExplainLinesCheck{.expected = expected.lines, .actual = std::move(actual)});
}

Verdict checkRows(const RewrittenQuery& query, const ExpectedRows& expected)
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
    return runCheck(
        QueryResultCheck{.resultFile = *query.resultFile, .expectedSchema = query.declaredSinkSchema, .expectedTuples = expected.rows});
}

Verdict checkQuery(const Bridge::StatementOutcome& outcome, const RewrittenQuery& query)
{
    if (not hasCompleted(outcome))
    {
        return checkFailed(outcome, query.expected);
    }
    if (const auto* expectedError = std::get_if<ExpectedError>(&query.expected))
    {
        return std::unexpected{Mismatch{fmt::format("expected the error {} but the query succeeded", expectedError->code)}};
    }
    const auto* expectedRows = std::get_if<ExpectedRows>(&query.expected);
    INVARIANT(expectedRows != nullptr, "a query that is neither an EXPLAIN nor an expected error states its rows");
    return checkRows(query, *expectedRows);
}

/// A half that did not complete leaves an incomplete result file, so the block reports that failure instead of comparing.
Verdict checkDifferential(const std::span<const Bridge::StatementOutcome> outcomes, const RewrittenDifferential& block)
{
    if (const auto failedHalf = std::ranges::find_if_not(outcomes, hasCompleted); failedHalf != outcomes.end())
    {
        return checkFailed(*failedHalf, Expectation{ExpectedRows{}});
    }
    if (outcomes.size() < 2)
    {
        return std::unexpected{Mismatch{"the second half of the differential block never ran"}};
    }
    return runCheck(DifferentialCheck{.firstResultFile = block.firstResultFile, .secondResultFile = block.secondResultFile});
}

}

Verdict checkTestCase(
    const std::span<const Bridge::StatementOutcome> outcomes, const RewrittenTestCase& testCase, const OriginalNames& originalNames)
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
