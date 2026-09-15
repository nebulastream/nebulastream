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

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <variant>

#include <fmt/format.h>
#include <nes-coordinator-bridge/coordinator.h>
#include <BridgeError.hpp>

#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/Check.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/QueryResultChecker.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Checks a statement that failed against the error the test expects.
/// The coordinator reports one code for the statement: the planner's when planning failed, and the failed fragment's
/// otherwise, so a test states the code that it expects and not where it came from.
Verdict checkFailed(const Bridge::StatementOutcome& outcome, const Expectation& expectation)
{
    const std::string message{outcome.error.msg};
    const auto* expectedError = std::get_if<ExpectedError>(&expectation);
    if (expectedError == nullptr)
    {
        return std::unexpected(Mismatch{fmt::format("the query failed with an unexpected error: {}", message)});
    }

    const auto expectedCode = static_cast<uint16_t>(expectedError->code);
    if (outcome.error.code != expectedCode
        or (expectedError->message.has_value() and message.find(*expectedError->message) == std::string::npos))
    {
        return std::unexpected(Mismatch{fmt::format(
            "expected the error \"{}({})\" to occur, but it did not. Actual: {}({})",
            expectedError->message.value_or(""),
            expectedCode,
            message,
            outcome.error.code)});
    }
    return Success{};
}

/// Checks the plan that an EXPLAIN printed, which the coordinator answers with instead of a result file.
/// The printed plan uses the qualified names, so the qualifying prefix comes off before the comparison and the plan
/// reads as the test wrote it.
Verdict checkExplained(const Bridge::StatementOutcome& outcome, const ExpectedPlan& expected, const std::string_view qualifyingPrefix)
{
    if (not Bridge::isNone(outcome.error))
    {
        return checkFailed(outcome, Expectation{expected});
    }
    auto actual = unqualified(std::string{outcome.result}, qualifyingPrefix);
    if (hasExplainRegexTags(expected.lines))
    {
        return runCheck(ExplainRegexCheck{.expected = expected.lines, .actual = std::move(actual)});
    }
    return runCheck(ExplainLinesCheck{.expected = expected.lines, .actual = std::move(actual)});
}

/// Checks a query that stopped against the rows that the test expects in its result file.
/// The coordinator holds the schema the sink was planned with, so the check reads it back from the file's header.
Verdict checkRows(const RewrittenQuery& query, const ExpectedRows& expected)
{
    /// A query whose sink discards its input writes no file, so there is nothing to compare.
    if (not query.resultFile.has_value())
    {
        if (not expected.rows.empty())
        {
            return std::unexpected(Mismatch{"the test expects rows, but the query writes no result file to compare them against"});
        }
        NES_INFO("Skipping the result check for {} because it writes no result file.", query.id);
        return Success{};
    }
    return runCheck(QueryResultCheck{.resultFile = *query.resultFile, .expectedSchema = std::nullopt, .expectedTuples = expected.rows});
}

Verdict checkQuery(const Bridge::StatementOutcome& outcome, const RewrittenQuery& query)
{
    if (not Bridge::isNone(outcome.error))
    {
        return checkFailed(outcome, query.expectation);
    }
    if (const auto* expectedError = std::get_if<ExpectedError>(&query.expectation))
    {
        return std::unexpected(Mismatch{fmt::format("expected the error {} but the query succeeded", expectedError->code)});
    }
    const auto* expectedRows = std::get_if<ExpectedRows>(&query.expectation);
    INVARIANT(expectedRows != nullptr, "a query that is neither an EXPLAIN nor an expected error states its rows");
    return checkRows(query, *expectedRows);
}

/// Checks that the two halves of a differential block agree.
/// A half that failed leaves an incomplete result file, so the block reports that failure instead of comparing.
Verdict checkDifferential(const std::span<const Bridge::StatementOutcome> outcomes, const RewrittenDifferential& block)
{
    for (const auto& outcome : outcomes)
    {
        if (not Bridge::isNone(outcome.error))
        {
            return checkFailed(outcome, Expectation{ExpectedRows{}});
        }
    }
    if (outcomes.size() < 2)
    {
        return std::unexpected(Mismatch{"the second half of the differential block never ran"});
    }
    return runCheck(DifferentialCheck{.firstResultFile = block.firstResultFile, .secondResultFile = block.secondResultFile});
}

}

Verdict
checkCase(const std::span<const Bridge::StatementOutcome> outcomes, const RewrittenCase& testCase, const std::string_view qualifyingPrefix)
{
    INVARIANT(not outcomes.empty(), "a checked case submitted at least one statement");
    return std::visit(
        Overloaded{
            [&](const RewrittenQuery& query) { return checkQuery(outcomes.front(), query); },
            [&](const RewrittenDifferential& block) { return checkDifferential(outcomes, block); },
            [&](const RewrittenExplain& explain) { return checkExplained(outcomes.front(), explain.expected, qualifyingPrefix); }},
        testCase.action);
}

}
