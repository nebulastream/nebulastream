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
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Model/Expectation.hpp>
#include <Model/RewrittenTest.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/Check.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/ResultChecker.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Overloaded.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{

/// Whether every local query of this statement stopped, which is the only state that leaves a complete result file.
bool stopped(const StatementOutcome& outcome)
{
    return outcome.reached.has_value() and outcome.reached->getGlobalQueryStatus() == DistributedQueryStatus::Stopped;
}

/// What went wrong with a statement, whether it failed before reaching the workers or on them.
/// The errors are flattened over the workers that reported one, because a test names the code it expects and not
/// where it came from.
struct Failure
{
    std::vector<Exception> errors;
    std::string description;
};

Failure failureOf(const StatementOutcome& outcome)
{
    if (not outcome.reached.has_value())
    {
        return Failure{.errors = {outcome.reached.error()}, .description = outcome.reached.error().what()};
    }

    Failure failure;
    for (const auto& exceptions : outcome.reached->getExceptions() | std::views::values)
    {
        failure.errors.insert(failure.errors.end(), exceptions.begin(), exceptions.end());
    }
    if (const auto coalesced = outcome.reached->coalesceException(); coalesced.has_value())
    {
        failure.description = coalesced->what();
    }
    return failure;
}

/// Checks a statement that failed against the error the test expects.
/// Errors beyond the expected one are tolerated, because a failure on one pipeline can raise further errors on the
/// pipelines connected to it.
Verdict checkFailed(const Failure& actual, const Expectation& expectation)
{
    if (actual.errors.empty())
    {
        return std::unexpected(Mismatch{"the query failed without reporting an error"});
    }

    const auto* expectedError = std::get_if<ExpectedError>(&expectation);
    if (expectedError == nullptr)
    {
        return std::unexpected(Mismatch{fmt::format("the query failed with an unexpected error: {}", actual.description)});
    }

    const auto occurred = std::ranges::any_of(actual.errors, [&](const Exception& error) { return error.code() == expectedError->code; });
    if (not occurred)
    {
        return std::unexpected(Mismatch{fmt::format(
            "expected the error \"{}({})\" to occur, but it did not. Actual: {}",
            expectedError->message,
            expectedError->code,
            actual.description)});
    }
    return {};
}

/// Checks the plan an EXPLAIN printed, which is computed while compiling because an EXPLAIN never reaches a worker.
/// The printed plan carries the qualified names, so the qualifying prefix comes off before the comparison and the plan
/// reads as the test wrote it.
Verdict checkExplained(const StatementOutcome& outcome, const ExpectedPlan& expected)
{
    if (not outcome.explained.has_value())
    {
        return checkFailed(failureOf(outcome), Expectation{expected});
    }
    return runCheck(ExplainCheck{.actualOutput = unqualified(*outcome.explained, expected.qualifyingPrefix), .expected = expected.lines});
}

/// Checks a query that stopped against the rows the test expects in its result file.
Verdict checkRows(const StatementOutcome& outcome, const RewrittenQuery& query, const ExpectedRows& expected)
{
    /// A query whose sink discards its input writes no file, so there is nothing to compare.
    if (not query.resultFile.has_value())
    {
        NES_INFO("Skipping the result check for {} because it writes no result file.", query.id);
        return {};
    }
    INVARIANT(outcome.sinkOutputSchema.has_value(), "a query that ran has a compiled plan and so a sink schema");
    return runCheck(ResultCheck{.resultFile = *query.resultFile, .expectedSchema = *outcome.sinkOutputSchema, .expected = expected.rows});
}

Verdict checkQuery(const StatementOutcome& outcome, const RewrittenQuery& query)
{
    if (const auto* expectedPlan = std::get_if<ExpectedPlan>(&query.expectation))
    {
        return checkExplained(outcome, *expectedPlan);
    }
    if (not stopped(outcome))
    {
        return checkFailed(failureOf(outcome), query.expectation);
    }
    if (const auto* expectedError = std::get_if<ExpectedError>(&query.expectation))
    {
        return std::unexpected(Mismatch{fmt::format("expected the error {} but the query succeeded", expectedError->code)});
    }
    const auto* expectedRows = std::get_if<ExpectedRows>(&query.expectation);
    INVARIANT(expectedRows != nullptr, "a query that is neither an EXPLAIN nor an expected error states its rows");
    return checkRows(outcome, query, *expectedRows);
}

/// Checks that the two halves of a differential block agree.
/// A half that did not stop leaves an incomplete result file, so the block reports that failure instead of comparing.
Verdict checkDifferential(const std::span<const StatementOutcome> outcomes, const RewrittenDifferential& block)
{
    for (const auto& outcome : outcomes)
    {
        if (not stopped(outcome))
        {
            return checkFailed(failureOf(outcome), Expectation{ExpectedRows{}});
        }
    }
    if (outcomes.size() < 2)
    {
        return std::unexpected(Mismatch{"the second half of the differential block never ran"});
    }
    return runCheck(DifferentialCheck{.resultFile = block.firstResultFile, .differentialResultFile = block.secondResultFile});
}

}

Verdict checkCase(const std::span<const StatementOutcome> outcomes, const RewrittenCase& testCase)
{
    INVARIANT(not outcomes.empty(), "a checked case submitted at least one statement");
    return std::visit(
        Overloaded{
            [&](const RewrittenQuery& query) { return checkQuery(outcomes.front(), query); },
            [&](const RewrittenDifferential& block) { return checkDifferential(outcomes, block); }},
        testCase.action);
}

}
