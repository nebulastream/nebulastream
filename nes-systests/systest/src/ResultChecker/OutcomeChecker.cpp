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
#include <expected>
#include <span>
#include <string>
#include <variant>

#include <coordinator/lib.h>
#include <fmt/format.h>

#include <Model/Expectation.hpp>
#include <Model/RunnableTest.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/ResultChecker.hpp>
#include <Rewriter/NameQualifier.hpp>
#include <Util/Overloaded.hpp>

namespace NES
{
namespace
{

/// Checks a query the test expected to fail.
/// The query has to raise the expected error code, and a message containing the expected substring when the test gives one.
Verdict checkExpectedError(const StatementOutcome& outcome, const ExpectedError& expected)
{
    const auto expectedCode = static_cast<uint16_t>(expected.code);
    if (outcome.error.code == 0)
    {
        return std::unexpected(Mismatch{fmt::format("expected error {} but the query succeeded", expectedCode)});
    }
    if (outcome.error.code != expectedCode)
    {
        return std::unexpected(Mismatch{
            fmt::format("expected error {} but got error {}: {}", expectedCode, outcome.error.code, std::string{outcome.error.msg})});
    }
    if (const std::string message{outcome.error.msg}; expected.message.has_value() and message.find(*expected.message) == std::string::npos)
    {
        return std::unexpected(Mismatch{fmt::format("expected error message containing '{}' but got: {}", *expected.message, message)});
    }
    return {};
}

/// Checks the coordinator's answer to one query against the expectation, which is a different check per kind of expectation.
Verdict checkQuery(const StatementOutcome& outcome, const RunnableQuery& query)
{
    const auto& resultFile = query.resultFile;
    return std::visit(
        Overloaded{
            [&](const ExpectedRows& expectedRows) -> Verdict
            {
                if (outcome.error.code != 0)
                {
                    return std::unexpected(Mismatch{fmt::format("query failed: {}", std::string{outcome.error.msg})});
                }
                /// A query whose sink discards its input writes no file, so reaching a successful terminal state is the whole check.
                /// A test that still expects rows from such a sink asks for the impossible.
                if (not resultFile.has_value())
                {
                    if (not expectedRows.rows.empty())
                    {
                        return std::unexpected(Mismatch{fmt::format(
                            "expected {} result rows, but this query writes into a sink that discards them", expectedRows.rows.size())});
                    }
                    return {};
                }
                return Systest::checkResult(Systest::ResultCheck{
                    .resultFile = *resultFile, .expected = expectedRows.rows, .rowsAreJson = expectedRows.rowsAreJson});
            },
            [&](const ExpectedError& expectedError) -> Verdict { return checkExpectedError(outcome, expectedError); },
            [&](const ExpectedPlan& plan) -> Verdict
            {
                if (outcome.error.code != 0)
                {
                    return std::unexpected(Mismatch{fmt::format("explain failed: {}", std::string{outcome.error.msg})});
                }
                return Systest::checkExplain(Systest::ExplainCheck{
                    .actualOutput = unqualified(std::string{outcome.result}, plan.qualifyingPrefix), .expected = plan.lines});
            }},
        query.expectation);
}

/// Checks a differential block: both halves have to run without failing, and their result files have to agree.
/// The runner submits the second half only after the first succeeded, so a single outcome is a failed first half.
Verdict checkDifferentialCase(const std::span<const StatementOutcome> outcomes, const RunnableDifferential& differential)
{
    if (outcomes.front().error.code != 0)
    {
        return std::unexpected(Mismatch{fmt::format("first differential query failed: {}", std::string{outcomes.front().error.msg})});
    }
    if (outcomes.size() < 2)
    {
        return std::unexpected(Mismatch{"the second differential query was never submitted"});
    }
    if (outcomes.back().error.code != 0)
    {
        return std::unexpected(Mismatch{fmt::format("second differential query failed: {}", std::string{outcomes.back().error.msg})});
    }
    return Systest::checkDifferential(
        Systest::DifferentialCheck{.resultFile = differential.secondResultFile, .differentialResultFile = differential.firstResultFile});
}

}

Verdict checkCase(const std::span<const StatementOutcome> outcomes, const RunnableCase& testCase)
{
    return std::visit(
        Overloaded{
            [&](const RunnableQuery& query) { return checkQuery(outcomes.front(), query); },
            [&](const RunnableDifferential& differential) { return checkDifferentialCase(outcomes, differential); }},
        testCase.action);
}

}
