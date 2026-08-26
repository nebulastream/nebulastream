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

#include <expected>
#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>

namespace NES
{

namespace
{

/// A snapshot of one local query on one worker in the given state, with the error the worker reported, if any.
DistributedQueryStatusSnapshot snapshotIn(const QueryStatus state, const std::optional<Exception>& error)
{
    LocalQueryStatusSnapshot local;
    local.queryId = QueryId::createLocal(LocalQueryId(generateUUID()));
    local.state = state;
    local.metrics.error = error;
    DistributedQueryStatusSnapshot snapshot;
    snapshot.localStatusSnapshots[Host("localhost:8080")].emplace(local.queryId, local);
    return snapshot;
}

StatementOutcome outcomeOf(std::expected<DistributedQueryStatusSnapshot, Exception> reached)
{
    return StatementOutcome{.reached = std::move(reached), .sinkOutputSchema = std::nullopt, .explained = std::nullopt, .execution = {}};
}

RewrittenTestCase queryExpecting(Expectation expectation)
{
    return RewrittenTestCase{
        .action = RewrittenQuery{
            .sql = "", .id = SystestQueryId{1}, .resultFile = std::nullopt, .inputFiles = {}, .expectation = std::move(expectation)}};
}

}

class OutcomeCheckerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("OutcomeCheckerTest.log", LogLevel::LOG_DEBUG); }
};

/// The test states the code that it expects and not where the error came from, so an error on the worker matches as well
/// as one raised before the statement reached a worker.
TEST_F(OutcomeCheckerTest, ExpectedErrorMatchesByCodeWhereverItWasRaised)
{
    const auto expected = queryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});

    const std::vector beforeWorker{outcomeOf(std::unexpected{InvalidQuerySyntax("rejected")})};
    EXPECT_TRUE(checkTestCase(beforeWorker, expected, {}).has_value());

    const std::vector onWorker{outcomeOf(snapshotIn(QueryStatus::Failed, InvalidQuerySyntax("rejected")))};
    EXPECT_TRUE(checkTestCase(onWorker, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, ExpectedErrorWithAnotherCodeIsAMismatch)
{
    const auto expected = queryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});
    const std::vector outcomes{outcomeOf(snapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};

    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("expected the error"), std::string::npos);
}

/// The message narrows the match, so the same code with a different message does not pass.
TEST_F(OutcomeCheckerTest, ExpectedErrorMessageHasToOccur)
{
    const auto expected = queryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = "unknown source"});
    const std::vector matching{outcomeOf(snapshotIn(QueryStatus::Failed, InvalidQuerySyntax("unknown source stream")))};
    EXPECT_TRUE(checkTestCase(matching, expected, {}).has_value());

    const std::vector other{outcomeOf(snapshotIn(QueryStatus::Failed, InvalidQuerySyntax("unknown sink")))};
    EXPECT_FALSE(checkTestCase(other, expected, {}).has_value());
}

/// A query that was expected to fail but stopped normally is a mismatch, not a pass with nothing to compare.
TEST_F(OutcomeCheckerTest, AQueryThatStopsAlthoughAnErrorWasExpectedIsAMismatch)
{
    const auto expected = queryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});
    const std::vector outcomes{outcomeOf(snapshotIn(QueryStatus::Stopped, std::nullopt))};

    EXPECT_FALSE(checkTestCase(outcomes, expected, {}).has_value());
}

/// A failure where rows were expected reports the error, so the report says what went wrong rather than only that it did.
TEST_F(OutcomeCheckerTest, AnUnexpectedFailureReportsTheError)
{
    const auto expected = queryExpecting(ExpectedRows{});
    const std::vector outcomes{outcomeOf(snapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};

    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("boom"), std::string::npos);
}

/// The halves run one after the other, so a failed first half leaves the second unsubmitted and the block reports that failure.
TEST_F(OutcomeCheckerTest, ADifferentialBlockWithAFailedHalfReportsTheFailure)
{
    const RewrittenTestCase block{
        .action = RewrittenDifferential{
            .firstSql = "",
            .firstId = SystestQueryId{1},
            .firstResultFile = "first.csv",
            .secondSql = "",
            .secondId = SystestQueryId{2},
            .secondResultFile = "second.csv"}};
    const std::vector outcomes{outcomeOf(snapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};

    const auto verdict = checkTestCase(outcomes, block, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("boom"), std::string::npos);
}

}
