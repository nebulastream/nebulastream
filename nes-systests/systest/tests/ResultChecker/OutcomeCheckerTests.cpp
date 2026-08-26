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

#include <exception>
#include <expected>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/Verdict.hpp>
#include <ResultChecker/OutcomeChecker.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/UUID.hpp>
#include <BaseUnitTest.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>

namespace NES
{

namespace
{

/// One local query on one worker.
DistributedQueryStatusSnapshot createSnapshotIn(const QueryStatus state, const std::optional<Exception>& error)
{
    LocalQueryStatusSnapshot local;
    local.queryId = QueryId::createLocal(LocalQueryId{generateUUID()});
    local.state = state;
    local.metrics.error = error;
    DistributedQueryStatusSnapshot snapshot;
    snapshot.localStatusSnapshots[Host{"localhost:8080"}].emplace(local.queryId, local);
    return snapshot;
}

StatementOutcome createOutcomeOf(std::expected<DistributedQueryStatusSnapshot, Exception> reached)
{
    return StatementOutcome{.reached = std::move(reached), .sinkOutputSchema = std::nullopt, .explained = std::nullopt};
}

RewrittenTestCase createQueryExpecting(Expectation expectation)
{
    return RewrittenTestCase{
        .action = RewrittenQuery{
            .sql = "", .id = SystestQueryId{1}, .resultFile = std::nullopt, .inputFiles = {}, .expected = std::move(expectation)}};
}

StatementOutcome createExplainedOutcome(std::string plan)
{
    return StatementOutcome{
        .reached = std::unexpected{InvalidQuerySyntax("an EXPLAIN never reaches a worker")},
        .sinkOutputSchema = std::nullopt,
        .explained = std::move(plan)};
}

RewrittenTestCase createExplainExpecting(std::vector<std::string> lines)
{
    return RewrittenTestCase{
        .action = RewrittenExplain{.sql = "", .id = SystestQueryId{1}, .expected = ExpectedPlan{.lines = std::move(lines)}}};
}

}

class OutcomeCheckerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("OutcomeCheckerTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(OutcomeCheckerTest, ExpectedErrorMatchesByCodeWhereverItWasRaised)
{
    const auto expected = createQueryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});

    const std::vector beforeWorker{createOutcomeOf(std::unexpected{InvalidQuerySyntax("rejected")})};
    EXPECT_TRUE(checkTestCase(beforeWorker, expected, {}).has_value());

    const std::vector onWorker{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, InvalidQuerySyntax("rejected")))};
    EXPECT_TRUE(checkTestCase(onWorker, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, ExpectedErrorWithAnotherCodeIsAMismatch)
{
    const auto expected = createQueryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};

    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("expected the error"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, ExpectedErrorMessageHasToOccur)
{
    const auto expected = createQueryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = "unknown source"});
    const std::vector matching{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, InvalidQuerySyntax("unknown source stream")))};
    EXPECT_TRUE(checkTestCase(matching, expected, {}).has_value());

    const std::vector other{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, InvalidQuerySyntax("unknown sink")))};
    EXPECT_FALSE(checkTestCase(other, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, AQueryThatStopsAlthoughAnErrorWasExpectedIsAMismatch)
{
    const auto expected = createQueryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Stopped, std::nullopt))};

    EXPECT_FALSE(checkTestCase(outcomes, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, AnUnexpectedFailureReportsTheError)
{
    const auto expected = createQueryExpecting(ExpectedRows{});
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};

    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("boom"), std::string::npos);
}

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
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};

    const auto verdict = checkTestCase(outcomes, block, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("boom"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, OnlyAPassingVerdictCountsAsPassed)
{
    EXPECT_TRUE(hasPassed(CaseOutcome{Verdict{Success{}}}));
    EXPECT_FALSE(hasPassed(CaseOutcome{Verdict{std::unexpected{Mismatch{"boom"}}}}));
    EXPECT_FALSE(hasPassed(CaseOutcome{Skipped{.reason = "the file's setup failed"}}));
}

TEST_F(OutcomeCheckerTest, ExpectedRowsWithoutAResultFileAreAMismatch)
{
    const auto expected = createQueryExpecting(ExpectedRows{.rows = {"1"}});
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Stopped, std::nullopt))};
    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("writes no result file"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, NoExpectedRowsAndNoResultFilePass)
{
    const auto expected = createQueryExpecting(ExpectedRows{});
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Stopped, std::nullopt))};
    EXPECT_TRUE(checkTestCase(outcomes, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, AFailureWithoutAReportedErrorIsAMismatch)
{
    const auto expected = createQueryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Failed, std::nullopt))};
    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("without reporting an error"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, ExpectedErrorMatchesAmongSeveralReportedErrors)
{
    auto snapshot = createSnapshotIn(QueryStatus::Failed, QueryStatusFailed("collateral"));
    auto other = createSnapshotIn(QueryStatus::Failed, InvalidQuerySyntax("rejected"));
    snapshot.localStatusSnapshots.at(Host{"localhost:8080"}).merge(other.localStatusSnapshots.at(Host{"localhost:8080"}));

    const auto expected = createQueryExpecting(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt});
    const std::vector outcomes{createOutcomeOf(std::move(snapshot))};
    EXPECT_TRUE(checkTestCase(outcomes, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, ADifferentialBlockWithOnlyOneOutcomeIsAMismatch)
{
    const RewrittenTestCase block{
        .action = RewrittenDifferential{
            .firstSql = "",
            .firstId = SystestQueryId{1},
            .firstResultFile = "first.csv",
            .secondSql = "",
            .secondId = SystestQueryId{2},
            .secondResultFile = "second.csv"}};
    const std::vector outcomes{createOutcomeOf(createSnapshotIn(QueryStatus::Stopped, std::nullopt))};
    const auto verdict = checkTestCase(outcomes, block, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("second half"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, ADifferentialBlockWithAFailedSecondHalfReportsTheFailure)
{
    const RewrittenTestCase block{
        .action = RewrittenDifferential{
            .firstSql = "",
            .firstId = SystestQueryId{1},
            .firstResultFile = "first.csv",
            .secondSql = "",
            .secondId = SystestQueryId{2},
            .secondResultFile = "second.csv"}};
    const std::vector outcomes{
        createOutcomeOf(createSnapshotIn(QueryStatus::Stopped, std::nullopt)),
        createOutcomeOf(createSnapshotIn(QueryStatus::Failed, QueryStatusFailed("boom")))};
    const auto verdict = checkTestCase(outcomes, block, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("boom")) << verdict.error().detail;
}

TEST_F(OutcomeCheckerTest, ExplainRestoresThePrefixedNamesBeforeComparing)
{
    const auto expected = createExplainExpecting({"SINK(OUT)"});
    const OriginalNames names{{"TESTKEY_OUT", "OUT"}};
    const std::vector outcomes{createExplainedOutcome("SINK(TESTKEY_OUT)")};
    EXPECT_TRUE(checkTestCase(outcomes, expected, names).has_value());
    EXPECT_FALSE(checkTestCase(outcomes, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, ExplainWithAnotherPlanIsAMismatch)
{
    const auto expected = createExplainExpecting({"SINK(OTHER)"});
    const std::vector outcomes{createExplainedOutcome("SINK(OUT)")};
    EXPECT_FALSE(checkTestCase(outcomes, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, AnExplainThatDidNotBindReportsTheFailure)
{
    const auto expected = createExplainExpecting({"SINK(OUT)"});
    const std::vector outcomes{createOutcomeOf(std::unexpected{InvalidQuerySyntax("rejected")})};
    const auto verdict = checkTestCase(outcomes, expected, {});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("rejected"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, ExplainWithRegexTagsMatchesByPattern)
{
    const auto expected = createExplainExpecting({R"(<REGEX>SINK\(SINK[0-9]+\)</REGEX>)"});
    const std::vector outcomes{createExplainedOutcome("SINK(SINK42)")};
    EXPECT_TRUE(checkTestCase(outcomes, expected, {}).has_value());
}

TEST_F(OutcomeCheckerTest, AFailedFileEntryReportsTheActivityAndCountsAsFailed)
{
    const auto entry = createFailedFileEntry("errors/Broken", {}, "could not prepare", TestException("boom"));
    EXPECT_FALSE(entry.id.queryIdInFile.has_value());
    EXPECT_FALSE(hasPassed(entry.outcome));
    const auto& verdict = std::get<Verdict>(entry.outcome);
    ASSERT_FALSE(verdict.has_value());
    EXPECT_NE(verdict.error().detail.find("could not prepare: "), std::string::npos);
    EXPECT_NE(verdict.error().detail.find("boom"), std::string::npos);
}

TEST_F(OutcomeCheckerTest, AFailedFileEntryFallsBackToTheErrorCodeWhenTheMessageIsEmpty)
{
    try
    {
        throw Exception{"", ErrorCode::TestException};
    }
    catch (const std::exception& exception)
    {
        const auto entry = createFailedFileEntry("errors/Broken", {}, "could not prepare", exception);
        const auto& verdict = std::get<Verdict>(entry.outcome);
        ASSERT_FALSE(verdict.has_value());
        EXPECT_EQ(verdict.error().detail, fmt::format("could not prepare: {}", ErrorCode::TestException));
    }
}

}
