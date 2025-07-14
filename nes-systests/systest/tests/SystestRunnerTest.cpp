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

#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Identifiers/NESStrongType.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SystestParser.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

namespace
{

NES::QuerySummary makeSummary(const NES::QueryId id, const NES::QueryStatus status, const std::shared_ptr<NES::Exception>& err)
{
    NES::QuerySummary querySummary;
    querySummary.queryId = id;
    querySummary.currentStatus = status;
    if (status == NES::QueryStatus::Failed && err)
    {
        NES::QueryRunSummary run;
        run.error = *err;
        querySummary.runs.push_back(run);
    }
    return querySummary;
}

NES::Systest::SystestQuery makeQuery(
    const std::expected<NES::Systest::SystestQuery::PlanInfo, NES::Exception> planInfoOrException,
    std::variant<std::vector<std::string>, NES::Systest::ExpectedError> expected)
{
    return NES::Systest::SystestQuery{
        "test_query",
        NES::INVALID<NES::Systest::SystestQueryId>,
        SYSTEST_DATA_DIR "filter.dummy",
        NES::Configuration::SystestConfiguration{}.workingDir.getValue(),
        "SELECT * FROM test",
        planInfoOrException,
        std::move(expected),
        std::make_shared<std::vector<std::jthread>>()};
}
}

namespace NES::Systest
{

class SystestRunnerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestRunnerTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestRunnerTest test class.");
    }
    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestRunnerTest test class."); }
};

class MockSubmitter final : public QuerySubmitter
{
public:
    MOCK_METHOD((std::expected<QueryId, Exception>), registerQuery, (const LogicalPlan&), (override));
    MOCK_METHOD(void, startQuery, (QueryId), (override));
    MOCK_METHOD(void, stopQuery, (QueryId), (override));
    MOCK_METHOD(void, unregisterQuery, (QueryId), (override));
    MOCK_METHOD(QuerySummary, waitForQueryTermination, (QueryId), (override));
    MOCK_METHOD(std::vector<QuerySummary>, finishedQueries, (), (override));
};

TEST_F(SystestRunnerTest, ExpectedErrorDuringParsing)
{
    const testing::InSequence seq;
    MockSubmitter submitter;

    constexpr ErrorCode expectedCode = ErrorCode::InvalidQuerySyntax;
    auto parseError = std::unexpected(Exception{"parse error", static_cast<uint64_t>(expectedCode)});

    const auto result = runQueries({makeQuery(parseError, ExpectedError{.code = expectedCode, .message = std::nullopt})}, 1, submitter);
    EXPECT_TRUE(result.empty()) << "query should pass because error was expected";
}

TEST_F(SystestRunnerTest, RuntimeFailureWithUnexpectedCode)
{
    const testing::InSequence seq;
    MockSubmitter submitter;
    constexpr QueryId id{7};

    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(submitter, startQuery(id));

    /// Runtime fails with unexpected error code 10000
    const auto runtimeErr = std::make_shared<Exception>(Exception{"runtime boom", 10000});

    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(testing::Return(std::vector{makeSummary(id, QueryStatus::Failed, runtimeErr)}))
        .WillRepeatedly(testing::Return(std::vector<QuerySummary>{}));

    const LogicalPlan plan{};

    const auto result = runQueries(
        {makeQuery(SystestQuery::PlanInfo{.queryPlan = plan, .sourcesToFilePathsAndCounts = {}, .sinkOutputSchema = Schema{}}, {})},
        1,
        submitter);

    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
    EXPECT_EQ(result.front().exception->code(), 10000);
}

TEST_F(SystestRunnerTest, MissingExpectedRuntimeError)
{
    const testing::InSequence seq;
    MockSubmitter submitter;
    constexpr QueryId id{11};

    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(submitter, startQuery(id));

    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(testing::Return(std::vector{makeSummary(id, QueryStatus::Stopped, nullptr)}))
        .WillRepeatedly(testing::Return(std::vector<QuerySummary>{}));

    const LogicalPlan plan{};

    const auto result = runQueries(
        {makeQuery(
            SystestQuery::PlanInfo{.queryPlan = plan, .sourcesToFilePathsAndCounts = {}, .sinkOutputSchema = Schema{}},
            ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt})},
        1,
        submitter);

    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
}
}
