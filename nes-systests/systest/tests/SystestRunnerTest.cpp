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
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
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
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SystestParser.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

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

class MockSubmitter : public QuerySubmitter
{
public:
    MOCK_METHOD((std::expected<QueryId, Exception>), registerQuery, (const LogicalPlan&), (override));
    MOCK_METHOD(void, startQuery, (QueryId), (override));
    MOCK_METHOD(void, stopQuery, (QueryId), (override));
    MOCK_METHOD(void, unregisterQuery, (QueryId), (override));
    MOCK_METHOD(QuerySummary, waitForQueryTermination, (QueryId), (override));
    MOCK_METHOD(std::vector<QuerySummary>, finishedQueries, (), (override));
};

static QuerySummary makeSummary(QueryId id, QueryStatus status, const std::shared_ptr<Exception>& err = nullptr)
{
    QuerySummary s;
    s.queryId = id;
    s.currentStatus = status;
    if (status == QueryStatus::Failed && err)
    {
        QueryRunSummary run;
        run.error = *err;
        s.runs.push_back(run);
    }
    return s;
}

using ::testing::InSequence;
using ::testing::Return;

static SystestQuery makeQuery(const LogicalPlan& plan, std::optional<ExpectedError> expected = std::nullopt)
{
    return SystestQuery{
        "test_query", "SELECT * FROM test", TEST_DATA_DIR "filter.dummy", plan, 0, PATH_TO_BINARY_DIR, {}, {}, std::move(expected)};
}
/// Overload for parse‑time error
static SystestQuery createSystestQuery(const std::unexpected<Exception>& parseErr, const ExpectedError& expected)
{
    return SystestQuery{
        "test_query",
        "SELECT * FROM test",
        TEST_DATA_DIR "filter.dummy",
        parseErr, /// invalid plan
        0,
        PATH_TO_BINARY_DIR,
        {},
        {},
        expected};
}

TEST_F(SystestRunnerTest, ExpectedErrorDuringParsing)
{
    const InSequence seq;
    MockSubmitter submitter;

    constexpr ErrorCode expectedCode = ErrorCode::InvalidQuerySyntax;
    auto parseError = std::unexpected(Exception{"parse error", static_cast<uint64_t>(expectedCode)});

    auto dummyQueryResultMap = QueryResultMap{};
    const auto result = runQueries(
        {createSystestQuery(std::move(parseError), ExpectedError{.code = expectedCode, .message = std::nullopt})},
        1,
        submitter,
        dummyQueryResultMap);
    EXPECT_TRUE(result.empty()) << "query should pass because error was expected";
}

TEST_F(SystestRunnerTest, RuntimeFailureWithUnexpectedCode)
{
    const InSequence seq;
    MockSubmitter submitter;
    const QueryId id{7};

    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(submitter, startQuery(id));

    /// Runtime fails with unexpected error code 10000
    auto runtimeErr = std::make_shared<Exception>(Exception{"runtime boom", 10000});

    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(Return(std::vector{makeSummary(id, QueryStatus::Failed, runtimeErr)}))
        .WillRepeatedly(Return(std::vector<QuerySummary>{}));

    const LogicalPlan plan{};

    auto dummyQueryResultMap = QueryResultMap{};
    const auto result = runQueries({makeQuery(plan)}, 1, submitter, dummyQueryResultMap);

    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
    EXPECT_EQ(result.front().exception->code(), 10000);
}

TEST_F(SystestRunnerTest, MissingExpectedRuntimeError)
{
    const InSequence seq;
    MockSubmitter submitter;
    const QueryId id{11};

    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(submitter, startQuery(id));

    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(Return(std::vector{makeSummary(id, QueryStatus::Stopped)}))
        .WillRepeatedly(Return(std::vector<QuerySummary>{}));

    const LogicalPlan plan{};

    auto dummyQueryResultMap = QueryResultMap{};
    const auto result = runQueries(
        {makeQuery(plan, ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt})},
        1,
        submitter,
        dummyQueryResultMap);

    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
}
}
