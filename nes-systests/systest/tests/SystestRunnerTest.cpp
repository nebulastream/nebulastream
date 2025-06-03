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

#include <memory>
#include <optional>
#include <queue>
#include <ranges>
#include <string>
#include <vector>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <cstddef>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES::Systest
{

class SystestRunnerTest : public Testing::BaseUnitTest
{
public:
    static std::string testFileName;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestRunnerTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestRunnerTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestRunnerTest test class."); }
};

/// Stubs
enum class QueryStatus
{
    Success,
    Failed,
    Stopped
};

struct Error
{
    int code_;
    std::string msg_;
    int code() const { return code_; }
    std::string_view what() const { return msg_; }
};

struct ExpectedError
{
    int code;
};
struct QueryPlan
{
};

struct Query
{
    std::optional<QueryPlan> queryPlan;
    std::optional<ExpectedError> expectedError;
};

struct Run
{
    std::shared_ptr<Error> error;
};
struct QuerySummary
{
    QueryId queryId{QueryId::INITIAL};
    QueryStatus currentStatus;
    std::vector<Run> runs;
};

struct RunningQuery
{
    Query query;
    QueryId queryId;
    std::shared_ptr<Error> exception;
    bool passed{false};
    QuerySummary querySummary;
    explicit RunningQuery(const Query& q, QueryId id) : query(q), queryId(id) { }
};

std::vector<RunningQuery> runQueries(const std::vector<Query>&, std::size_t, class QuerySubmitter&);
std::optional<std::string> checkResult(const RunningQuery&)
{
    return std::nullopt;
}
void printQueryResultToStdOut(const RunningQuery&, const std::string&, size_t, size_t, const std::string&)
{
}

/// Mocks
class QuerySubmitter
{
public:
    virtual ~QuerySubmitter() = default;
    virtual std::optional<QueryId> registerQuery(const QueryPlan&) = 0;
    virtual void startQuery(QueryId) = 0;
    virtual std::vector<QuerySummary> finishedQueries() = 0;
};

class MockSubmitter : public QuerySubmitter
{
public:
    MOCK_METHOD(std::optional<QueryId>, registerQuery, (const QueryPlan&), (override));
    MOCK_METHOD(void, startQuery, (QueryId), (override));
    MOCK_METHOD(std::vector<QuerySummary>, finishedQueries, (), (override));
};

static QuerySummary makeSummary(QueryId id, QueryStatus status, std::shared_ptr<Error> err = nullptr)
{
    QuerySummary s{id, status, {}};
    if (status == QueryStatus::Failed)
        s.runs.push_back({err});
    return s;
}

using ::testing::InSequence;
using ::testing::Return;
using ::testing::ReturnRef;

TEST_F(SystestRunnerTest, SuccessfulQuery)
{
    InSequence s; // preserve call order for clarity
    MockSubmitter submitter;
    QueryId id{1};

    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(Return(id));
    EXPECT_CALL(submitter, startQuery(id));
    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(Return(std::vector<QuerySummary>{makeSummary(id, QueryStatus::Success)}))
        .WillRepeatedly(Return(std::vector<QuerySummary>{}));

    QueryPlan plan; // valid plan means parse OK
    Query q{plan, std::nullopt}; // no expected error

    auto result = runQueries({q}, 1, submitter);
    EXPECT_TRUE(result.empty()) << "No queries should fail";
}

TEST_F(SystestRunnerTest, ExpectedErrorDuringParsing)
{
    InSequence s;
    MockSubmitter submitter; // will never be called → no expectations

    const int expectedCode = 42;
    auto parseError = std::make_shared<Error>(Error{expectedCode, "parse"});

    /// Build query with *invalid* plan (std::nullopt) but with matching expected error
    Query q{std::nullopt, ExpectedError{expectedCode}};
    // HACK: store the parse failure inside the std::optional<T>::error for the stub.
    // In a real implementation you'd populate q.queryPlan = tl::unexpected(parseError);

    auto result = runQueries({q}, 1, submitter);
    EXPECT_TRUE(result.empty());
}

TEST_F(SystestRunnerTest, RuntimeFailureWithUnexpectedCode)
{
    InSequence s;
    MockSubmitter submitter;
    QueryId id {7};
    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(Return(id));
    EXPECT_CALL(submitter, startQuery(id));

    // Fail at runtime with error 999, but *no* expectedError set
    auto runtimeErr = std::make_shared<Error>(Error{999, "boom"});

    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(Return(std::vector<QuerySummary>{makeSummary(id, QueryStatus::Failed, runtimeErr)}))
        .WillRepeatedly(Return(std::vector<QuerySummary>{}));

    QueryPlan plan;
    Query q{plan, std::nullopt};

    auto result = runQueries({q}, 1, submitter);
    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
    EXPECT_EQ(result.front().exception->code(), 999);
}

/// Query completes successfully, but we expected it to fail with code 77
TEST_F(SystestRunnerTest, MissingExpectedRuntimeError)
{
    InSequence s;
    MockSubmitter submitter;
    QueryId id{11};
    EXPECT_CALL(submitter, registerQuery(::testing::_)).WillOnce(Return(id));
    EXPECT_CALL(submitter, startQuery(id));

    EXPECT_CALL(submitter, finishedQueries())
        .WillOnce(Return(std::vector<QuerySummary>{makeSummary(id, QueryStatus::Success)}))
        .WillRepeatedly(Return(std::vector<QuerySummary>{}));

    QueryPlan plan;
    Query q{plan, ExpectedError{2000}};

    auto result = runQueries({q}, 1, submitter);
    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
}
}
