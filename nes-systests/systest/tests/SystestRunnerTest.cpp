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

#include <SystestRunner.hpp>

#include <chrono>
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
#include <Identifiers/NESStrongType.hpp>
#include <Listeners/QueryLog.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SystestProgressTracker.hpp>
#include <SystestState.hpp>
#include <WorkerStatus.hpp>

namespace
{

/// NOLINTBEGIN(bugprone-unchecked-optional-access)

NES::LocalQueryStatus makeSummary(const NES::QueryId id, const NES::QueryState currState, const std::shared_ptr<NES::Exception>& err)
{
    NES::LocalQueryStatus queryStatus;
    queryStatus.queryId = id;
    queryStatus.state = currState;
    if (currState == NES::QueryState::Failed && err)
    {
        NES::QueryMetrics metrics;
        metrics.error = *err;
        queryStatus.metrics = metrics;
    }
    return queryStatus;
}

NES::Systest::SystestQuery makeQuery(
    const std::expected<NES::Systest::SystestQuery::PlanInfo, NES::Exception> planInfoOrException,
    std::variant<std::vector<std::string>, NES::Systest::ExpectedError> expected)
{
    return NES::Systest::SystestQuery{
        .testName = "test_query",
        .queryIdInFile = NES::INVALID<NES::Systest::SystestQueryId>,
        .testFilePath = SYSTEST_DATA_DIR "filter.dummy",
        .workingDir = NES::SystestConfiguration{}.workingDir.getValue(),
        .queryDefinition = "SELECT * FROM test",
        .planInfoOrException = planInfoOrException,
        .expectedResultsOrExpectedError = std::move(expected),
        .additionalSourceThreads = std::make_shared<std::vector<std::jthread>>(),
        .configurationOverride = NES::Systest::ConfigurationOverride{},
        .differentialQueryPlan = std::nullopt};
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

    SinkDescriptor dummySinkDescriptor
        = SinkCatalog{}.addSinkDescriptor("dummySink", Schema{}, "Print", {{"input_format", "CSV"}}, {}).value();
};

class MockQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    MOCK_METHOD((std::expected<QueryId, Exception>), registerQuery, (LogicalPlan), (override));
    MOCK_METHOD((std::expected<void, Exception>), start, (QueryId), (override));
    MOCK_METHOD((std::expected<void, Exception>), stop, (QueryId), (override));
    MOCK_METHOD((std::expected<void, Exception>), unregister, (QueryId), (override));
    MOCK_METHOD((std::expected<LocalQueryStatus, Exception>), status, (QueryId), (const, override));
    MOCK_METHOD((std::expected<WorkerStatus, Exception>), workerStatus, (std::chrono::system_clock::time_point), (const, override));
};

TEST_F(SystestRunnerTest, ExpectedErrorDuringParsing)
{
    const testing::InSequence seq;
    QuerySubmitter submitter{std::make_unique<QueryManager>(std::make_unique<MockQuerySubmissionBackend>())};
    SystestProgressTracker progressTracker;
    constexpr ErrorCode expectedCode = ErrorCode::InvalidQuerySyntax;
    const auto parseError = std::unexpected(Exception{"parse error", static_cast<uint64_t>(expectedCode)});

    const auto result = runQueries(
        {makeQuery(parseError, ExpectedError{.code = expectedCode, .message = std::nullopt})},
        1,
        submitter,
        progressTracker,
        discardPerformanceMessage);
    EXPECT_TRUE(result.empty()) << "query should pass because error was expected";
}

TEST_F(SystestRunnerTest, RuntimeFailureWithUnexpectedCode)
{
    const testing::InSequence seq;
    constexpr QueryId id{7};
    /// Runtime fails with unexpected error code 10000
    const auto runtimeErr = std::make_shared<Exception>(Exception{"runtime boom", 10000});
    auto mockBackend = std::make_unique<MockQuerySubmissionBackend>();
    EXPECT_CALL(*mockBackend, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(*mockBackend, start(id));
    EXPECT_CALL(*mockBackend, status(id))
        .WillOnce(testing::Return(makeSummary(id, QueryState::Registered, nullptr)))
        .WillOnce(testing::Return(makeSummary(id, QueryState::Started, nullptr)))
        .WillRepeatedly(testing::Return(makeSummary(id, QueryState::Failed, runtimeErr)));
    SystestProgressTracker progressTracker;

    QuerySubmitter submitter{std::make_unique<QueryManager>(std::move(mockBackend))};
    SourceCatalog sourceCatalog;
    auto testLogicalSource = sourceCatalog.addLogicalSource("testSource", Schema{});
    const std::unordered_map<std::string, std::string> parserConfig{{"type", "CSV"}};
    auto testPhysicalSource
        = sourceCatalog.addPhysicalSource(testLogicalSource.value(), "File", {{"file_path", "/dev/null"}}, parserConfig);
    auto sourceOperator = SourceDescriptorLogicalOperator{testPhysicalSource.value()};
    const LogicalPlan plan{SinkLogicalOperator{dummySinkDescriptor}.withChildren({sourceOperator})};

    const auto result
        = runQueries({makeQuery(SystestQuery::PlanInfo{plan, Schema{}}, {})}, 1, submitter, progressTracker, discardPerformanceMessage);

    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
    EXPECT_EQ(result.front().exception->code(), 10000);
}

TEST_F(SystestRunnerTest, MissingExpectedRuntimeError)
{
    const testing::InSequence seq;
    constexpr QueryId id{11};

    auto mockBackend = std::make_unique<MockQuerySubmissionBackend>();
    EXPECT_CALL(*mockBackend, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(*mockBackend, start(id));
    EXPECT_CALL(*mockBackend, status(id))
        .WillOnce(testing::Return(makeSummary(id, QueryState::Registered, nullptr)))
        .WillOnce(testing::Return(makeSummary(id, QueryState::Running, nullptr)))
        .WillRepeatedly(testing::Return(makeSummary(id, QueryState::Stopped, nullptr)));
    SystestProgressTracker progressTracker;

    QuerySubmitter submitter{std::make_unique<QueryManager>(std::move(mockBackend))};
    SourceCatalog sourceCatalog;
    auto testLogicalSource = sourceCatalog.addLogicalSource("testSource", Schema{});
    const std::unordered_map<std::string, std::string> parserConfig{{"type", "CSV"}};
    auto testPhysicalSource
        = sourceCatalog.addPhysicalSource(testLogicalSource.value(), "File", {{"file_path", "/dev/null"}}, parserConfig);
    auto sourceOperator = SourceDescriptorLogicalOperator{testPhysicalSource.value()};
    const LogicalPlan plan{SinkLogicalOperator{dummySinkDescriptor}.withChildren({sourceOperator})};

    const auto result = runQueries(
        {makeQuery(SystestQuery::PlanInfo{plan, Schema{}}, ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt})},
        1,
        submitter,
        progressTracker,
        discardPerformanceMessage);

    ASSERT_EQ(result.size(), 1);
    EXPECT_FALSE(result.front().passed);
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
