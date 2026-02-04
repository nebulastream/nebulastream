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
#include <filesystem>
#include <memory>
#include <optional>
#include <random>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <fmt/format.h>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Identifiers/NESStrongType.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SystestBinder.hpp>
#include <SystestConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestRunner.hpp>
#include <SystestState.hpp>

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
        .differentialQueryPlan = std::nullopt,
        .inlineEventScripts = {},
        .inlineEventControllers = {}};
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

    SinkDescriptor dummySinkDescriptor = SinkCatalog{}.addSinkDescriptor("dummySink", Schema{}, "Print", {{"input_format", "CSV"}}).value();
};

class MockQueryManager final : public QueryManager
{
public:
    MOCK_METHOD((std::expected<QueryId, Exception>), registerQuery, (const LogicalPlan&), (override));
    MOCK_METHOD((std::expected<void, Exception>), start, (QueryId), (noexcept, override));
    MOCK_METHOD((std::expected<void, Exception>), stop, (QueryId), (noexcept, override));
    MOCK_METHOD((std::expected<void, Exception>), unregister, (QueryId), (noexcept, override));
    MOCK_METHOD((std::expected<LocalQueryStatus, Exception>), status, (QueryId), (const, noexcept, override));
};

TEST_F(SystestRunnerTest, ExpectedErrorDuringParsing)
{
    const testing::InSequence seq;
    QuerySubmitter submitter{std::make_unique<MockQueryManager>()};
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

    auto mockManager = std::make_unique<MockQueryManager>();
    EXPECT_CALL(*mockManager, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(*mockManager, start(id));
    EXPECT_CALL(*mockManager, status(id))
        .WillOnce(testing::Return(makeSummary(id, QueryState::Failed, runtimeErr)))
        .WillRepeatedly(testing::Return(LocalQueryStatus{}));
    SystestProgressTracker progressTracker;

    QuerySubmitter submitter{std::move(mockManager)};
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

    auto mockManager = std::make_unique<MockQueryManager>();
    EXPECT_CALL(*mockManager, registerQuery(::testing::_)).WillOnce(testing::Return(std::expected<QueryId, Exception>{id}));
    EXPECT_CALL(*mockManager, start(id));
    EXPECT_CALL(*mockManager, status(id))
        .WillOnce(testing::Return(makeSummary(id, QueryState::Stopped, nullptr)))
        .WillRepeatedly(testing::Return(LocalQueryStatus{}));
    SystestProgressTracker progressTracker;

    QuerySubmitter submitter{std::move(mockManager)};
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

namespace
{
std::vector<SystestQuery> loadInlineEventQueries(const std::string& testFilePath, const std::filesystem::path& workingDir)
{
    SystestConfiguration config{};
    config.directlySpecifiedTestFiles.setValue(testFilePath);
    config.workingDir.setValue(workingDir.string());
    config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);

    std::filesystem::remove_all(workingDir);
    std::filesystem::create_directories(workingDir);

    const auto testFileMap = loadTestFileMap(config);
    SystestBinder binder{workingDir.string(), config.testDataDir.getValue(), config.configDir.getValue()};
    auto [queries, loaded] = binder.loadOptimizeQueries(testFileMap);
    EXPECT_EQ(loaded, testFileMap.size());
    return queries;
}
}

TEST_F(SystestRunnerTest, RandomizedInlineAndRegularQueriesDoNotInterfere)
{
    const auto testFilePath = (std::filesystem::path{SYSTEST_DATA_DIR} / "InlineEventMixed.dummy").string();
    std::mt19937 rng(42);

    for (int iteration = 0; iteration < 5; ++iteration)
    {
        const auto workingDir
            = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests" / "tests" / fmt::format("InlineEventRun{}", iteration);
        auto queries = loadInlineEventQueries(testFilePath, workingDir);
        ASSERT_FALSE(queries.empty());

        std::ranges::shuffle(queries, rng);

        std::vector<SystestQuery> restartableQueries;
        std::vector<SystestQuery> regularQueries;
        for (auto& query : queries)
        {
            if (hasInlineEventWorkerRestart(query))
            {
                restartableQueries.push_back(query);
            }
            else
            {
                regularQueries.push_back(query);
            }
        }

        SystestProgressTracker tracker;
        tracker.setTotalQueries(queries.size());
        SingleNodeWorkerConfiguration configuration;

        if (!regularQueries.empty())
        {
            const auto failedRegular = runQueriesAtLocalWorker(regularQueries, 1, configuration, tracker);
            EXPECT_TRUE(failedRegular.empty());
        }

        if (!restartableQueries.empty())
        {
            const auto failedInline = runInlineEventQueriesWithWorkerRestart(restartableQueries, configuration, tracker);
            EXPECT_TRUE(failedInline.empty());
        }
    }
}

namespace
{
struct GeneratedInlineScript
{
    std::vector<std::string> scriptLines;
    std::vector<std::string> expectedTuples;
};

GeneratedInlineScript generateCrashRestartScript(std::mt19937& rng, uint64_t& tupleIdCounter)
{
    GeneratedInlineScript result;

    /// Ensure at least one crash/restart.
    const auto segments = std::uniform_int_distribution<int>(2, 4)(rng);
    uint64_t currentTs = 0;
    for (int segment = 0; segment < segments; ++segment)
    {
        const auto tuplesInSegment = std::uniform_int_distribution<int>(1, 3)(rng);
        for (int i = 0; i < tuplesInSegment; ++i)
        {
            const uint64_t val = ++tupleIdCounter;
            const uint64_t ts = currentTs;
            currentTs += std::uniform_int_distribution<int>(10, 40)(rng);
            const auto line = fmt::format("{},{}", val, ts);
            result.scriptLines.push_back(line);
            if (segment == segments - 1)
            {
                result.expectedTuples.push_back(line);
            }
        }
        if (segment < segments - 1)
        {
            result.scriptLines.emplace_back("<CRASH>");
            result.scriptLines.emplace_back("<RESTART>");
        }
    }
    return result;
}

std::filesystem::path writeRandomInlineTestFile(const std::filesystem::path& dir, const GeneratedInlineScript& script, int iteration)
{
    const auto filePath = dir / fmt::format("RandomInlineCrash_{}.test", iteration);
    std::ofstream out(filePath);
    out << "# name: random/inline/RandomInlineCrash_" << iteration << "\n";
    out << "# description: Randomized inline crash/restart script\n";
    out << "CREATE LOGICAL SOURCE stream(i64 INT64, ts UINT64);\n";
    out << "CREATE PHYSICAL SOURCE FOR stream TYPE TCP;\n";
    out << "ATTACH INLINE\n";
    for (const auto& line : script.scriptLines)
    {
        out << line << "\n";
    }
    out << "\n";
    out << "CREATE SINK sink(stream.i64 INT64, stream.ts UINT64) TYPE File;\n";
    out << "SELECT i64, ts FROM stream INTO sink;\n";
    out << "----\n";
    for (const auto& line : script.expectedTuples)
    {
        out << line << "\n";
    }
    return filePath;
}
}

TEST_F(SystestRunnerTest, RandomizedInlineCrashScriptsTerminateAndProduceExpectedTuples)
{
    std::mt19937 rng(1337);
    uint64_t tupleIdCounter = 0;

    const auto baseWorkingDir = std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-systests" / "tests" / "RandomInlineCrash";
    std::filesystem::remove_all(baseWorkingDir);
    std::filesystem::create_directories(baseWorkingDir);

    for (int iteration = 0; iteration < 5; ++iteration)
    {
        const auto script = generateCrashRestartScript(rng, tupleIdCounter);
        ASSERT_FALSE(script.expectedTuples.empty());

        const auto testFilePath = writeRandomInlineTestFile(baseWorkingDir, script, iteration);

        SystestConfiguration config{};
        config.directlySpecifiedTestFiles.setValue(testFilePath.string());
        config.workingDir.setValue(baseWorkingDir.string());
        config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);

        const auto testFileMap = loadTestFileMap(config);
        SystestBinder binder{baseWorkingDir.string(), config.testDataDir.getValue(), config.configDir.getValue()};
        auto [queries, loaded] = binder.loadOptimizeQueries(testFileMap);
        ASSERT_EQ(loaded, testFileMap.size());
        ASSERT_EQ(queries.size(), 1);

        SystestProgressTracker tracker;
        tracker.setTotalQueries(1);
        SingleNodeWorkerConfiguration configuration;
        const auto failedInline = runInlineEventQueriesWithWorkerRestart(queries, configuration, tracker);
        EXPECT_TRUE(failedInline.empty());
    }
}

/// NOLINTEND(bugprone-unchecked-optional-access)
}
