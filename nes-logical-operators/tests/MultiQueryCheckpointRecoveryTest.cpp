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

#include <algorithm>
#include <cctype>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <memory>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp>

#include <BaseUnitTest.hpp>
#include <LegacyOptimizer.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

namespace NES
{
namespace
{
struct QueryScenario
{
    std::string logicalSourceName;
    std::filesystem::path sourceFile;
    std::filesystem::path sinkFile;
    size_t replayRowCount;
};

struct JoinQueryScenario
{
    std::string leftLogicalSourceName;
    std::string rightLogicalSourceName;
    std::filesystem::path leftSourceFile;
    std::filesystem::path rightSourceFile;
    std::filesystem::path sinkFile;
};

struct SinkAggregateRow
{
    uint64_t start;
    uint64_t end;
    uint64_t count;
};

std::filesystem::path makeUniqueTestDirectory()
{
    return std::filesystem::temp_directory_path()
        / ("multi-query-checkpoint-recovery-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
}

void writeSequentialRows(const std::filesystem::path& filePath, const size_t rowCount)
{
    std::ofstream out(filePath, std::ios::trunc);
    ASSERT_TRUE(out.is_open()) << "Failed to open " << filePath;
    for (size_t row = 1; row <= rowCount; ++row)
    {
        out << "0," << row << '\n';
    }
}

void writeRows(const std::filesystem::path& filePath, const std::initializer_list<std::string_view> rows)
{
    std::ofstream out(filePath, std::ios::trunc);
    ASSERT_TRUE(out.is_open()) << "Failed to open " << filePath;
    for (const auto row : rows)
    {
        out << row << '\n';
    }
}

std::vector<std::string> readNonEmptyLines(const std::filesystem::path& filePath)
{
    std::ifstream in(filePath);
    EXPECT_TRUE(in.is_open()) << "Failed to open " << filePath;

    std::vector<std::string> lines;
    for (std::string line; std::getline(in, line);)
    {
        if (!line.empty())
        {
            lines.push_back(line);
        }
    }
    return lines;
}

std::vector<std::filesystem::path> findCheckpointBundles(const std::filesystem::path& checkpointDirectory)
{
    std::vector<std::filesystem::path> bundles;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(checkpointDirectory, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
    {
        const auto& entry = *it;
        if (!entry.is_directory() || entry.path().filename() == ".recovery_snapshot")
        {
            continue;
        }
        if (std::filesystem::exists(entry.path() / "plan.pb") && std::filesystem::exists(entry.path() / "cache_manifest.json"))
        {
            bundles.push_back(entry.path());
        }
    }
    EXPECT_FALSE(ec) << "Failed to inspect checkpoint directory " << checkpointDirectory << ": " << ec.message();
    return bundles;
}

std::vector<std::filesystem::path> findFilesWithPrefix(const std::filesystem::path& directory, std::string_view filePrefix)
{
    std::vector<std::filesystem::path> files;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(directory, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
    {
        if (!it->is_regular_file())
        {
            continue;
        }
        const auto filename = it->path().filename().string();
        if (filename.starts_with(filePrefix))
        {
            files.push_back(it->path());
        }
    }
    EXPECT_FALSE(ec) << "Failed to inspect directory " << directory << ": " << ec.message();
    std::sort(files.begin(), files.end());
    return files;
}

std::optional<SinkAggregateRow> parseAggregateRow(const std::string& line)
{
    if (line.empty() || !std::isdigit(static_cast<unsigned char>(line.front())))
    {
        return std::nullopt;
    }

    const auto firstComma = line.find(',');
    const auto secondComma = line.find(',', firstComma == std::string::npos ? 0 : firstComma + 1);
    const auto thirdComma = line.find(',', secondComma == std::string::npos ? 0 : secondComma + 1);
    if (firstComma == std::string::npos || secondComma == std::string::npos || thirdComma != std::string::npos)
    {
        ADD_FAILURE() << "Malformed aggregate output row: " << line;
        return std::nullopt;
    }

    try
    {
        return SinkAggregateRow{
            .start = std::stoull(line.substr(0, firstComma)),
            .end = std::stoull(line.substr(firstComma + 1, secondComma - firstComma - 1)),
            .count = std::stoull(line.substr(secondComma + 1))};
    }
    catch (const std::exception& ex)
    {
        ADD_FAILURE() << "Failed to parse aggregate output row '" << line << "': " << ex.what();
        return std::nullopt;
    }
}

std::vector<SinkAggregateRow> readAggregateRows(const std::filesystem::path& filePath)
{
    std::vector<SinkAggregateRow> rows;
    for (const auto& line : readNonEmptyLines(filePath))
    {
        if (auto row = parseAggregateRow(line))
        {
            rows.push_back(*row);
        }
    }
    return rows;
}

uint64_t totalCount(const std::vector<SinkAggregateRow>& rows)
{
    return std::accumulate(
        rows.begin(),
        rows.end(),
        uint64_t{0},
        [](const auto sum, const auto& row)
        { return sum + row.count; });
}

std::string describeSinkOutput(const std::filesystem::path& filePath)
{
    const auto lines = readNonEmptyLines(filePath);
    return fmt::format("{}", fmt::join(lines, " | "));
}

template <typename Predicate>
void waitUntil(std::string_view description, Predicate&& predicate)
{
    constexpr auto timeout = std::chrono::seconds(20);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        if (predicate())
        {
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    FAIL() << "Timed out waiting for " << description;
}
}

class MultiQueryCheckpointRecoveryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("MultiQueryCheckpointRecoveryTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MultiQueryCheckpointRecoveryTest test case.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        testDirectory = makeUniqueTestDirectory();
        checkpointDirectory = testDirectory / "checkpoints";
        dataDirectory = testDirectory / "data";
        std::filesystem::create_directories(checkpointDirectory);
        std::filesystem::create_directories(dataDirectory);

        sourceCatalog = std::make_shared<SourceCatalog>();
        sinkCatalog = std::make_shared<SinkCatalog>();
        binder = std::make_shared<StatementBinder>(
            sourceCatalog,
            [](AntlrSQLParser::QueryContext* queryContext)
            { return AntlrSQLQueryParser::bindReplayableQueryPlan(queryContext); });
        sourceStatementHandler = std::make_unique<SourceStatementHandler>(sourceCatalog, RequireHostConfig{});
        sinkStatementHandler = std::make_unique<SinkStatementHandler>(sinkCatalog, RequireHostConfig{});
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove_all(testDirectory, ec);
        BaseUnitTest::TearDown();
    }

protected:
    void applyCatalogStatement(std::string_view sql) const
    {
        const auto statementResult = binder->parseAndBindSingle(sql);
        ASSERT_TRUE(statementResult.has_value()) << statementResult.error();
        const auto& statement = statementResult.value();

        if (std::holds_alternative<CreateLogicalSourceStatement>(statement))
        {
            const auto result = sourceStatementHandler->apply(std::get<CreateLogicalSourceStatement>(statement));
            ASSERT_TRUE(result.has_value()) << result.error();
            return;
        }
        if (std::holds_alternative<CreatePhysicalSourceStatement>(statement))
        {
            const auto result = sourceStatementHandler->apply(std::get<CreatePhysicalSourceStatement>(statement));
            ASSERT_TRUE(result.has_value()) << result.error();
            return;
        }
        if (std::holds_alternative<CreateSinkStatement>(statement))
        {
            const auto result = sinkStatementHandler->apply(std::get<CreateSinkStatement>(statement));
            ASSERT_TRUE(result.has_value()) << result.error();
            return;
        }

        FAIL() << "Unexpected non-catalog statement in setup: " << sql;
    }

    LogicalPlan buildPlan(const QueryScenario& scenario) const
    {
        applyCatalogStatement(fmt::format("CREATE LOGICAL SOURCE {}(i64 INT64, ts UINT64)", scenario.logicalSourceName));
        applyCatalogStatement(fmt::format(
            "CREATE PHYSICAL SOURCE FOR {} TYPE File SET ('{}' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\\n' AS "
            "PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)",
            scenario.logicalSourceName,
            scenario.sourceFile.string()));
        applyCatalogStatement(fmt::format(
            "CREATE SINK sink_{}({}.start UINT64, {}.end UINT64, {}.i64_out UINT64) TYPE File SET ('{}' AS `SINK`.FILE_PATH, "
            "'CSV' AS `SINK`.INPUT_FORMAT)",
            scenario.logicalSourceName,
            scenario.logicalSourceName,
            scenario.logicalSourceName,
            scenario.logicalSourceName,
            scenario.sinkFile.string()));

        const auto query = fmt::format(
            "SELECT start, end, COUNT(i64) as i64_out FROM {} WINDOW TUMBLING(ts, size 100ms) INTO sink_{}",
            scenario.logicalSourceName,
            scenario.logicalSourceName);
        const auto statementResult = binder->parseAndBindSingle(query);
        if (!statementResult.has_value())
        {
            throw std::runtime_error(statementResult.error().what());
        }
        if (!std::holds_alternative<QueryStatement>(statementResult.value()))
        {
            throw std::runtime_error("Expected query statement while building checkpoint recovery test plan");
        }
        return std::get<QueryStatement>(statementResult.value()).plan;
    }

    LogicalPlan buildJoinPlan(const JoinQueryScenario& scenario) const
    {
        applyCatalogStatement(fmt::format("CREATE LOGICAL SOURCE {}(id UINT64, ts UINT64)", scenario.leftLogicalSourceName));
        applyCatalogStatement(fmt::format(
            "CREATE PHYSICAL SOURCE FOR {} TYPE File SET ('{}' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\\n' AS "
            "PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)",
            scenario.leftLogicalSourceName,
            scenario.leftSourceFile.string()));
        applyCatalogStatement(fmt::format("CREATE LOGICAL SOURCE {}(id2 UINT64, ts UINT64)", scenario.rightLogicalSourceName));
        applyCatalogStatement(fmt::format(
            "CREATE PHYSICAL SOURCE FOR {} TYPE File SET ('{}' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\\n' AS "
            "PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER)",
            scenario.rightLogicalSourceName,
            scenario.rightSourceFile.string()));

        const auto sinkName = fmt::format("sink_{}_{}", scenario.leftLogicalSourceName, scenario.rightLogicalSourceName);
        applyCatalogStatement(fmt::format(
            "CREATE SINK {}({}{}.start UINT64, {}{}.end UINT64, {}.id UINT64, {}.ts UINT64, {}.id2 UINT64, {}.ts UINT64) "
            "TYPE File SET ('{}' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT)",
            sinkName,
            scenario.leftLogicalSourceName,
            scenario.rightLogicalSourceName,
            scenario.leftLogicalSourceName,
            scenario.rightLogicalSourceName,
            scenario.leftLogicalSourceName,
            scenario.leftLogicalSourceName,
            scenario.rightLogicalSourceName,
            scenario.rightLogicalSourceName,
            scenario.sinkFile.string()));

        const auto query = fmt::format(
            "SELECT * FROM (SELECT * FROM {}) INNER JOIN (SELECT * FROM {}) ON id = id2 WINDOW TUMBLING(ts, size 100ms) "
            "INTO {}",
            scenario.leftLogicalSourceName,
            scenario.rightLogicalSourceName,
            sinkName);
        const auto statementResult = binder->parseAndBindSingle(query);
        if (!statementResult.has_value())
        {
            throw std::runtime_error(statementResult.error().what());
        }
        if (!std::holds_alternative<QueryStatement>(statementResult.value()))
        {
            throw std::runtime_error("Expected join query statement while building checkpoint recovery test plan");
        }
        return std::get<QueryStatement>(statementResult.value()).plan;
    }

    static void waitForStoppedQuery(SingleNodeWorker& worker, const QueryId queryId)
    {
        waitUntil(
            fmt::format("query {} to stop", queryId),
            [&worker, queryId]()
            {
                auto status = worker.getQueryStatus(queryId);
                return status.has_value() && status->state == QueryState::Stopped;
            });
    }

    std::filesystem::path testDirectory;
    std::filesystem::path checkpointDirectory;
    std::filesystem::path dataDirectory;
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::shared_ptr<StatementBinder> binder;
    std::unique_ptr<SourceStatementHandler> sourceStatementHandler;
    std::unique_ptr<SinkStatementHandler> sinkStatementHandler;
};

TEST_F(MultiQueryCheckpointRecoveryTest, RestoresMultipleCheckpointedQueriesAcrossWorkerRestart)
{
    constexpr size_t initialRowCount = 100;
    constexpr size_t queryCount = 8;
    constexpr uint64_t manualCheckpointIntervalMs = 3'600'000;

    std::vector<QueryScenario> scenarios;
    scenarios.reserve(queryCount);
    for (size_t index = 0; index < queryCount; ++index)
    {
        scenarios.push_back(QueryScenario{
            .logicalSourceName = fmt::format("stream_{:02}", index),
            .sourceFile = dataDirectory / fmt::format("stream_{:02}.csv", index),
            .sinkFile = dataDirectory / fmt::format("sink_{:02}.csv", index),
            .replayRowCount = 3 + index});
    }

    std::vector<LogicalPlan> plans;
    plans.reserve(scenarios.size());
    for (const auto& scenario : scenarios)
    {
        writeSequentialRows(scenario.sourceFile, initialRowCount);
        plans.push_back(buildPlan(scenario));
    }

    SingleNodeWorkerConfiguration checkpointingConfig;
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointIntervalMs.setValue(manualCheckpointIntervalMs);
    checkpointingConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(false);

    std::vector<QueryId> initialQueryIds;
    initialQueryIds.reserve(plans.size());
    {
        SingleNodeWorker worker(checkpointingConfig);
        for (const auto& plan : plans)
        {
            auto queryId = worker.registerQuery(plan);
            ASSERT_TRUE(queryId.has_value()) << queryId.error().what();
            initialQueryIds.push_back(*queryId);
        }
        for (const auto& queryId : initialQueryIds)
        {
            auto started = worker.startQuery(queryId);
            ASSERT_TRUE(started.has_value()) << started.error().what();
        }
        for (const auto& queryId : initialQueryIds)
        {
            waitForStoppedQuery(worker, queryId);
        }

        CheckpointManager::runCallbacksOnce();

        for (const auto& queryId : initialQueryIds)
        {
            auto unregistered = worker.unregisterQuery(queryId);
            ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
        }
    }

    const auto bundles = findCheckpointBundles(checkpointDirectory);
    ASSERT_EQ(bundles.size(), scenarios.size());
    for (const auto& bundle : bundles)
    {
        EXPECT_TRUE(std::filesystem::exists(bundle / "plan.pb")) << bundle.string();
        EXPECT_TRUE(std::filesystem::exists(bundle / "cache_manifest.json")) << bundle.string();
    }

    for (const auto& scenario : scenarios)
    {
        std::filesystem::remove(scenario.sinkFile);
        writeSequentialRows(scenario.sourceFile, scenario.replayRowCount);
    }

    SingleNodeWorkerConfiguration recoveryConfig;
    recoveryConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    recoveryConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(true);

    {
        SingleNodeWorker worker(recoveryConfig);
        for (const auto& recoveredQueryId : initialQueryIds)
        {
            waitForStoppedQuery(worker, recoveredQueryId);
            auto unregistered = worker.unregisterQuery(recoveredQueryId);
            ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
        }
    }

    std::vector<uint64_t> recoveredTotals;
    recoveredTotals.reserve(scenarios.size());
    for (const auto& scenario : scenarios)
    {
        const auto rows = readAggregateRows(scenario.sinkFile);
        ASSERT_FALSE(rows.empty()) << "Recovered sink output for " << scenario.logicalSourceName << ": " << describeSinkOutput(scenario.sinkFile);
        recoveredTotals.push_back(totalCount(rows));
    }

    for (const auto& scenario : scenarios)
    {
        std::filesystem::remove(scenario.sinkFile);
    }

    SingleNodeWorkerConfiguration coldConfig;
    coldConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    coldConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(false);

    {
        SingleNodeWorker worker(coldConfig);
        std::vector<QueryId> coldQueryIds;
        coldQueryIds.reserve(plans.size());
        for (const auto& plan : plans)
        {
            auto queryId = worker.registerQuery(plan);
            ASSERT_TRUE(queryId.has_value()) << queryId.error().what();
            coldQueryIds.push_back(*queryId);
        }
        for (const auto& queryId : coldQueryIds)
        {
            auto started = worker.startQuery(queryId);
            ASSERT_TRUE(started.has_value()) << started.error().what();
        }
        for (const auto& queryId : coldQueryIds)
        {
            waitForStoppedQuery(worker, queryId);
            auto unregistered = worker.unregisterQuery(queryId);
            ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
        }
    }

    std::vector<uint64_t> coldTotals;
    coldTotals.reserve(scenarios.size());
    for (const auto& scenario : scenarios)
    {
        const auto rows = readAggregateRows(scenario.sinkFile);
        ASSERT_FALSE(rows.empty()) << "Cold sink output for " << scenario.logicalSourceName << ": " << describeSinkOutput(scenario.sinkFile);
        coldTotals.push_back(totalCount(rows));
    }

    for (size_t index = 0; index < scenarios.size(); ++index)
    {
        const auto& scenario = scenarios[index];
        EXPECT_GT(recoveredTotals[index], coldTotals[index])
            << "Recovered totals should exceed cold totals for " << scenario.logicalSourceName << ". recovered="
            << recoveredTotals[index] << " cold=" << coldTotals[index];
        if (recoveredTotals[index] > coldTotals[index])
        {
            EXPECT_GE(recoveredTotals[index] - coldTotals[index], initialRowCount - 2)
                << "Recovered totals should include checkpointed rows for " << scenario.logicalSourceName << ". recovered="
                << recoveredTotals[index] << " cold=" << coldTotals[index];
        }
    }
}

TEST_F(MultiQueryCheckpointRecoveryTest, CorruptAggregationCheckpointFailsRecoveryInsteadOfSilentlyReplayingPastCheckpoint)
{
    constexpr size_t initialRowCount = 100;
    constexpr size_t replayRowCount = 8;
    constexpr uint64_t manualCheckpointIntervalMs = 3'600'000;

    const QueryScenario scenario{
        .logicalSourceName = "stream_corrupt_agg",
        .sourceFile = dataDirectory / "stream_corrupt_agg.csv",
        .sinkFile = dataDirectory / "sink_corrupt_agg.csv",
        .replayRowCount = replayRowCount};
    writeSequentialRows(scenario.sourceFile, initialRowCount);
    auto plan = buildPlan(scenario);

    SingleNodeWorkerConfiguration checkpointingConfig;
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointIntervalMs.setValue(manualCheckpointIntervalMs);
    checkpointingConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(false);

    QueryId initialQueryId = INVALID_QUERY_ID;
    {
        SingleNodeWorker worker(checkpointingConfig);
        auto queryId = worker.registerQuery(plan);
        ASSERT_TRUE(queryId.has_value()) << queryId.error().what();
        initialQueryId = *queryId;

        auto started = worker.startQuery(initialQueryId);
        ASSERT_TRUE(started.has_value()) << started.error().what();
        waitForStoppedQuery(worker, initialQueryId);

        CheckpointManager::runCallbacksOnce();

        auto unregistered = worker.unregisterQuery(initialQueryId);
        ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
    }

    const auto bundles = findCheckpointBundles(checkpointDirectory);
    ASSERT_EQ(bundles.size(), 1U);
    const auto stateDirectory = bundles.front() / "state";
    const auto aggregationCheckpointFiles = findFilesWithPrefix(stateDirectory, "aggregation_hashmap_");
    ASSERT_EQ(aggregationCheckpointFiles.size(), 1U);
    const auto aggregationCheckpointFile = aggregationCheckpointFiles.front();

    {
        std::ofstream out(aggregationCheckpointFile, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open()) << "Failed to corrupt " << aggregationCheckpointFile;
        out << "corrupt";
    }

    std::filesystem::remove(scenario.sinkFile);
    writeSequentialRows(scenario.sourceFile, replayRowCount);

    SingleNodeWorkerConfiguration recoveryConfig;
    recoveryConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    recoveryConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(true);

    {
        SingleNodeWorker worker(recoveryConfig);
        const auto recoveredQueryId = initialQueryId;
        waitUntil(
            fmt::format("query {} to fail", recoveredQueryId),
            [&worker, recoveredQueryId]()
            {
                auto status = worker.getQueryStatus(recoveredQueryId);
                return status.has_value() && status->state == QueryState::Failed;
            });

        auto status = worker.getQueryStatus(recoveredQueryId);
        ASSERT_TRUE(status.has_value()) << status.error().what();
        EXPECT_EQ(status->state, QueryState::Failed);

        auto unregistered = worker.unregisterQuery(recoveredQueryId);
        ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
    }

    if (std::filesystem::exists(scenario.sinkFile))
    {
        EXPECT_TRUE(readAggregateRows(scenario.sinkFile).empty())
            << "Corrupt checkpoint recovery should not emit partial output: " << describeSinkOutput(scenario.sinkFile);
    }

    std::error_code cleanupEc;
    std::filesystem::remove(aggregationCheckpointFile, cleanupEc);
}

TEST_F(MultiQueryCheckpointRecoveryTest, IncompleteCacheManifestSkipsBundleRecovery)
{
    constexpr size_t initialRowCount = 100;
    constexpr uint64_t manualCheckpointIntervalMs = 3'600'000;

    const QueryScenario scenario{
        .logicalSourceName = "stream_incomplete_manifest",
        .sourceFile = dataDirectory / "stream_incomplete_manifest.csv",
        .sinkFile = dataDirectory / "sink_incomplete_manifest.csv",
        .replayRowCount = 5};
    writeSequentialRows(scenario.sourceFile, initialRowCount);
    auto plan = buildPlan(scenario);

    QueryId initialQueryId = INVALID_QUERY_ID;
    SingleNodeWorkerConfiguration checkpointingConfig;
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointIntervalMs.setValue(manualCheckpointIntervalMs);
    checkpointingConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(false);
    {
        SingleNodeWorker worker(checkpointingConfig);
        auto queryId = worker.registerQuery(plan);
        ASSERT_TRUE(queryId.has_value()) << queryId.error().what();
        initialQueryId = *queryId;

        auto started = worker.startQuery(initialQueryId);
        ASSERT_TRUE(started.has_value()) << started.error().what();
        waitForStoppedQuery(worker, initialQueryId);

        CheckpointManager::runCallbacksOnce();

        auto unregistered = worker.unregisterQuery(initialQueryId);
        ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
    }

    const auto bundles = findCheckpointBundles(checkpointDirectory);
    ASSERT_EQ(bundles.size(), 1U);
    const auto manifestPath = bundles.front() / "cache_manifest.json";

    std::ifstream manifestIn(manifestPath);
    ASSERT_TRUE(manifestIn.is_open()) << "Failed to open " << manifestPath;
    auto manifestJson = nlohmann::json::parse(manifestIn);
    manifestIn.close();
    manifestJson.erase("plan_fingerprint");

    std::ofstream manifestOut(manifestPath, std::ios::trunc);
    ASSERT_TRUE(manifestOut.is_open()) << "Failed to rewrite " << manifestPath;
    manifestOut << manifestJson.dump(2);
    manifestOut.close();

    std::filesystem::remove(scenario.sinkFile);

    SingleNodeWorkerConfiguration recoveryConfig;
    recoveryConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    recoveryConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(true);
    {
        SingleNodeWorker worker(recoveryConfig);
        const auto unrecoveredQueryId = initialQueryId;
        auto status = worker.getQueryStatus(unrecoveredQueryId);
        EXPECT_FALSE(status.has_value());
        EXPECT_FALSE(std::filesystem::exists(scenario.sinkFile));
    }
}

TEST_F(MultiQueryCheckpointRecoveryTest, CorruptHashJoinCheckpointFailsRecoveryInsteadOfSilentlyReplayingPastCheckpoint)
{
    constexpr uint64_t manualCheckpointIntervalMs = 3'600'000;

    const JoinQueryScenario scenario{
        .leftLogicalSourceName = "left_stream_corrupt_join",
        .rightLogicalSourceName = "right_stream_corrupt_join",
        .leftSourceFile = dataDirectory / "left_stream_corrupt_join.csv",
        .rightSourceFile = dataDirectory / "right_stream_corrupt_join.csv",
        .sinkFile = dataDirectory / "sink_corrupt_join.csv"};
    writeRows(scenario.leftSourceFile, {"1,0", "1,5"});
    writeRows(scenario.rightSourceFile, {});
    auto plan = buildJoinPlan(scenario);

    QueryId initialQueryId = INVALID_QUERY_ID;
    SingleNodeWorkerConfiguration checkpointingConfig;
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    checkpointingConfig.workerConfiguration.checkpointConfiguration.checkpointIntervalMs.setValue(manualCheckpointIntervalMs);
    checkpointingConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(false);
    {
        SingleNodeWorker worker(checkpointingConfig);
        auto queryId = worker.registerQuery(plan);
        ASSERT_TRUE(queryId.has_value()) << queryId.error().what();
        initialQueryId = *queryId;

        auto started = worker.startQuery(initialQueryId);
        ASSERT_TRUE(started.has_value()) << started.error().what();
        waitForStoppedQuery(worker, initialQueryId);

        CheckpointManager::runCallbacksOnce();

        auto unregistered = worker.unregisterQuery(initialQueryId);
        ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
    }

    const auto bundles = findCheckpointBundles(checkpointDirectory);
    ASSERT_EQ(bundles.size(), 1U);
    const auto stateDirectory = bundles.front() / "state";
    const auto hashJoinCheckpointFiles = findFilesWithPrefix(stateDirectory, "hash_join_checkpoint_");
    ASSERT_EQ(hashJoinCheckpointFiles.size(), 1U);
    const auto hashJoinCheckpointFile = hashJoinCheckpointFiles.front();

    {
        std::ofstream out(hashJoinCheckpointFile, std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.is_open()) << "Failed to corrupt " << hashJoinCheckpointFile;
        out << "corrupt";
    }

    std::filesystem::remove(scenario.sinkFile);
    writeRows(scenario.leftSourceFile, {});
    writeRows(scenario.rightSourceFile, {"1,10", "1,20"});

    SingleNodeWorkerConfiguration recoveryConfig;
    recoveryConfig.workerConfiguration.checkpointConfiguration.checkpointDirectory.setValue(checkpointDirectory.string());
    recoveryConfig.workerConfiguration.checkpointConfiguration.recoverFromCheckpoint.setValue(true);
    {
        SingleNodeWorker worker(recoveryConfig);
        const auto recoveredQueryId = initialQueryId;
        waitUntil(
            fmt::format("query {} to fail", recoveredQueryId),
            [&worker, recoveredQueryId]()
            {
                auto status = worker.getQueryStatus(recoveredQueryId);
                return status.has_value() && status->state == QueryState::Failed;
            });

        auto status = worker.getQueryStatus(recoveredQueryId);
        ASSERT_TRUE(status.has_value()) << status.error().what();
        EXPECT_EQ(status->state, QueryState::Failed);

        auto unregistered = worker.unregisterQuery(recoveredQueryId);
        ASSERT_TRUE(unregistered.has_value()) << unregistered.error().what();
    }

    if (std::filesystem::exists(scenario.sinkFile))
    {
        const auto lines = readNonEmptyLines(scenario.sinkFile);
        EXPECT_TRUE(std::none_of(
            lines.begin(),
            lines.end(),
            [](const auto& line) { return !line.empty() && std::isdigit(static_cast<unsigned char>(line.front())); }))
            << "Corrupt hash join recovery should not emit partial output: " << describeSinkOutput(scenario.sinkFile);
    }

    std::error_code cleanupEc;
    std::filesystem::remove(hashJoinCheckpointFile, cleanupEc);
}

}
