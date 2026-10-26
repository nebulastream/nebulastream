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
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <BaseUnitTest.hpp>
#include <ModelCatalog.hpp>
#include <QueryId.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{
constexpr std::string_view WorkerHost = "localhost:8080";
constexpr uint64_t WindowSize = 100;
/// Every row has the same width, which divides the buffer size, so that every source buffer ends at a tuple boundary, as replay requires.
constexpr size_t RowWidth = 16;
/// A file source only emits complete buffers. A chunk fills whole buffers, so the source emits all of it without waiting for more input.
constexpr uint64_t RowsPerChunk = 2048;
/// Long enough that the worker only checkpoints when the test asks it to.
constexpr uint64_t ManualCheckpointIntervalMs = 3'600'000;
constexpr auto Timeout = std::chrono::seconds(60);

using WindowCount = std::tuple<uint64_t, uint64_t, uint64_t>;

/// Pads the first field of each row with ones to the row width. Queries only count the rows, or join rows with equal padding.
std::string makeRows(const uint64_t firstTimestamp, const uint64_t lastTimestamp)
{
    std::string rows;
    for (auto timestamp = firstTimestamp; timestamp <= lastTimestamp; ++timestamp)
    {
        const auto timestampString = std::to_string(timestamp);
        rows += fmt::format("{},{}\n", std::string(RowWidth - 2 - timestampString.size(), '1'), timestampString);
    }
    return rows;
}

std::vector<WindowCount> expectedWindowCounts(const uint64_t lastTimestamp)
{
    std::vector<WindowCount> windows;
    for (uint64_t start = 0; start <= lastTimestamp; start += WindowSize)
    {
        const auto firstTimestamp = std::max<uint64_t>(start, 1);
        const auto lastTimestampInWindow = std::min(lastTimestamp, start + WindowSize - 1);
        windows.emplace_back(start, start + WindowSize, lastTimestampInWindow - firstTimestamp + 1);
    }
    return windows;
}

std::vector<std::string> readDataRows(const std::filesystem::path& file)
{
    std::vector<std::string> rows;
    std::ifstream in(file);
    for (std::string line; std::getline(in, line);)
    {
        /// The first line holds the schema.
        if (not line.empty() and std::isdigit(static_cast<unsigned char>(line.front())))
        {
            rows.push_back(line);
        }
    }
    return rows;
}

std::vector<WindowCount> readWindowCounts(const std::filesystem::path& sinkFile)
{
    std::vector<WindowCount> windows;
    for (const auto& row : readDataRows(sinkFile))
    {
        const auto fields = row | std::views::split(',')
            | std::views::transform([](const auto& field) { return std::stoull(std::string(field.begin(), field.end())); })
            | std::ranges::to<std::vector>();
        EXPECT_EQ(fields.size(), 3) << "Unexpected sink row " << row;
        if (fields.size() == 3)
        {
            windows.emplace_back(fields[0], fields[1], fields[2]);
        }
    }
    std::ranges::sort(windows);
    return windows;
}

std::string readFile(const std::filesystem::path& file)
{
    std::ifstream in(file, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

bool waitUntil(const std::function<bool()>& condition)
{
    const auto deadline = std::chrono::steady_clock::now() + Timeout;
    while (not condition())
    {
        if (std::chrono::steady_clock::now() > deadline)
        {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return true;
}

/// The windows of the last buffers that a source emitted may stay open until more input arrives, so the test cannot wait for specific
/// windows. Once the output settles, no window is on its way to the sink, which a checkpoint would miss.
bool waitUntilWindowsSettle(const std::filesystem::path& sinkFile)
{
    constexpr auto settleTime = std::chrono::milliseconds(500);
    auto windows = readWindowCounts(sinkFile);
    auto lastChange = std::chrono::steady_clock::now();
    return waitUntil(
        [&]
        {
            auto currentWindows = readWindowCounts(sinkFile);
            if (currentWindows != windows)
            {
                windows = std::move(currentWindows);
                lastChange = std::chrono::steady_clock::now();
                return false;
            }
            return not windows.empty() and std::chrono::steady_clock::now() - lastChange > settleTime;
        });
}

/// Lets the test decide when a file source sees which input. Until the pipe is closed, the source waits for more input, so the query keeps
/// running while the test checkpoints it.
class InputPipe
{
public:
    explicit InputPipe(std::filesystem::path path) : path(std::move(path))
    {
        if (mkfifo(this->path.c_str(), 0600) != 0)
        {
            throw std::runtime_error(fmt::format("Cannot create the pipe {}", this->path.string()));
        }
        /// Opening the pipe for reading as well does not wait for the source to open it.
        descriptor = ::open(this->path.c_str(), O_RDWR);
        if (descriptor < 0)
        {
            throw std::runtime_error(fmt::format("Cannot open the pipe {}", this->path.string()));
        }
    }

    ~InputPipe() { close(); }

    InputPipe(const InputPipe&) = delete;
    InputPipe& operator=(const InputPipe&) = delete;
    InputPipe(InputPipe&&) = delete;
    InputPipe& operator=(InputPipe&&) = delete;

    void write(const std::string_view rows)
    {
        for (auto remaining = rows; not remaining.empty();)
        {
            const auto written = ::write(descriptor, remaining.data(), remaining.size());
            if (written < 0)
            {
                throw std::runtime_error(fmt::format("Cannot write to the pipe {}", path.string()));
            }
            remaining.remove_prefix(static_cast<size_t>(written));
        }
        content += rows;
    }

    /// The source sees the end of its input.
    void close()
    {
        if (descriptor >= 0)
        {
            ::close(descriptor);
            descriptor = -1;
        }
    }

    /// A recovered file source skips the input that it persisted before, which requires a regular file.
    void replaceByFile() const
    {
        std::filesystem::remove(path);
        std::ofstream(path, std::ios::binary) << content;
    }

    [[nodiscard]] const std::filesystem::path& getPath() const { return path; }

private:
    std::filesystem::path path;
    int descriptor = -1;
    std::string content;
};

struct AggregationQuery
{
    std::string sourceName;
    std::filesystem::path inputFile;
    std::filesystem::path sinkFile;
};

struct JoinQuery
{
    std::string leftSourceName;
    std::string rightSourceName;
    std::filesystem::path leftInputFile;
    std::filesystem::path rightInputFile;
    std::filesystem::path sinkFile;
};
}

class MultiQueryCheckpointRecoveryTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("MultiQueryCheckpointRecoveryTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup MultiQueryCheckpointRecoveryTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        testDirectory = std::filesystem::temp_directory_path()
            / fmt::format("multi-query-checkpoint-recovery-{}", std::chrono::steady_clock::now().time_since_epoch().count());
        checkpointDirectory = testDirectory / "checkpoints";
        std::filesystem::create_directories(checkpointDirectory);
        workerCatalog->addWorker(Host(std::string(WorkerHost)), "", Capacity(CapacityKind::Unlimited{}), {});
    }

    void TearDown() override
    {
        std::error_code errorCode;
        std::filesystem::remove_all(testDirectory, errorCode);
        BaseUnitTest::TearDown();
    }

protected:
    [[nodiscard]] SingleNodeWorkerConfiguration workerConfiguration(const bool recoverFromCheckpoint) const
    {
        SingleNodeWorkerConfiguration configuration;
        configuration.workerConfiguration.checkpoint.checkpointDirectory.setValue(checkpointDirectory.string());
        configuration.workerConfiguration.checkpoint.checkpointIntervalMs.setValue(ManualCheckpointIntervalMs);
        configuration.workerConfiguration.checkpoint.recoverFromCheckpoint.setValue(recoverFromCheckpoint);
        return configuration;
    }

    void applyCatalogStatement(const std::string_view sql) const
    {
        const auto statement = binder.parseAndBindSingle(sql);
        ASSERT_TRUE(statement.has_value()) << statement.error().what();
        handleStatements({*statement}, sourceStatementHandler, sinkStatementHandler);
    }

    void createFileSource(const std::string_view logicalSchema, const std::string& sourceName, const std::filesystem::path& inputFile) const
    {
        applyCatalogStatement(fmt::format("CREATE LOGICAL SOURCE {}({})", sourceName, logicalSchema));
        applyCatalogStatement(fmt::format(
            R"(CREATE PHYSICAL SOURCE FOR {} TYPE File SET('{}' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE", '\n' AS INPUT_FORMATTER.TUPLE_DELIMITER, ',' AS INPUT_FORMATTER.FIELD_DELIMITER))",
            sourceName,
            inputFile.string()));
    }

    [[nodiscard]] LogicalPlan planQuery(const std::string_view sql) const
    {
        const auto statement = binder.parseAndBindSingle(sql);
        if (not statement.has_value())
        {
            throw statement.error();
        }
        const auto distributedPlan = queryOptimizer.optimize(std::get<QueryStatement>(*statement).plan);
        return distributedPlan[Host(std::string(WorkerHost))].front();
    }

    [[nodiscard]] LogicalPlan planAggregation(const AggregationQuery& query) const
    {
        createFileSource("i64 INT64 NOT NULL, ts UINT64 NOT NULL", query.sourceName, query.inputFile);
        applyCatalogStatement(fmt::format(
            R"(CREATE SINK sink_{}(start UINT64 NOT NULL, end UINT64 NOT NULL, i64_count UINT64 NOT NULL) TYPE File SET('{}' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT))",
            query.sourceName,
            query.sinkFile.string()));
        return planQuery(fmt::format(
            "SELECT start, end, COUNT(i64) AS i64_count FROM {} WINDOW TUMBLING(ts, size {} ms) INTO sink_{}",
            query.sourceName,
            WindowSize,
            query.sourceName));
    }

    [[nodiscard]] LogicalPlan planJoin(const JoinQuery& query) const
    {
        createFileSource("id UINT64 NOT NULL, ts UINT64 NOT NULL", query.leftSourceName, query.leftInputFile);
        createFileSource("id2 UINT64 NOT NULL, ts2 UINT64 NOT NULL", query.rightSourceName, query.rightInputFile);
        applyCatalogStatement(fmt::format(
            R"(CREATE SINK sink_join(start UINT64 NOT NULL, end UINT64 NOT NULL, id UINT64 NOT NULL, ts UINT64 NOT NULL, id2 UINT64 NOT NULL, ts2 UINT64 NOT NULL) TYPE File SET('{}' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT))",
            query.sinkFile.string()));
        return planQuery(fmt::format(
            "SELECT * FROM (SELECT * FROM {}) INNER JOIN (SELECT * FROM {}) ON id = id2 WINDOW TUMBLING(ts, ts2, size {} ms) INTO "
            "sink_join",
            query.leftSourceName,
            query.rightSourceName,
            WindowSize));
    }

    [[nodiscard]] static bool waitForState(const SingleNodeWorker& worker, const QueryId& queryId, const QueryStatus state)
    {
        return waitUntil(
            [&]
            {
                const auto status = worker.getQueryStatus(queryId);
                return status.has_value() and status->state == state;
            });
    }

    [[nodiscard]] std::vector<std::filesystem::path> findQueryDirectories() const
    {
        std::vector<std::filesystem::path> queryDirectories;
        for (const auto& entry : std::filesystem::directory_iterator(checkpointDirectory))
        {
            if (entry.is_directory() and entry.path().filename().string().starts_with(CheckpointManager::QueryDirectoryPrefix))
            {
                queryDirectories.push_back(entry.path());
            }
        }
        std::ranges::sort(queryDirectories);
        return queryDirectories;
    }

    [[nodiscard]] static std::optional<std::filesystem::path> findCompletedRound(const std::filesystem::path& queryDirectory)
    {
        const auto round = CheckpointManager::loadFile(queryDirectory / "completed_round");
        if (not round)
        {
            return std::nullopt;
        }
        return queryDirectory / "rounds" / *round;
    }

    /// The highest input that the round covers among the sources of the query.
    [[nodiscard]] static uint64_t getHighestSourceCut(const std::filesystem::path& roundDirectory)
    {
        uint64_t highestCut = 0;
        std::error_code errorCode;
        for (const auto& entry : std::filesystem::directory_iterator(roundDirectory / "source", errorCode))
        {
            highestCut = std::max<uint64_t>(highestCut, std::stoull(CheckpointManager::loadFile(entry.path()).value_or("0")));
        }
        return highestCut;
    }

    [[nodiscard]] static std::vector<std::filesystem::path> findStateFiles(const std::filesystem::path& roundDirectory)
    {
        std::vector<std::filesystem::path> stateFiles;
        for (const auto& entry : std::filesystem::recursive_directory_iterator(roundDirectory / "state"))
        {
            if (entry.is_regular_file())
            {
                stateFiles.push_back(entry.path());
            }
        }
        return stateFiles;
    }

    /// Checkpoints until every query took part in a round whose sources cover some input. A query only takes part in rounds once it runs.
    [[nodiscard]] bool checkpointUntilAllQueriesCoverInput(SingleNodeWorker& worker, const size_t queryCount) const
    {
        return waitUntil(
            [&]
            {
                EXPECT_TRUE(worker.checkpoint().has_value());
                const auto queryDirectories = findQueryDirectories();
                return queryDirectories.size() == queryCount
                    and std::ranges::all_of(
                           queryDirectories,
                           [](const auto& queryDirectory)
                           {
                               const auto round = findCompletedRound(queryDirectory);
                               return round and getHighestSourceCut(*round) > 0;
                           });
            });
    }

    /// Runs the queries up to a checkpoint and on to their end, so that the sinks and the persisted input contain more than the checkpoint
    /// covers. Afterwards, the worker stops without checkpointing again, as if it crashed after the checkpoint.
    void runAggregationsPastCheckpoint(
        const std::vector<AggregationQuery>& queries, const std::vector<uint64_t>& lastTimestamps, std::vector<QueryId>& queryIds)
    {
        std::vector<LogicalPlan> plans;
        for (const auto& query : queries)
        {
            plans.push_back(planAggregation(query));
        }

        SingleNodeWorker worker(workerConfiguration(false));
        /// Declared after the worker, so that the sources see the end of their input before the worker stops them.
        std::vector<std::unique_ptr<InputPipe>> pipes;
        for (const auto& [query, plan] : std::views::zip(queries, plans))
        {
            pipes.push_back(std::make_unique<InputPipe>(query.inputFile));
            const auto queryId = worker.startQuery(plan);
            ASSERT_TRUE(queryId.has_value()) << queryId.error().what();
            queryIds.push_back(*queryId);
        }

        for (const auto& [query, pipe] : std::views::zip(queries, pipes))
        {
            pipe->write(makeRows(1, RowsPerChunk));
            ASSERT_TRUE(waitUntilWindowsSettle(query.sinkFile)) << query.sourceName;
        }
        ASSERT_TRUE(checkpointUntilAllQueriesCoverInput(worker, queries.size()));

        for (const auto& [pipe, lastTimestamp, queryId] : std::views::zip(pipes, lastTimestamps, queryIds))
        {
            pipe->write(makeRows(RowsPerChunk + 1, lastTimestamp));
            pipe->close();
            ASSERT_TRUE(waitForState(worker, queryId, QueryStatus::Stopped));
            pipe->replaceByFile();
        }
    }

    std::filesystem::path testDirectory;
    std::filesystem::path checkpointDirectory;
    std::shared_ptr<SourceCatalog> sourceCatalog = std::make_shared<SourceCatalog>();
    std::shared_ptr<SinkCatalog> sinkCatalog = std::make_shared<SinkCatalog>();
    std::shared_ptr<WorkerCatalog> workerCatalog = std::make_shared<WorkerCatalog>();
    StatementBinder binder{
        sourceCatalog,
        [](auto&& queryContext) { return AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(queryContext)>(queryContext)); }};
    mutable SourceStatementHandler sourceStatementHandler{sourceCatalog, DefaultHost(std::string(WorkerHost))};
    mutable SinkStatementHandler sinkStatementHandler{sinkCatalog, DefaultHost(std::string(WorkerHost))};
    QueryOptimizer queryOptimizer{
        QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, std::make_shared<ModelCatalog>()};
};

TEST_F(MultiQueryCheckpointRecoveryTest, RestoresMultipleCheckpointedQueriesAcrossWorkerRestart)
{
    constexpr size_t queryCount = 4;
    std::vector<AggregationQuery> queries;
    std::vector<uint64_t> lastTimestamps;
    for (size_t index = 0; index < queryCount; ++index)
    {
        queries.push_back(
            {.sourceName = fmt::format("stream_{}", index),
             .inputFile = testDirectory / fmt::format("stream_{}.csv", index),
             .sinkFile = testDirectory / fmt::format("sink_{}.csv", index)});
        /// The queries process a different amount of input after the checkpoint.
        lastTimestamps.push_back(RowsPerChunk * (index + 2));
    }

    std::vector<QueryId> queryIds;
    runAggregationsPastCheckpoint(queries, lastTimestamps, queryIds);
    ASSERT_FALSE(HasFatalFailure());

    const auto queryDirectories = findQueryDirectories();
    ASSERT_EQ(queryDirectories.size(), queryCount);
    for (const auto& queryDirectory : queryDirectories)
    {
        const auto round = findCompletedRound(queryDirectory);
        ASSERT_TRUE(round.has_value()) << queryDirectory;
        EXPECT_EQ(findStateFiles(*round).size(), 1) << *round;
        /// The checkpoint discarded the input that it covers, so the recovered results are only complete if recovery restored the state.
        bool prunedInput = false;
        for (const auto& source : std::filesystem::directory_iterator(queryDirectory / "source_replay"))
        {
            prunedInput = prunedInput or std::filesystem::exists(source.path() / "pruned.meta");
        }
        EXPECT_TRUE(prunedInput) << queryDirectory;
    }

    {
        SingleNodeWorker worker(workerConfiguration(true));
        for (const auto& queryId : queryIds)
        {
            EXPECT_TRUE(waitForState(worker, queryId, QueryStatus::Stopped)) << queryId;
        }
    }

    for (const auto& [query, lastTimestamp] : std::views::zip(queries, lastTimestamps))
    {
        EXPECT_EQ(readWindowCounts(query.sinkFile), expectedWindowCounts(lastTimestamp)) << query.sourceName;
    }
}

TEST_F(MultiQueryCheckpointRecoveryTest, CorruptAggregationCheckpointFailsRecoveryInsteadOfSilentlyReplayingPastCheckpoint)
{
    const std::vector<AggregationQuery> queries{
        {.sourceName = "stream_corrupt_agg",
         .inputFile = testDirectory / "stream_corrupt_agg.csv",
         .sinkFile = testDirectory / "sink.csv"}};
    const std::vector lastTimestamps{2 * RowsPerChunk};

    std::vector<QueryId> queryIds;
    runAggregationsPastCheckpoint(queries, lastTimestamps, queryIds);
    ASSERT_FALSE(HasFatalFailure());

    const auto queryDirectories = findQueryDirectories();
    ASSERT_EQ(queryDirectories.size(), 1);
    const auto round = findCompletedRound(queryDirectories.front());
    ASSERT_TRUE(round.has_value());
    const auto stateFiles = findStateFiles(*round);
    ASSERT_EQ(stateFiles.size(), 1);
    std::ofstream(stateFiles.front(), std::ios::binary | std::ios::trunc) << "corrupt";

    {
        SingleNodeWorker worker(workerConfiguration(true));
        EXPECT_TRUE(waitForState(worker, queryIds.front(), QueryStatus::Failed));
    }

    /// Without the state, the windows that the checkpoint covered partially would be emitted with lower counts.
    const auto expectedWindows = expectedWindowCounts(lastTimestamps.front());
    for (const auto& window : readWindowCounts(queries.front().sinkFile))
    {
        EXPECT_NE(std::ranges::find(expectedWindows, window), expectedWindows.end()) << fmt::format("Unexpected window {}", window);
    }
}

TEST_F(MultiQueryCheckpointRecoveryTest, UnreadableManifestSkipsRecovery)
{
    const std::vector<AggregationQuery> queries{
        {.sourceName = "stream_unreadable_manifest",
         .inputFile = testDirectory / "stream_unreadable_manifest.csv",
         .sinkFile = testDirectory / "sink.csv"}};
    const std::vector lastTimestamps{2 * RowsPerChunk};

    std::vector<QueryId> queryIds;
    runAggregationsPastCheckpoint(queries, lastTimestamps, queryIds);
    ASSERT_FALSE(HasFatalFailure());

    const auto queryDirectories = findQueryDirectories();
    ASSERT_EQ(queryDirectories.size(), 1);
    std::ofstream(queryDirectories.front() / "manifest", std::ios::trunc) << "format_version=0\n";
    const auto sinkContent = readFile(queries.front().sinkFile);

    {
        const SingleNodeWorker worker(workerConfiguration(true));
        EXPECT_FALSE(worker.getQueryStatus(queryIds.front()).has_value());
    }
    EXPECT_EQ(readFile(queries.front().sinkFile), sinkContent);
}

TEST_F(MultiQueryCheckpointRecoveryTest, CorruptHashJoinCheckpointFailsRecoveryInsteadOfSilentlyReplayingPastCheckpoint)
{
    const JoinQuery query{
        .leftSourceName = "left_stream_corrupt_join",
        .rightSourceName = "right_stream_corrupt_join",
        .leftInputFile = testDirectory / "left_stream_corrupt_join.csv",
        .rightInputFile = testDirectory / "right_stream_corrupt_join.csv",
        .sinkFile = testDirectory / "sink.csv"};
    const auto plan = planJoin(query);

    QueryId queryId = INVALID_QUERY_ID;
    {
        SingleNodeWorker worker(workerConfiguration(false));
        InputPipe leftPipe(query.leftInputFile);
        InputPipe rightPipe(query.rightInputFile);
        const auto startedQueryId = worker.startQuery(plan);
        ASSERT_TRUE(startedQueryId.has_value()) << startedQueryId.error().what();
        queryId = *startedQueryId;

        /// Only the left side receives input, so the state of the join holds the left rows, but the join emits nothing yet.
        leftPipe.write(makeRows(1, RowsPerChunk));
        ASSERT_TRUE(checkpointUntilAllQueriesCoverInput(worker, 1));

        leftPipe.close();
        rightPipe.close();
        ASSERT_TRUE(waitForState(worker, queryId, QueryStatus::Stopped));
        leftPipe.replaceByFile();
        rightPipe.replaceByFile();
    }

    const auto queryDirectories = findQueryDirectories();
    ASSERT_EQ(queryDirectories.size(), 1);
    const auto round = findCompletedRound(queryDirectories.front());
    ASSERT_TRUE(round.has_value());
    const auto stateFiles = findStateFiles(*round);
    ASSERT_EQ(stateFiles.size(), 1);
    std::ofstream(stateFiles.front(), std::ios::binary | std::ios::trunc) << "corrupt";
    std::filesystem::remove(query.sinkFile);

    {
        SingleNodeWorker worker(workerConfiguration(true));
        EXPECT_TRUE(waitForState(worker, queryId, QueryStatus::Failed));
    }

    EXPECT_TRUE(readDataRows(query.sinkFile).empty()) << fmt::format("{}", readDataRows(query.sinkFile));
}

}
