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

/// The point of the whole port: the query engine's own task events, aggregated into a statistic, reaching the
/// StatisticInterface.
///
///   TaskStatisticListener -> InProcessFeed -> InProcessSource -> WindowedAggregation(ScalarStatistic Avg)
///                         -> StatisticStoreWriter -> GrpcSink -> StatisticInterface
///
/// A second query over a Generator source supplies the load, because the statistic query itself is excluded from
/// the feed -- the listener recognises a query that reads an InProcess source and drops its events, so a query
/// observing the engine cannot observe itself.
///
/// This is the in-process counterpart of scripts/run-engine-stats-demo.sh, which does the same thing through the
/// REPL and SQL.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <CollectionDomain.hpp>
#include <ConditionTrigger.hpp>
#include <DefaultStatisticQueryGenerator.hpp>
#include <DistributedLogicalPlan.hpp>
#include <Metric.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <RequestStatisticStatement.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticInterface.hpp>
#include <StatisticTestSupport.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{

using namespace StatisticTestSupport;

constexpr auto FEED_NAME = "engine_events";
constexpr auto STATS_SOURCE = "engineStats";
constexpr auto LOAD_SOURCE = "endless";

/// One statistic window. Short, because the test has to wait for at least one to close.
constexpr uint64_t TASK_WINDOW_MS = 500;

/// Exactly the columns TaskStatisticListener formats, in order.
Schema<UnqualifiedUnboundField, Ordered> engineStatsSchema()
{
    const auto uint64Type = DataTypeProvider::provideDataType(DataType::Type::UINT64, DataType::NULLABLE::NOT_NULLABLE);
    const auto varsized = DataTypeProvider::provideDataType(DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE);
    return Schema<UnqualifiedUnboundField, Ordered>{
        UnqualifiedUnboundField{Identifier::parse("event_type"), varsized},
        UnqualifiedUnboundField{Identifier::parse("ts_us"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("thread_id"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("query_id"), varsized},
        UnqualifiedUnboundField{Identifier::parse("pipeline_id"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("task_id"), uint64Type},
        UnqualifiedUnboundField{Identifier::parse("tuples"), uint64Type}};
}

Schema<UnqualifiedUnboundField, Ordered> loadSchema()
{
    return Schema<UnqualifiedUnboundField, Ordered>{UnqualifiedUnboundField{Identifier::parse("ts"), DataType::Type::UINT64}};
}

}

class StatisticTaskQueueTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticTaskQueueTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        StatisticStoreRegistry::instance().clear();
    }

    void TearDown() override
    {
        StatisticStoreRegistry::instance().clear();
        BaseUnitTest::TearDown();
    }
};

TEST_F(StatisticTaskQueueTest, TaskQueueEventsReachTheStatisticInterfaceAsAStatistic)
{
    const auto loadOutput = std::filesystem::temp_directory_path() / "task-queue-load.csv";
    std::filesystem::remove(loadOutput);

    /// The listener only runs when task statistics are enabled, and it publishes to the configured feed.
    SingleNodeWorkerConfiguration workerConfiguration;
    workerConfiguration.enableTaskStatistics.setValue(true);
    workerConfiguration.taskStatisticsFeed.setValue(FEED_NAME);

    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const auto workerCatalog = std::make_shared<WorkerCatalog>();
    const auto modelCatalog = std::make_shared<ModelCatalog>();
    workerCatalog->addWorker(TEST_HOST, "localhost:0", Capacity{CapacityKind::Unlimited{}}, {});

    /// The engine's own events, read back as a stream.
    const auto statsSource = sourceCatalog->addLogicalSource(Identifier::parse(STATS_SOURCE), engineStatsSchema());
    ASSERT_TRUE(statsSource.has_value());
    ASSERT_TRUE(sourceCatalog
                    ->addPhysicalSource(
                        *statsSource,
                        Identifier::parse("InProcess"),
                        TEST_HOST,
                        {{Identifier::parse("FEED_NAME"), FEED_NAME}},
                        {{Identifier::parse("type"), "CSV"}})
                    .has_value());

    /// Something for the engine to actually do, so that there are task events to observe.
    const auto loadSource = sourceCatalog->addLogicalSource(Identifier::parse(LOAD_SOURCE), loadSchema());
    ASSERT_TRUE(loadSource.has_value());
    ASSERT_TRUE(sourceCatalog
                    ->addPhysicalSource(
                        *loadSource,
                        Identifier::parse("Generator"),
                        TEST_HOST,
                        {{Identifier::parse("STOP_GENERATOR_WHEN_SEQUENCE_FINISHES"), "ALL"},
                         {Identifier::parse("MAX_RUNTIME_MS"), "20000"},
                         {Identifier::parse("GENERATOR_RATE_CONFIG"), "emit_rate 500"},
                         {Identifier::parse("SEED"), "1"},
                         {Identifier::parse("GENERATOR_SCHEMA"), "SEQUENCE UINT64 0 10000000 1"}},
                        {{Identifier::parse("type"), "CSV"}})
                    .has_value());

    const QueryOptimizer optimizer{QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog};
    SingleNodeWorker worker{workerConfiguration};

    const auto submitFn = [&optimizer, &worker](LogicalPlan plan) -> std::expected<QueryId, Exception>
    {
        const auto distributed = optimizer.optimize(std::move(plan));
        if (distributed.size() != 1 or distributed.begin()->second.size() != 1)
        {
            return std::unexpected(QueryStartFailed("expected exactly one local plan"));
        }
        return worker.startQuery(distributed.begin()->second.front());
    };

    StatisticInterface statisticInterface{std::make_unique<DefaultStatisticQueryGenerator>(), submitFn};
    ASSERT_FALSE(statisticInterface.startGrpcServer().empty());

    /// The load query. Started first so the statistic query has events to see from the outset.
    auto loadPlan = LogicalPlanBuilder::createLogicalPlan(Identifier::parse(LOAD_SOURCE));
    loadPlan = addFileSink(loadPlan, loadOutput);
    const auto loadQuery = submitFn(loadPlan);
    ASSERT_TRUE(loadQuery.has_value()) << loadQuery.error().what();

    /// Every closed window fires this, which is what "the events reached the statistic interface" means.
    std::atomic<int> reportsSeen{0};
    std::atomic<double> lastAverage{0.0};
    RequestStatisticBuildStatement statement{
        .domain = DataDomain{.logicalSourceName = STATS_SOURCE, .fieldName = "tuples"},
        .metric = Metric::Average,
        .windowType = Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{TASK_WINDOW_MS}}},
        /// Ingestion time, which is the default: ts_us is microseconds, and the engine's own clock is what
        /// matters here anyway.
        .conditionTrigger = withCallback(
            ALWAYS_SEND,
            [&reportsSeen, &lastAverage](StatisticTuple::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure, double value)
            {
                reportsSeen.fetch_add(1);
                lastAverage.store(value);
            }),
        .options = {}};

    const auto collected = statisticInterface.collectNewStatistic(statement);
    ASSERT_TRUE(collected.has_value()) << collected.error().what();

    /// Wait for the engine to produce events, a window to close, and the report to make it back over gRPC.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{60};
    while (reportsSeen.load() == 0 and std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
    }

    EXPECT_GT(reportsSeen.load(), 0) << "no statistic over the engine's task events reached the statistic interface";
    /// Task events carry a tuple count, so the average over them is a real number rather than a placeholder.
    EXPECT_GT(lastAverage.load(), 0.0) << "the reported average over the engine's task events was not a real value";

    const auto store = StatisticStoreRegistry::instance().getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});
    const auto statistics = store->getStatistics(collected->statisticId, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{~uint64_t{0}});
    EXPECT_FALSE(statistics.empty()) << "no statistic was persisted for the task-queue stream";
    for (const auto& statistic : statistics)
    {
        EXPECT_GT(statistic.getNumberOfSeenTuples(), 0U) << "a window closed without having seen any task event";
    }

    /// Neither query terminates on its own: the InProcess source never reaches end-of-stream.
    EXPECT_TRUE(worker.stopQuery(loadQuery.value()).has_value());
    EXPECT_TRUE(worker.stopQuery(collected->queryId).has_value());
}

}
