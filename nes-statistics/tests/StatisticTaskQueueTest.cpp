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

/// The point of the whole port: the query engine's own task events, aggregated into a statistic, leaving the
/// worker as a report.
///
///   TaskStatisticListener -> task event feed -> EngineEvents source -> WindowedAggregation(Avg, COUNT(*))
///                         -> StatisticStoreWriter -> StatisticStoreReader -> GrpcSink -> report service
///
/// The plan is the one LogicalPlanBuilder::addStatisticBuild/addStatisticProbe produce, which is what the
/// coordinator's statistics crate generates as SQL. The report service is the recording stub, because the real
/// one is Rust and out of process.
///
/// A second query over a Generator source supplies the load, because the statistic query itself is excluded from
/// the feed -- the listener recognises a query that reads the event feed and drops its events, so a query
/// observing the engine cannot observe itself.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/StatisticIdentifiers.hpp>
#include <Operators/Statistic/StatisticBlobType.hpp>
#include <Operators/Statistic/StatisticFieldNames.hpp>
#include <Operators/Windows/Aggregations/AvgAggregationLogicalFunction.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Plans/LogicalPlanBuilder.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <HostPolicy.hpp>
#include <PlannerBridge.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticReportRecorder.hpp>
#include <StatisticTestSupport.hpp>
#include <TransactionalCatalog.hpp>

namespace NES
{
namespace
{

using namespace StatisticTestSupport;

constexpr auto STATS_SOURCE = "engineStats";
constexpr auto LOAD_SOURCE = "endless";

/// One statistic window. Short, because the test has to wait for at least one to close.
constexpr uint64_t TASK_WINDOW_MS = 500;

/// Exactly the columns TaskStatisticListener formats, in order.
constexpr auto ENGINE_STATS_COLUMNS = "event_type VARSIZED NOT NULL, ts_us UINT64 NOT NULL, thread_id UINT64 NOT NULL, "
                                      "query_id VARSIZED NOT NULL, pipeline_id UINT64 NOT NULL, task_id UINT64 NOT NULL, "
                                      "tuples UINT64 NOT NULL";

}

class StatisticTaskQueueTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticTaskQueueTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        globalStatisticStore().clear();
    }

    void TearDown() override
    {
        globalStatisticStore().clear();
        BaseUnitTest::TearDown();
    }
};

TEST_F(StatisticTaskQueueTest, TaskQueueEventsReachAStatisticReportService)
{
    const auto loadOutput = std::filesystem::temp_directory_path() / "task-queue-load.csv";
    std::filesystem::remove(loadOutput);

    TestStatisticServiceServer reportService;
    ASSERT_TRUE(reportService.isRunning()) << "could not start a test report service";
    ASSERT_NE(reportService.getPort(), 0U);

    /// The listener only runs when task statistics are enabled, and it publishes into this worker's own task
    /// event feed, which an EngineEvents source reads back.
    SingleNodeWorkerConfiguration workerConfiguration;
    workerConfiguration.enableTaskStatistics.setValue(true);

    auto context = create_test_planner_context();
    const auto seed = [&context](const std::string& statement)
    {
        const auto planned = plan_sql(context->context(), rust::Str{statement.data(), statement.size()}, rust::Str{}, rust::Str{});
        context->execute_seed_statement(planned.json);
    };
    seed(fmt::format("CREATE WORKER '{}' SET ('localhost:9090' AS DATA, 10 AS \"CAPACITY\");", TEST_HOST.getRawValue()));

    /// The engine's own events, read back as a stream.
    seed(fmt::format("CREATE LOGICAL SOURCE {} ({});", STATS_SOURCE, ENGINE_STATS_COLUMNS));
    seed(fmt::format(
        R"(CREATE PHYSICAL SOURCE FOR {} TYPE EngineEvents SET ('{}' AS "SOURCE"."HOST", 'CSV' AS INPUT_FORMATTER."TYPE");)",
        STATS_SOURCE,
        TEST_HOST.getRawValue()));

    /// Something for the engine to actually do, so that there are task events to observe.
    seed(fmt::format("CREATE LOGICAL SOURCE {} (ts UINT64 NOT NULL);", LOAD_SOURCE));
    seed(fmt::format(
        R"(CREATE PHYSICAL SOURCE FOR {} TYPE Generator SET ('{}' AS "SOURCE"."HOST", 'ALL' AS "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES, )"
        R"('20000' AS "SOURCE".MAX_RUNTIME_MS, 'emit_rate 500' AS "SOURCE".GENERATOR_RATE_CONFIG, 1 AS "SOURCE".SEED, )"
        R"('SEQUENCE UINT64 0 10000000 1' AS "SOURCE".GENERATOR_SCHEMA, 'CSV' AS INPUT_FORMATTER."TYPE");)",
        LOAD_SOURCE,
        TEST_HOST.getRawValue()));

    const auto catalog = std::make_shared<TransactionalCatalog>(context->context(), DefaultHost{std::string{TEST_HOST.getRawValue()}});

    const QueryOptimizer optimizer{QueryOptimizerConfiguration{}, catalog};
    SingleNodeWorker worker{workerConfiguration, TEST_HOST};

    uint64_t nextQueryId{1};
    const auto submit = [&optimizer, &worker, &nextQueryId](LogicalPlan plan) -> std::expected<QueryId, Exception>
    {
        const auto distributed = optimizer.optimize(std::move(plan));
        if (distributed.size() != 1 or distributed.begin()->second.size() != 1)
        {
            return std::unexpected(QueryStartFailed("expected exactly one local plan"));
        }
        auto localPlan = distributed.begin()->second.front();
        localPlan.setQueryId(QueryId{static_cast<QueryId::Underlying>(nextQueryId++)});
        return worker.startQuery(localPlan);
    };

    /// The load query. Started first so the statistic query has events to see from the outset.
    const auto loadQuery = submit(addFileSink(LogicalPlanBuilder::createLogicalPlan(Identifier::parse(LOAD_SOURCE)), loadOutput));
    ASSERT_TRUE(loadQuery.has_value()) << loadQuery.error().what();

    /// Ingestion time, which is what the statistics crate emits by default: ts_us is microseconds, and the
    /// engine's own clock is what matters here anyway.
    const TypedLogicalFunction<UnboundFieldAccessLogicalFunction> tuples{UnboundFieldAccessLogicalFunction{Identifier::parse("tuples")}};
    auto statisticPlan = LogicalPlanBuilder::addStatisticBuild(
        LogicalPlanBuilder::createLogicalPlan(Identifier::parse(STATS_SOURCE)),
        Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{TASK_WINDOW_MS}}},
        Windowing::TimeCharacteristic{Windowing::UnboundTimeCharacteristic{Windowing::IngestionTimeCharacteristic{}}},
        StatisticId{STATISTIC_ID},
        AvgAggregationLogicalFunction{tuples});
    statisticPlan = LogicalPlanBuilder::addStatisticProbe(
        std::move(statisticPlan),
        StatisticId{STATISTIC_ID},
        StatisticBlobType{AvgAggregationLogicalFunction::getName()},
        {{Identifier::parse(std::string{StatisticFieldNames::VALUE}),
          DataTypeProvider::provideDataType(DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE)}});
    const auto statisticQuery = submit(addGrpcSink(statisticPlan, reportService.getPort()));
    ASSERT_TRUE(statisticQuery.has_value()) << statisticQuery.error().what();

    /// Wait for the engine to produce events, a window to close, and the report to make it back over gRPC.
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{60};
    while (reportService.reports().empty() and std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
    }

    const auto reports = reportService.reports();
    ASSERT_FALSE(reports.empty()) << "no statistic over the engine's task events reached the report service";
    for (const auto& report : reports)
    {
        EXPECT_EQ(report.statistic_id(), STATISTIC_ID);
        /// Task events carry a tuple count, so the average over them is a real number rather than a placeholder.
        EXPECT_GT(report.value(), 0.0) << "the reported average over the engine's task events was not a real value";
    }

    const auto statistics = globalStatisticStore().getStatistics(StatisticId{STATISTIC_ID}, Timestamp{0}, Timestamp{~uint64_t{0}});
    EXPECT_FALSE(statistics.empty()) << "no statistic was persisted for the task-queue stream";
    for (const auto& statistic : statistics)
    {
        EXPECT_GT(statistic->getNumberOfSeenMeasurements(), 0U) << "a window closed without having seen any task event";
    }

    /// Neither query terminates on its own: the event feed never reaches end-of-stream.
    EXPECT_TRUE(worker.stopQuery(loadQuery.value()).has_value());
    EXPECT_TRUE(worker.stopQuery(statisticQuery.value()).has_value());
}

}
