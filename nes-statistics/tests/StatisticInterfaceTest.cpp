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

/// The statistic interface driving the whole thing: collectNewStatistic deploys a build query that reports back over
/// gRPC, and getStatistics deploys a probe query, impulses it, and collects the answers.
///
/// Unlike the other suites this uses a named logical source, because that is what a real request names, so it
/// registers one in a SourceCatalog and hands the same catalog to the optimizer.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ComparisonFunctions/GreaterLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/UnboundFieldAccessLogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Operators/Sinks/AnonymousSinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StatisticStore/StatisticStoreRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <WindowTypes/Measures/TimeCharacteristic.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <WindowTypes/Types/TimeBasedWindowType.hpp>
#include <WindowTypes/Types/TumblingWindow.hpp>
#include <fmt/format.h>
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
#include <QueryStatus.hpp>
#include <RequestStatisticStatement.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <StatisticInterface.hpp>
#include <StatisticRegistry.hpp>
#include <StatisticTestSupport.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{

using namespace StatisticTestSupport;

constexpr auto SOURCE_NAME = "teststream";

/// Owns the worker, the catalogs and the optimizer, and exposes the SubmitQueryFn the statistic interface needs.
///
/// Query ids are recorded so a test can wait for the build query to finish before probing: the probe reads what
/// the build wrote, so the two must not overlap.
class TestSubmissionBackend
{
public:
    explicit TestSubmissionBackend(const std::filesystem::path& inputPath)
        : sourceCatalog(std::make_shared<SourceCatalog>())
        , sinkCatalog(std::make_shared<SinkCatalog>())
        , workerCatalog(std::make_shared<WorkerCatalog>())
        , modelCatalog(std::make_shared<ModelCatalog>())
    {
        workerCatalog->addWorker(TEST_HOST, "localhost:0", Capacity{CapacityKind::Unlimited{}}, {});

        const auto logicalSource = sourceCatalog->addLogicalSource(Identifier::parse(SOURCE_NAME), inputSchema());
        if (logicalSource.has_value())
        {
            const auto physical = sourceCatalog->addPhysicalSource(
                *logicalSource,
                Identifier::parse("File"),
                TEST_HOST,
                {{Identifier::parse("FILE_PATH"), inputPath.string()}},
                {{Identifier::parse("type"), "CSV"}});
            (void)physical;
        }

        optimizer
            = std::make_unique<QueryOptimizer>(QueryOptimizerConfiguration{}, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);
        worker = std::make_unique<SingleNodeWorker>(SingleNodeWorkerConfiguration{});
    }

    [[nodiscard]] StatisticInterface::SubmitQueryFn submitFn()
    {
        return [this](LogicalPlan plan) -> std::expected<QueryId, Exception>
        {
            const auto distributed = optimizer->optimize(std::move(plan));
            if (distributed.size() != 1)
            {
                return std::unexpected(QueryStartFailed("expected exactly one local plan, got {}", distributed.size()));
            }
            const auto& localPlans = distributed.begin()->second;
            if (localPlans.size() != 1)
            {
                return std::unexpected(QueryStartFailed("expected exactly one local plan for the worker"));
            }
            auto queryId = worker->startQuery(localPlans.front());
            if (queryId.has_value())
            {
                const std::lock_guard lock(mutex);
                submitted.push_back(queryId.value());
            }
            return queryId;
        };
    }

    /// Waits until every query submitted so far has stopped. The build query reads a file, so it terminates.
    [[nodiscard]] bool waitForAllStopped(const std::chrono::milliseconds timeout) const
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            bool allStopped = true;
            {
                const std::lock_guard lock(mutex);
                for (const auto& queryId : submitted)
                {
                    const auto status = worker->getQueryStatus(queryId);
                    if (not status.has_value() or status.value().state != QueryStatus::Stopped)
                    {
                        allStopped = false;
                        break;
                    }
                }
            }
            if (allStopped)
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{50});
        }
        return false;
    }

private:
    std::shared_ptr<SourceCatalog> sourceCatalog;
    std::shared_ptr<SinkCatalog> sinkCatalog;
    std::shared_ptr<WorkerCatalog> workerCatalog;
    std::shared_ptr<ModelCatalog> modelCatalog;
    std::unique_ptr<QueryOptimizer> optimizer;
    std::unique_ptr<SingleNodeWorker> worker;

    mutable std::mutex mutex;
    std::vector<QueryId> submitted;
};

RequestStatisticBuildStatement averageOverValue()
{
    return RequestStatisticBuildStatement{
        .domain = DataDomain{.logicalSourceName = SOURCE_NAME, .fieldName = "value"},
        .metric = Metric::Average,
        .windowType = Windowing::TimeBasedWindowType{Windowing::TumblingWindow{Windowing::TimeMeasure{WINDOW_SIZE_MS}}},
        .timeCharacteristic
        = Windowing::TimeCharacteristic{Windowing::UnboundTimeCharacteristic{Windowing::TimeCharacteristicWrapper::createEventTime(
            UnboundFieldAccessLogicalFunction{Identifier::parse("ts")}, Windowing::TimeUnit::Milliseconds())}},
        .conditionTrigger = NEVER_SEND,
        .options = {}};
}

/// The sink's type is not in the explain output before optimization -- an unoptimized plan just says
/// ANONYMOUS_SINK -- so read it off the operator instead.
std::string sinkTypeOf(const LogicalPlan& plan)
{
    const auto sink = plan.getRootOperators().front().tryGetAs<AnonymousSinkLogicalOperator>();
    return sink.has_value() ? (*sink)->getSinkType().asCanonicalString() : std::string{"<not an anonymous sink>"};
}

StatisticRegistry::Key keyFor(const RequestStatisticBuildStatement& statement)
{
    return StatisticRegistry::Key{
        .metric = statement.metric, .collectionDomain = statement.domain, .windowSize = statement.windowType.getSize()};
}

}

class StatisticInterfaceTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase() { Logger::setupLogging("StatisticInterfaceTest.log", LogLevel::LOG_DEBUG); }

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

/// The build half through the statistic interface: a request turns into a deployed query whose statistics land in the
/// store. Under NEVER_SEND the query reports nothing -- see PlanShapeDependsOnTheTrigger -- so the store is the
/// only observable effect, which is exactly what getStatistics later reads.
TEST_F(StatisticInterfaceTest, CollectNewStatisticUnderNeverSendStillPersists)
{
    const auto inputPath = writeInput("statisticInterface-collect-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticInterface statisticInterface{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    const auto address = statisticInterface.startGrpcServer();
    ASSERT_FALSE(address.empty());

    const auto statement = averageOverValue();
    const auto result = statisticInterface.collectNewStatistic(statement);
    ASSERT_TRUE(result.has_value()) << result.error().what();
    EXPECT_FALSE(result->alreadyExisted);

    ASSERT_TRUE(backend.waitForAllStopped(std::chrono::seconds{60})) << "the build query never finished";

    const auto store = StatisticStoreRegistry::instance().getOrCreate(std::string{StatisticStoreRegistry::DEFAULT_STORE_NAME});
    const auto statistics
        = store->getStatistics(result->statisticId, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{WINDOW_SIZE_MS * 10});
    EXPECT_EQ(statistics.size(), 2U) << "expected one statistic per closed window";
}

/// A second identical request must not deploy a second query.
TEST_F(StatisticInterfaceTest, IdenticalRequestsAreDeduplicated)
{
    const auto inputPath = writeInput("statisticInterface-dedup-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticInterface statisticInterface{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    statisticInterface.startGrpcServer();

    const auto statement = averageOverValue();
    const auto first = statisticInterface.collectNewStatistic(statement);
    ASSERT_TRUE(first.has_value()) << first.error().what();
    const auto second = statisticInterface.collectNewStatistic(statement);
    ASSERT_TRUE(second.has_value()) << second.error().what();

    EXPECT_TRUE(second->alreadyExisted);
    EXPECT_EQ(first->statisticId, second->statisticId);
    EXPECT_EQ(first->queryId, second->queryId);

    /// Let the query finish before the backend, and with it the worker, goes out of scope. Tearing a worker down
    /// while a pipeline is still being compiled trips CompiledExecutablePipelineStage's invariant that start()
    /// runs before execute(), which made this test abort roughly one run in three.
    EXPECT_TRUE(backend.waitForAllStopped(std::chrono::seconds{60})) << "the build query never finished";
}

/// The read half through the statistic interface: after a statistic has been built, getStatistics deploys the probe,
/// impulses it, and returns what the probe read back out of the store. The two windows average 20 and 200, so a
/// correct sum is 220.
TEST_F(StatisticInterfaceTest, GetStatisticsProbesTheCollectedStatistic)
{
    const auto inputPath = writeInput("statisticInterface-probe-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticInterface statisticInterface{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    statisticInterface.startGrpcServer();

    const auto statement = averageOverValue();
    const auto collected = statisticInterface.collectNewStatistic(statement);
    ASSERT_TRUE(collected.has_value()) << collected.error().what();

    /// The probe reads what the build wrote, so the build has to be done first.
    ASSERT_TRUE(backend.waitForAllStopped(std::chrono::seconds{60})) << "the build query never finished";

    const auto probed
        = statisticInterface.getStatistics({keyFor(statement)}, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{WINDOW_SIZE_MS * 10});

    ASSERT_TRUE(probed.has_value()) << "no report came back before the timeout";
    EXPECT_DOUBLE_EQ(probed.value(), 220.0) << "expected the sum of both windows' averages";
}

/// The domains and metrics outside this port's slice keep their interface and fail cleanly.
TEST_F(StatisticInterfaceTest, UnsupportedRequestsFailWithNotImplemented)
{
    const auto inputPath = writeInput("statisticInterface-unsupported-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticInterface statisticInterface{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    statisticInterface.startGrpcServer();

    auto infrastructure = averageOverValue();
    infrastructure.domain = InfrastructureDomain{Host{"worker1"}};
    const auto infrastructureResult = statisticInterface.collectNewStatistic(infrastructure);
    ASSERT_FALSE(infrastructureResult.has_value());
    EXPECT_EQ(infrastructureResult.error().code(), ErrorCode::NotImplemented);

    auto workload = averageOverValue();
    workload.domain = WorkloadDomain{.queryId = QueryId::invalid(), .operatorId = OperatorId{1}, .fieldName = "value"};
    const auto workloadResult = statisticInterface.collectNewStatistic(workload);
    ASSERT_FALSE(workloadResult.has_value());
    EXPECT_EQ(workloadResult.error().code(), ErrorCode::NotImplemented);

    auto unsupportedMetric = averageOverValue();
    unsupportedMetric.metric = Metric::Cardinality;
    const auto metricResult = statisticInterface.collectNewStatistic(unsupportedMetric);
    ASSERT_FALSE(metricResult.has_value());
    EXPECT_EQ(metricResult.error().code(), ErrorCode::NotImplemented);
}

/// A trigger carrying a predicate compiles a store read into the build query itself, so that only the windows
/// satisfying it are reported. The two windows average 20 and 200, so "> 100" must fire exactly once -- if the
/// probe or the selection were missing this would fire twice, or not at all.
TEST_F(StatisticInterfaceTest, AConditionalTriggerFiresOnlyForMatchingWindows)
{
    const auto inputPath = writeInput("statisticInterface-conditional-input.csv");
    TestSubmissionBackend backend{inputPath};

    StatisticInterface statisticInterface{std::make_unique<DefaultStatisticQueryGenerator>(), backend.submitFn()};
    statisticInterface.startGrpcServer();

    std::atomic<int> fired{0};
    std::atomic<double> lastValue{0.0};
    auto statement = averageOverValue();
    statement.conditionTrigger = ConditionTrigger{
        .condition = fmt::format("{} > 100.0", StatisticFieldNames::VALUE),
        .callback = [&fired, &lastValue](StatisticTuple::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure, double value)
        {
            fired.fetch_add(1);
            lastValue.store(value);
        }};

    const auto collected = statisticInterface.collectNewStatistic(statement);
    ASSERT_TRUE(collected.has_value()) << collected.error().what();
    ASSERT_TRUE(backend.waitForAllStopped(std::chrono::seconds{60})) << "the build query never finished";

    /// The reports are sent from the sink as the query drains, so give the last one a moment to land.
    std::this_thread::sleep_for(std::chrono::seconds{1});

    EXPECT_EQ(fired.load(), 1) << "only the window averaging 200 should have passed the predicate";
    /// And the callback is handed the value itself, not just the window it covers.
    EXPECT_DOUBLE_EQ(lastValue.load(), 200.0);
}

/// What the generator emits is decided entirely by the trigger, so it is worth pinning directly rather than
/// inferring it from runtime effects -- especially "no report is sent", which is otherwise an absence of
/// evidence.
TEST_F(StatisticInterfaceTest, PlanShapeDependsOnTheTrigger)
{
    const DefaultStatisticQueryGenerator generator;
    const std::string address = "localhost:1234";

    /// NEVER_SEND: nothing would consume a report, so the query terminates in a VoidSink and never touches the
    /// network. The statistic is still written -- the writer is fused into the aggregation, below the sink.
    const auto neverSendPlan = generator.generateQuery(averageOverValue(), StatisticTuple::StatisticId{1}, address);
    EXPECT_EQ(sinkTypeOf(neverSendPlan), "VOID");
    EXPECT_EQ(explain(neverSendPlan, ExplainVerbosity::Debug).find("SCALARSTATISTICPROBE"), std::string::npos);

    /// ALWAYS_SEND still needs the store read, because the callback is handed the value. What it does not get is
    /// a Selection: every closed window is reported.
    auto callbackOnly = averageOverValue();
    callbackOnly.conditionTrigger
        = withCallback(ALWAYS_SEND, [](StatisticTuple::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure, double) { });
    const auto callbackPlan = generator.generateQuery(callbackOnly, StatisticTuple::StatisticId{1}, address);
    const auto callbackExplain = explain(callbackPlan, ExplainVerbosity::Debug);
    EXPECT_EQ(sinkTypeOf(callbackPlan), "GRPC");
    EXPECT_NE(callbackExplain.find("SCALARSTATISTICPROBE"), std::string::npos) << callbackExplain;
    EXPECT_EQ(callbackExplain.find("SELECTION"), std::string::npos) << callbackExplain;

    /// A predicate is evaluated over the value, which only the store has, so this one reads its own writes back.
    auto withPredicate = averageOverValue();
    withPredicate.conditionTrigger = ConditionTrigger{
        .condition = fmt::format("{} > 100.0", StatisticFieldNames::VALUE),
        .callback = [](StatisticTuple::StatisticId, Windowing::TimeMeasure, Windowing::TimeMeasure, double) { }};
    const auto predicatePlan = generator.generateQuery(withPredicate, StatisticTuple::StatisticId{1}, address);
    const auto predicateExplain = explain(predicatePlan, ExplainVerbosity::Debug);
    EXPECT_EQ(sinkTypeOf(predicatePlan), "GRPC");
    EXPECT_NE(predicateExplain.find("SCALARSTATISTICPROBE"), std::string::npos) << predicateExplain;
    EXPECT_NE(predicateExplain.find("SELECTION"), std::string::npos) << predicateExplain;
}

}
