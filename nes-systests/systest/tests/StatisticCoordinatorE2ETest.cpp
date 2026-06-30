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

/// End-to-end PoC test for the StatisticCoordinator dataflow:
///   build query (generator -> projection -> StatisticStoreWriter -> FileSink/FIFO) populates the shared store,
///   probe query (generator -> projection(const keys) -> StatisticStoreReader -> FileSink/FIFO) reads it back,
///   and the value travels over the named pipe to the coordinator's reader -> onStatisticReport.
/// The build query writes the constant value 42, so a successful round-trip returns 42.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryId.hpp>
#include <folly/Synchronized.h>
#include <Plans/LogicalPlanBuilder.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <WindowTypes/Measures/TimeMeasure.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <CollectionDomain.hpp>
#include <DefaultStatisticQueryGenerator.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <Metric.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Phases/SemanticAnalyzer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStatus.hpp>
#include <RequestStatisticStatement.hpp>
#include <Rules/Static/StatisticOptimizationRule.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <StatisticCoordinator.hpp>
#include <StatisticRegistry.hpp>
#include <StatisticRetrievalService.hpp>
#include <StatisticStore/DefaultStatisticStore.hpp>

namespace NES
{

namespace
{
/// Blocking stop used as the coordinator's StopQueryFn: stop the query, then poll until it actually reaches the
/// Stopped/Failed state (best-effort, with a timeout) so the caller knows the query has torn down.
std::expected<void, Exception> blockingStop(SingleNodeWorker& worker, const QueryId queryId)
{
    if (auto stopped = worker.stopQuery(queryId); not stopped.has_value())
    {
        return stopped;
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds{10};
    while (std::chrono::steady_clock::now() < deadline)
    {
        const auto status = worker.getQueryStatus(queryId);
        if (not status.has_value())
        {
            return std::unexpected(status.error());
        }
        if (status->state == QueryStatus::Stopped || status->state == QueryStatus::Failed)
        {
            return {};
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{20});
    }
    return {};
}

/// Bridges a bare SingleNodeWorker (low-level QueryId) to the coordinator's high-level DistributedQueryId callback
/// interface. Production deploys through the QueryManager and needs no such bridge; this id map exists only because
/// the unit harness drives a worker directly, one layer below the coordinator's deployment interface.
std::pair<StatisticCoordinator::SubmitQueryFn, StatisticCoordinator::StopQueryFn>
bareWorkerCallbacks(SingleNodeWorker& worker, const SemanticAnalyzer& semanticAnalyzer, const RuleBasedOptimizer& ruleOptimizer)
{
    auto ids = std::make_shared<folly::Synchronized<std::unordered_map<DistributedQueryId, QueryId>>>();
    auto counter = std::make_shared<std::atomic<uint64_t>>(0);

    StatisticCoordinator::SubmitQueryFn submitFn
        = [&worker, &semanticAnalyzer, &ruleOptimizer, ids, counter](LogicalPlan plan) -> std::expected<DistributedQueryId, Exception>
    {
        try
        {
            auto optimized = ruleOptimizer.optimize(semanticAnalyzer.analyse(std::move(plan)));
            auto queryId = worker.registerQuery(std::move(optimized));
            if (not queryId.has_value())
            {
                return std::unexpected(queryId.error());
            }
            if (auto started = worker.startQuery(queryId.value()); not started.has_value())
            {
                return std::unexpected(started.error());
            }
            const DistributedQueryId distributedId("stat-query-" + std::to_string(counter->fetch_add(1)));
            ids->wlock()->emplace(distributedId, queryId.value());
            return distributedId;
        }
        catch (const Exception& exception)
        {
            return std::unexpected(exception);
        }
    };

    StatisticCoordinator::StopQueryFn stopFn = [&worker, ids](const DistributedQueryId& distributedId) -> std::expected<void, Exception>
    {
        std::optional<QueryId> queryId;
        {
            const auto locked = ids->rlock();
            if (const auto it = locked->find(distributedId); it != locked->end())
            {
                queryId = it->second;
            }
        }
        if (not queryId.has_value())
        {
            return {};
        }
        return blockingStop(worker, queryId.value());
    };

    return {std::move(submitFn), std::move(stopFn)};
}
}

class StatisticCoordinatorE2ETest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("StatisticCoordinatorE2ETest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(StatisticCoordinatorE2ETest, BuildAndProbeRoundTripOverFifo)
{
    constexpr uint64_t windowSizeMs = 1000;

    /// The store is shared between the build and probe queries via constructor-DI into the worker's QueryCompiler.
    auto store = std::make_shared<DefaultStatisticStore>();
    SingleNodeWorker worker(SingleNodeWorkerConfiguration{}, Host("localhost"), store);

    /// Inline source/sink binding + type inference + trait decoration normally run inside the optimizer. The
    /// coordinator submits raw LogicalPlans, so we run the static optimizer phases here before registering with
    /// the worker (fresh empty catalogs suffice — the inline-binding rules register the inline source/sink).
    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const auto modelCatalog = std::make_shared<ModelCatalog>();
    const SemanticAnalyzer semanticAnalyzer(sourceCatalog, sinkCatalog, modelCatalog);
    const RuleBasedOptimizer ruleOptimizer{QueryOptimizerConfiguration{}};

    auto [submitFn, stopFn] = bareWorkerCallbacks(worker, semanticAnalyzer, ruleOptimizer);
    StatisticCoordinator coordinator(std::make_unique<DefaultStatisticQueryGenerator>(), std::move(submitFn), std::move(stopFn));

    coordinator.startResultReader();

    const DataDomain domain{.logicalSourceName = "statSource", .fieldName = "value"};
    const RequestStatisticBuildStatement statement{
        .domain = domain,
        .metric = Metric::MinVal,
        .windowSizeMs = windowSizeMs,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = std::nullopt,
        .conditionTrigger = std::nullopt,
        .options = {}};

    const auto collectResult = coordinator.collectNewStatistic(statement);
    if (not collectResult.has_value())
    {
        FAIL() << "collectNewStatistic failed: " << collectResult.error().what();
    }
    EXPECT_FALSE(collectResult->alreadyExisted);

    /// Give the build query time to compile, run, populate the store, and stream notifications into the FIFO.
    std::this_thread::sleep_for(std::chrono::seconds{3});

    const StatisticRegistry::Key key{
        .metric = Metric::MinVal, .collectionDomain = domain, .windowSize = Windowing::TimeMeasure{windowSizeMs}};

    const auto value = coordinator.getStatistics(key, Windowing::TimeMeasure{0}, Windowing::TimeMeasure{windowSizeMs});

    /// Stop the (otherwise continuous) build query so it does not linger into the next test.
    coordinator.stopStatistic(key);
    coordinator.stopResultReader();

    ASSERT_TRUE(value.has_value()) << "getStatistics returned no value (timed out waiting on the FIFO)";
    EXPECT_DOUBLE_EQ(value.value(), 42.0);
}

/// Same dataflow, but driven through the StatisticRetrievalService's "continuous build, ad-hoc probe" model:
/// deployStatisticBuild(source) spins up the (mock) build for a source, then retrieveStatistic(source) probes that
/// running build and returns the scalar. The build is NOT torn down by the probe — it keeps running until stopped.
TEST_F(StatisticCoordinatorE2ETest, AdHocScalarRetrievalViaService)
{
    const std::string statisticSource = "ADHOCSOURCE";

    auto store = std::make_shared<DefaultStatisticStore>();
    SingleNodeWorker worker(SingleNodeWorkerConfiguration{}, Host("localhost"), store);

    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const auto modelCatalog = std::make_shared<ModelCatalog>();
    const SemanticAnalyzer semanticAnalyzer(sourceCatalog, sinkCatalog, modelCatalog);
    const RuleBasedOptimizer ruleOptimizer{QueryOptimizerConfiguration{}};

    auto [submitFn, stopFn] = bareWorkerCallbacks(worker, semanticAnalyzer, ruleOptimizer);
    StatisticCoordinator coordinator(std::make_unique<DefaultStatisticQueryGenerator>(), std::move(submitFn), std::move(stopFn));
    coordinator.startResultReader();

    const StatisticRetrievalService retrievalService(coordinator);

    /// Deploy the continuous build for a source, then probe that source ad-hoc.
    retrievalService.deployStatisticBuild(statisticSource);
    /// Give the build query time to compile, run, and populate the store before we probe.
    std::this_thread::sleep_for(std::chrono::seconds{3});
    const auto value = retrievalService.retrieveStatistic(statisticSource);

    coordinator.stopResultReader();

    ASSERT_TRUE(value.has_value()) << "retrieveStatistic returned no value";
    EXPECT_DOUBLE_EQ(value.value(), 42.0);
}

/// Demonstrates statistics feeding a query-structuring decision: the StatisticOptimizationRule pulls a (mock)
/// statistic through the retrieval service while optimizing, prints it, and returns the plan unmodified (we have no
/// meaningful statistic to act on yet). This stands in for a future adaptive rewrite, e.g. filter reordering by
/// selectivity.
TEST_F(StatisticCoordinatorE2ETest, OptimizationRulePrintsStatisticAndLeavesPlanUnmodified)
{
    auto store = std::make_shared<DefaultStatisticStore>();
    SingleNodeWorker worker(SingleNodeWorkerConfiguration{}, Host("localhost"), store);

    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const auto modelCatalog = std::make_shared<ModelCatalog>();
    const SemanticAnalyzer semanticAnalyzer(sourceCatalog, sinkCatalog, modelCatalog);
    const RuleBasedOptimizer ruleOptimizer{QueryOptimizerConfiguration{}};

    auto [submitFn, stopFn] = bareWorkerCallbacks(worker, semanticAnalyzer, ruleOptimizer);
    StatisticCoordinator coordinator(std::make_unique<DefaultStatisticQueryGenerator>(), std::move(submitFn), std::move(stopFn));
    coordinator.startResultReader();

    auto retrievalService = std::make_shared<StatisticRetrievalService>(coordinator);

    /// Deploy the build for a source (as a GET_STATISTICS query on that source would), then let it populate the
    /// store. The rule is constructed to fetch that same source's statistic (as USE_STATISTIC=<source> would).
    const std::string statisticSource = "DEMOSOURCE";
    retrievalService->deployStatisticBuild(statisticSource);
    std::this_thread::sleep_for(std::chrono::seconds{3});

    /// A representative *user* query plan; the rule fetches the named statistic and leaves the plan unmodified.
    const LogicalPlan inputPlan = LogicalPlanBuilder::createLogicalPlan(Identifier::parse("userSource"));

    const StatisticOptimizationRule rule(retrievalService, statisticSource);
    const LogicalPlan outputPlan = rule.apply(inputPlan);

    coordinator.stopResultReader();

    EXPECT_EQ(outputPlan, inputPlan) << "StatisticOptimizationRule must return the plan unmodified";
}

/// Statistics are keyed by source: collecting on two distinct sources deploys two independent build queries, while
/// re-requesting a source already being collected reuses the running build (no duplicate). We use a lightweight
/// counting submit callback (no real worker) since we only care about how many builds are submitted.
TEST_F(StatisticCoordinatorE2ETest, EachSourceDeploysItsOwnBuild)
{
    std::atomic<int> submitCount{0};
    StatisticCoordinator::SubmitQueryFn countingSubmit = [&submitCount](LogicalPlan) -> std::expected<DistributedQueryId, Exception>
    { return DistributedQueryId("build-" + std::to_string(submitCount.fetch_add(1))); };

    StatisticCoordinator coordinator(std::make_unique<DefaultStatisticQueryGenerator>(), std::move(countingSubmit), nullptr);
    const StatisticRetrievalService retrievalService(coordinator);

    retrievalService.deployStatisticBuild("SOURCEA");
    retrievalService.deployStatisticBuild("SOURCEB");
    retrievalService.deployStatisticBuild("SOURCEA"); /// same source again -> deduplicated, no new build

    EXPECT_EQ(submitCount.load(), 2) << "two distinct sources must deploy two builds; the repeated source must not";
}

}
