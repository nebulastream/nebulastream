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

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <thread>
#include <utility>

#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
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

    StatisticCoordinator coordinator(
        std::make_unique<DefaultStatisticQueryGenerator>(),
        [&](LogicalPlan plan) -> std::expected<QueryId, Exception>
        {
            try
            {
                auto optimized = ruleOptimizer.optimize(semanticAnalyzer.analyse(std::move(plan)));
                auto queryId = worker.registerQuery(std::move(optimized));
                if (not queryId.has_value())
                {
                    return queryId;
                }
                if (auto started = worker.startQuery(queryId.value()); not started.has_value())
                {
                    return std::unexpected(started.error());
                }
                return queryId;
            }
            catch (const Exception& exception)
            {
                return std::unexpected(exception);
            }
        },
        [&worker](const QueryId queryId) { return blockingStop(worker, queryId); });

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

/// Same dataflow, but driven through the synchronous StatisticRetrievalService: a single blocking call starts
/// the ad-hoc build, starts the probe, waits for the value over the pipe, and returns the scalar.
TEST_F(StatisticCoordinatorE2ETest, AdHocScalarRetrievalViaService)
{
    constexpr uint64_t windowSizeMs = 1000;

    auto store = std::make_shared<DefaultStatisticStore>();
    SingleNodeWorker worker(SingleNodeWorkerConfiguration{}, Host("localhost"), store);

    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const auto modelCatalog = std::make_shared<ModelCatalog>();
    const SemanticAnalyzer semanticAnalyzer(sourceCatalog, sinkCatalog, modelCatalog);
    const RuleBasedOptimizer ruleOptimizer{QueryOptimizerConfiguration{}};

    StatisticCoordinator coordinator(
        std::make_unique<DefaultStatisticQueryGenerator>(),
        [&](LogicalPlan plan) -> std::expected<QueryId, Exception>
        {
            try
            {
                auto optimized = ruleOptimizer.optimize(semanticAnalyzer.analyse(std::move(plan)));
                auto queryId = worker.registerQuery(std::move(optimized));
                if (not queryId.has_value())
                {
                    return queryId;
                }
                if (auto started = worker.startQuery(queryId.value()); not started.has_value())
                {
                    return std::unexpected(started.error());
                }
                return queryId;
            }
            catch (const Exception& exception)
            {
                return std::unexpected(exception);
            }
        },
        [&worker](const QueryId queryId) { return blockingStop(worker, queryId); });
    coordinator.startResultReader();

    const StatisticRetrievalService retrievalService(coordinator);
    const RequestStatisticBuildStatement statement{
        .domain = DataDomain{.logicalSourceName = "adhocSource", .fieldName = "value"},
        .metric = Metric::Average,
        .windowSizeMs = windowSizeMs,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = std::nullopt,
        .conditionTrigger = std::nullopt,
        .options = {}};

    const auto value = retrievalService.retrieveStatistic(statement);

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
    constexpr uint64_t windowSizeMs = 1000;

    auto store = std::make_shared<DefaultStatisticStore>();
    SingleNodeWorker worker(SingleNodeWorkerConfiguration{}, Host("localhost"), store);

    const auto sourceCatalog = std::make_shared<SourceCatalog>();
    const auto sinkCatalog = std::make_shared<SinkCatalog>();
    const auto modelCatalog = std::make_shared<ModelCatalog>();
    const SemanticAnalyzer semanticAnalyzer(sourceCatalog, sinkCatalog, modelCatalog);
    const RuleBasedOptimizer ruleOptimizer{QueryOptimizerConfiguration{}};

    StatisticCoordinator coordinator(
        std::make_unique<DefaultStatisticQueryGenerator>(),
        [&](LogicalPlan plan) -> std::expected<QueryId, Exception>
        {
            try
            {
                auto optimized = ruleOptimizer.optimize(semanticAnalyzer.analyse(std::move(plan)));
                auto queryId = worker.registerQuery(std::move(optimized));
                if (not queryId.has_value())
                {
                    return queryId;
                }
                if (auto started = worker.startQuery(queryId.value()); not started.has_value())
                {
                    return std::unexpected(started.error());
                }
                return queryId;
            }
            catch (const Exception& exception)
            {
                return std::unexpected(exception);
            }
        },
        [&worker](const QueryId queryId) { return blockingStop(worker, queryId); });
    coordinator.startResultReader();

    StatisticRetrievalService retrievalService(coordinator);
    const RequestStatisticBuildStatement statement{
        .domain = DataDomain{.logicalSourceName = "ruleSource", .fieldName = "value"},
        .metric = Metric::Average,
        .windowSizeMs = windowSizeMs,
        .windowAdvanceMs = std::nullopt,
        .eventTimeFieldName = std::nullopt,
        .conditionTrigger = std::nullopt,
        .options = {}};

    /// A representative *user* query plan (one without statistic operators), so the rule's recursion guard does not
    /// skip it. The rule does not inspect the plan beyond that guard, so a bare source plan suffices.
    const LogicalPlan inputPlan = LogicalPlanBuilder::createLogicalPlan(Identifier::parse("userSource"));

    const StatisticOptimizationRule rule(retrievalService, statement);
    const LogicalPlan outputPlan = rule.apply(inputPlan);

    coordinator.stopResultReader();

    EXPECT_EQ(outputPlan, inputPlan) << "StatisticOptimizationRule must return the plan unmodified";
}

}
