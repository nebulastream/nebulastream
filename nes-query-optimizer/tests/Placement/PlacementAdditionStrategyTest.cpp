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

#include <Placement/PlacementAdditionStrategy.hpp>

#include <ranges>
#include <string>
#include <vector>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Placement/PlacementRemovalStrategy.hpp>
#include <Placement/QueryDecomposition.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <DeploymentContext.hpp>
#include <GlobalExecutionPlan.hpp>
#include <IncrementalPlacementTestBase.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <SharedQueryPlan.hpp>

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace NES
{

using namespace ::testing;

class PlacementAdditionStrategyTest : public IncrementalPlacementTestBase
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PlacementAdditionStrategyTest.log", LogLevel::LOG_DEBUG); }

    struct Amendment
    {
        PlacementRemovalResult removal;
        PlacementAdditionResult addition;
    };

    Amendment amend(const PinnedOperators& pinned)
    {
        const auto version = globalExecutionPlan.getNextVersion();
        auto removal = PlacementRemovalStrategy(globalExecutionPlan, *cluster.workerCatalog, PlacementAmendmentMode::Optimistic)
                           .updateGlobalExecutionPlan(*sharedQueryPlan, pinned.upstreamOperators, pinned.downstreamOperators, version);
        const QueryDecomposer decomposer(copyPtr(cluster.workerCatalog), copyPtr(cluster.sourceCatalog), copyPtr(cluster.sinkCatalog));
        auto addition
            = PlacementAdditionStrategy(
                  globalExecutionPlan,
                  copyPtr(cluster.workerCatalog),
                  decomposer,
                  defaultConfiguration<QueryOptimizerConfiguration>().network,
                  PlacementAmendmentMode::Optimistic)
                  .updateGlobalExecutionPlan(*sharedQueryPlan, pinned.upstreamOperators, pinned.downstreamOperators, version, removal);
        return {.removal = std::move(removal), .addition = std::move(addition)};
    }

    [[nodiscard]] std::string dataAddress(const std::string& worker) const
    {
        return cluster.workerCatalog->getWorker(Host(worker))->dataAddress;
    }

    [[nodiscard]] std::vector<DeploymentContext> contextsOn(const PlacementAdditionResult& result, const std::string& worker) const
    {
        return result.deploymentContexts | std::views::values
            | std::views::filter([&](const auto& context) { return context.plan.worker == Host(worker); }) | std::ranges::to<std::vector>();
    }
};

/// The union moves from base1 to the cloud. A new plan hosts it, the plans of the sources send to it over new chains, and its result is
/// delivered over a channel on the cloud worker to the plan of the sink, which keeps running.
TEST_F(PlacementAdditionStrategyTest, PlacesReplacedOperatorAndConnectsIt)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk");
    const auto unionOperator = single<UnionLogicalOperator>();
    const auto sink = single<SinkLogicalOperator>();
    const auto sinkPlan = planOn("cloud:1");
    const auto leaf1Plan = planOn("leaf1:1");
    const auto leaf2Plan = planOn("leaf2:1");

    const auto [removal, addition] = amend(moveLeaf("leaf1:1"));

    EXPECT_THAT(sharedQueryPlan->getOperatorState(unionOperator), Eq(OperatorState::Placed));
    EXPECT_THAT(sharedQueryPlan->getPinnedWorker(unionOperator), ::testing::Optional(Host("cloud:1")));
    EXPECT_THAT(addition.deploymentContexts, SizeIs(5));

    /// The new plan of the union in the cloud.
    const auto cloudContexts = contextsOn(addition, "cloud:1");
    ASSERT_THAT(cloudContexts, SizeIs(1));
    EXPECT_THAT(cloudContexts.front().plan.state, Eq(DecomposedQueryPlanState::MarkedForDeployment));
    EXPECT_THAT(cloudContexts.front().plan.hostedOperators, ElementsAre(unionOperator));

    /// The source plans keep running with a new receiver.
    const auto& leaf1Update = addition.deploymentContexts.at(leaf1Plan);
    EXPECT_THAT(leaf1Update.plan.state, Eq(DecomposedQueryPlanState::MarkedForRedeployment));
    ASSERT_THAT(leaf1Update.sinkUpdates, SizeIs(1));
    EXPECT_THAT(
        leaf1Update.sinkUpdates.front().oldChannel, Eq(removal.removedFirstHops.at({sourceOn("leaf1:1"), unionOperator}).channel.id));
    EXPECT_THAT(leaf1Update.sinkUpdates.front().newDataEndpoint, Eq(dataAddress("base2:1")));
    EXPECT_THAT(addition.deploymentContexts.at(leaf2Plan).sinkUpdates.front().newDataEndpoint, Eq(dataAddress("base1:1")));

    /// Forwarding plans on base1 (for leaf2) and base2 (for leaf1).
    for (const auto* base : {"base1:1", "base2:1"})
    {
        const auto forwarding = contextsOn(addition, base);
        ASSERT_THAT(forwarding, SizeIs(1)) << base;
        EXPECT_THAT(forwarding.front().plan.state, Eq(DecomposedQueryPlanState::MarkedForDeployment));
        EXPECT_THAT(forwarding.front().plan.hostedOperators, IsEmpty());
    }

    /// The plan of the sink is not changed: the union attaches to its network source over the channel of the removed chain.
    EXPECT_FALSE(addition.deploymentContexts.contains(sinkPlan));
    const auto unionToSink = globalExecutionPlan.getChannelChain(queryId, unionOperator, sink);
    ASSERT_THAT(unionToSink, SizeIs(1));
    EXPECT_THAT(unionToSink.front().channel.id, Eq(removal.removedLastHops.at({unionOperator, sink}).channel.id));
    EXPECT_THAT(unionToSink.front().channel.upstreamWorker, Eq(Host("cloud:1")));
    EXPECT_THAT(unionToSink.front().downstreamPlan, Eq(sinkPlan));
    EXPECT_THAT(unionToSink.front().upstreamPlan, Eq(cloudContexts.front().plan.id));

    const auto leaf1ToUnion = globalExecutionPlan.getChannelChain(queryId, sourceOn("leaf1:1"), unionOperator);
    ASSERT_THAT(leaf1ToUnion, SizeIs(2));
    EXPECT_THAT(leaf1ToUnion.front().upstreamPlan, Eq(leaf1Plan));
    EXPECT_THAT(leaf1ToUnion.front().channel.id, Eq(leaf1Update.sinkUpdates.front().newChannel));
    EXPECT_THAT(leaf1ToUnion.back().downstreamPlan, Eq(cloudContexts.front().plan.id));
    EXPECT_THAT(globalExecutionPlan.getChannels(queryId), SizeIs(5));
}

/// Only the path changes: the plan of the source sends over a new forwarding plan on base2, whose sink attaches to the plan of the sink.
TEST_F(PlacementAdditionStrategyTest, ReconnectsPinnedOperatorsOverNewPath)
{
    deploy("SELECT * FROM src3 INTO snk");
    const auto sink = single<SinkLogicalOperator>();
    const auto sourcePlan = planOn("leaf3:1");
    const auto [removal, addition] = amend(moveLeaf("leaf3:1"));

    ASSERT_THAT(addition.deploymentContexts, SizeIs(2));
    const auto& update = addition.deploymentContexts.at(sourcePlan);
    ASSERT_THAT(update.sinkUpdates, SizeIs(1));
    EXPECT_THAT(update.sinkUpdates.front().newDataEndpoint, Eq(dataAddress("base2:1")));
    ASSERT_THAT(contextsOn(addition, "base2:1"), SizeIs(1));

    const auto chain = globalExecutionPlan.getChannelChain(queryId, sourceOn("leaf3:1"), sink);
    ASSERT_THAT(chain, SizeIs(2));
    EXPECT_THAT(chain.at(0).channel.downstreamWorker, Eq(Host("base2:1")));
    EXPECT_THAT(chain.at(1).channel.id, Eq(removal.removedLastHops.at({sourceOn("leaf3:1"), sink}).channel.id));
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
