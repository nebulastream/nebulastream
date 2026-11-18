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

#include <Placement/PlacementRemovalStrategy.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <IncrementalPlacementTestBase.hpp>
#include <SharedQueryPlan.hpp>

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace NES
{

using namespace ::testing;

class PlacementRemovalStrategyTest : public IncrementalPlacementTestBase
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("PlacementRemovalStrategyTest.log", LogLevel::LOG_DEBUG); }
};

/// The union on base1 is no longer reachable from leaf1, so its plan is drained and its channels are removed. The plans of the sources
/// keep running and get a new sink, the plan of the sink keeps running and keeps its network source.
TEST_F(PlacementRemovalStrategyTest, DrainsPlanOfReplacedOperator)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk");
    const auto unionOperator = single<UnionLogicalOperator>();
    const auto sink = single<SinkLogicalOperator>();
    const auto basePlan = planOn("base1:1");
    ASSERT_THAT(cluster.workerCatalog->getOccupiedSlots(Host("base1:1")), Eq(1U));
    const auto unionToSink = globalExecutionPlan.getChannelChain(queryId, unionOperator, sink).front();

    const auto pinned = moveLeaf("leaf1:1");
    const auto version = globalExecutionPlan.getNextVersion();
    const auto result = PlacementRemovalStrategy(globalExecutionPlan, *cluster.workerCatalog)
                            .updateGlobalExecutionPlan(*sharedQueryPlan, pinned.upstreamOperators, pinned.downstreamOperators, version);

    ASSERT_THAT(result.deploymentContexts, SizeIs(1));
    const auto& context = result.deploymentContexts.at(basePlan);
    EXPECT_THAT(context.plan.state, Eq(DecomposedQueryPlanState::MarkedForMigration));
    EXPECT_THAT(context.plan.version, Eq(version));
    EXPECT_THAT(context.oldVersion, ::testing::Optional(Lt(version)));
    EXPECT_THAT(context.detachedChannels, ElementsAre(unionToSink.channel.id));

    EXPECT_THAT(
        result.removedFirstHops,
        UnorderedElementsAre(Key(LogicalEdge{sourceOn("leaf1:1"), unionOperator}), Key(LogicalEdge{sourceOn("leaf2:1"), unionOperator})));
    EXPECT_THAT(result.removedFirstHops.at({sourceOn("leaf1:1"), unionOperator}).upstreamPlan, Eq(planOn("leaf1:1")));
    EXPECT_THAT(result.removedLastHops, ElementsAre(Pair(LogicalEdge{unionOperator, sink}, unionToSink)));

    EXPECT_THAT(globalExecutionPlan.getChannels(queryId), IsEmpty());
    EXPECT_THAT(globalExecutionPlan.getDecomposedQueryPlan(queryId, basePlan)->state, Eq(DecomposedQueryPlanState::MarkedForMigration));
    EXPECT_THAT(sharedQueryPlan->getOperatorState(unionOperator), Eq(OperatorState::ToBePlaced));
    EXPECT_THAT(sharedQueryPlan->getPinnedWorker(unionOperator), Eq(std::nullopt));
    EXPECT_THAT(sharedQueryPlan->getPinnedWorker(sourceOn("leaf1:1")), ::testing::Optional(Host("leaf1:1")));
    EXPECT_THAT(cluster.workerCatalog->getOccupiedSlots(Host("base1:1")), Eq(0U));
}

/// Only the path changes: the forwarding plan on base1 is drained and detaches from the plan of the sink, which keeps running.
TEST_F(PlacementRemovalStrategyTest, DrainsForwardingPlanOfRemovedLink)
{
    deploy("SELECT * FROM src3 INTO snk");
    const auto sink = single<SinkLogicalOperator>();
    const auto chain = globalExecutionPlan.getChannelChain(queryId, sourceOn("leaf3:1"), sink);
    ASSERT_THAT(chain, SizeIs(2));

    const auto pinned = moveLeaf("leaf3:1");
    EXPECT_THAT(pinned.upstreamOperators, ElementsAre(sourceOn("leaf3:1")));
    EXPECT_THAT(pinned.downstreamOperators, ElementsAre(sink));
    const auto result
        = PlacementRemovalStrategy(globalExecutionPlan, *cluster.workerCatalog)
              .updateGlobalExecutionPlan(
                  *sharedQueryPlan, pinned.upstreamOperators, pinned.downstreamOperators, globalExecutionPlan.getNextVersion());

    ASSERT_THAT(result.deploymentContexts, SizeIs(1));
    EXPECT_THAT(result.deploymentContexts.at(planOn("base1:1")).detachedChannels, ElementsAre(chain.at(1).channel.id));
    EXPECT_THAT(result.removedFirstHops, ElementsAre(Pair(LogicalEdge{sourceOn("leaf3:1"), sink}, chain.at(0))));
    EXPECT_THAT(result.removedLastHops, ElementsAre(Pair(LogicalEdge{sourceOn("leaf3:1"), sink}, chain.at(1))));
}

/// The selection on base1 stays, but the union in the same plan is re-placed. A running plan cannot be changed, so this fails.
TEST_F(PlacementRemovalStrategyTest, RejectsPartiallyAffectedPlan)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 WHERE a > b INTO snk");
    ASSERT_THAT(sharedQueryPlan->getPinnedWorker(single<SelectionLogicalOperator>()), ::testing::Optional(Host("base1:1")));
    ASSERT_THAT(sharedQueryPlan->getPinnedWorker(single<UnionLogicalOperator>()), ::testing::Optional(Host("base1:1")));

    const auto pinned = moveLeaf("leaf1:1");
    EXPECT_THROW(
        (void)PlacementRemovalStrategy(globalExecutionPlan, *cluster.workerCatalog)
            .updateGlobalExecutionPlan(
                *sharedQueryPlan, pinned.upstreamOperators, pinned.downstreamOperators, globalExecutionPlan.getNextVersion()),
        Exception);
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
