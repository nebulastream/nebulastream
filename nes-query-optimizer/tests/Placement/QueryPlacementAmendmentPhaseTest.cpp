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

#include <Placement/QueryPlacementAmendmentPhase.hpp>

#include <algorithm>
#include <ranges>
#include <string>
#include <vector>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
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

namespace NES
{

using namespace ::testing;

class QueryPlacementAmendmentPhaseTest : public IncrementalPlacementTestBase
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("QueryPlacementAmendmentPhaseTest.log", LogLevel::LOG_DEBUG); }

    DeploymentUnit amend(const bool incremental)
    {
        const QueryDecomposer decomposer(copyPtr(cluster.workerCatalog), copyPtr(cluster.sourceCatalog), copyPtr(cluster.sinkCatalog));
        return QueryPlacementAmendmentPhase(
                   globalExecutionPlan,
                   copyPtr(cluster.workerCatalog),
                   decomposer,
                   defaultConfiguration<QueryOptimizerConfiguration>().network,
                   incremental,
                   PlacementAmendmentMode::Optimistic)
            .execute(*sharedQueryPlan);
    }

    static size_t countInState(const std::vector<DeploymentContext>& contexts, const DecomposedQueryPlanState state)
    {
        return std::ranges::count_if(contexts, [state](const auto& context) { return context.plan.state == state; });
    }
};

/// The reconfiguration marker is injected at the plans of both sources (Cond-1) and at the plan on base1, which leaf1 cannot reach
/// anymore (Cond-2).
TEST_F(QueryPlacementAmendmentPhaseTest, IncrementalAmendment)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk");
    const auto leaf1Plan = planOn("leaf1:1");
    const auto leaf2Plan = planOn("leaf2:1");
    const auto basePlan = planOn("base1:1");
    moveLeaf("leaf1:1");

    const auto unit = amend(true);
    EXPECT_THAT(sharedQueryPlan->getStatus(), Eq(SharedQueryPlanStatus::Processed));
    EXPECT_THAT(
        unit.reconfigurationMarkerUnits,
        UnorderedElementsAre(
            ReconfigurationMarkerUnit{.worker = Host("leaf1:1"), .plan = leaf1Plan},
            ReconfigurationMarkerUnit{.worker = Host("leaf2:1"), .plan = leaf2Plan},
            ReconfigurationMarkerUnit{.worker = Host("base1:1"), .plan = basePlan}));
    EXPECT_THAT(unit.deploymentRemovalContexts, IsEmpty());
    EXPECT_THAT(countInState(unit.deploymentAdditionContexts, DecomposedQueryPlanState::MarkedForMigration), Eq(1U));
    EXPECT_THAT(countInState(unit.deploymentAdditionContexts, DecomposedQueryPlanState::MarkedForRedeployment), Eq(2U));
    EXPECT_THAT(countInState(unit.deploymentAdditionContexts, DecomposedQueryPlanState::MarkedForDeployment), Eq(3U));
    EXPECT_THAT(unit.stateMigrations, IsEmpty());
    EXPECT_THAT(sharedQueryPlan->getChangeLogEntries(sharedQueryPlan->getCurrentChangeLogTimestamp()), IsEmpty());
}

/// The selection on base1 stays, but the union in the same plan moves. The selection is re-placed as well, so the plan is drained as a
/// whole, and the source of the selection gets pinned instead.
TEST_F(QueryPlacementAmendmentPhaseTest, PartiallyAffectedPlanIsReplacedAsAWhole)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 WHERE a > b INTO snk");
    const auto selection = single<SelectionLogicalOperator>();
    moveLeaf("leaf1:1");

    const auto unit = amend(true);
    EXPECT_THAT(sharedQueryPlan->getStatus(), Eq(SharedQueryPlanStatus::Processed));
    EXPECT_THAT(sharedQueryPlan->getOperatorState(selection), Eq(OperatorState::Placed));
    EXPECT_THAT(countInState(unit.deploymentAdditionContexts, DecomposedQueryPlanState::MarkedForMigration), Eq(1U));
    EXPECT_THAT(unit.reconfigurationMarkerUnits, Contains(ReconfigurationMarkerUnit{.worker = Host("leaf2:1"), .plan = planOn("leaf2:1")}));
}

/// The holistic baseline removes all plans and deploys all plans of the newly placed query.
TEST_F(QueryPlacementAmendmentPhaseTest, HolisticAmendment)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk");
    const auto oldPlans = globalExecutionPlan.getDecomposedQueryPlans(queryId);
    moveLeaf("leaf1:1");

    const auto unit = amend(false);
    EXPECT_THAT(unit.deploymentRemovalContexts, SizeIs(oldPlans.size()));
    EXPECT_THAT(countInState(unit.deploymentRemovalContexts, DecomposedQueryPlanState::MarkedForMigration), Eq(oldPlans.size()));
    EXPECT_THAT(unit.deploymentAdditionContexts, SizeIs(globalExecutionPlan.getDecomposedQueryPlans(queryId).size()));
    EXPECT_THAT(
        countInState(unit.deploymentAdditionContexts, DecomposedQueryPlanState::MarkedForDeployment),
        Eq(unit.deploymentAdditionContexts.size()));
    EXPECT_THAT(unit.reconfigurationMarkerUnits, IsEmpty());
    EXPECT_THAT(sharedQueryPlan->getPinnedWorker(single<UnionLogicalOperator>()), ::testing::Optional(Host("cloud:1")));
    for (const auto& context : unit.deploymentAdditionContexts)
    {
        EXPECT_THAT(std::ranges::count(oldPlans, context.plan.id, &DecomposedQueryPlan::id), Eq(0));
    }
}

/// A join placed on base1 moves to the cloud when leaf1 moves to base2. Its old plan hands its state over (MarkedForUpdateAndDrain).
class StatefulAmendmentTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("StatefulAmendmentTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(StatefulAmendmentTest, MovedJoinGetsStateMigration)
{
    DistributedTestCluster cluster;
    cluster.addWorker("cloud:1", {});
    cluster.addWorker("base1:1", {"cloud:1"}, 3);
    cluster.addWorker("base2:1", {"cloud:1"}, 3);
    cluster.addWorker("leaf1:1", {"base1:1"}, 0);
    cluster.addWorker("leaf2:1", {"base1:1"}, 0);
    cluster.addSource("stream0", {"ts0", "id0"}, "leaf1:1");
    cluster.addSource("stream1", {"ts1", "id1"}, "leaf2:1");
    cluster.addSink("snk", {"start", "end", "ts0", "id0", "ts1", "id1"}, "cloud:1");
    const auto plan = cluster.optimize(
        "SELECT * FROM (SELECT * FROM stream0) INNER JOIN (SELECT * FROM stream1) ON id0 = id1 WINDOW TUMBLING (ts0, ts1, size 1 sec) INTO "
        "snk");

    const DistributedQueryId queryId{"q"};
    GlobalExecutionPlan globalExecutionPlan;
    globalExecutionPlan.addQuery(queryId, plan);
    SharedQueryPlan sharedQueryPlan(queryId, plan.getGlobalPlan(), getPlacement(plan.getGlobalPlan()));
    for (const auto& operatorId : sharedQueryPlan.getGraph().getAllOperators())
    {
        cluster.workerCatalog->occupySlots(
            *sharedQueryPlan.getPinnedWorker(operatorId), getOperatorCapacityDemand(sharedQueryPlan.getGraph().getOperator(operatorId)));
    }
    const auto join = getOperatorByType<JoinLogicalOperator>(plan.getGlobalPlan()).front().getId();
    ASSERT_THAT(sharedQueryPlan.getPinnedWorker(join), ::testing::Optional(Host("base1:1")));
    const auto oldJoinPlan = globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, join)->id;

    ASSERT_TRUE(cluster.workerCatalog->removeLink(Host("leaf1:1"), Host("base1:1")));
    cluster.workerCatalog->addLink(Host("leaf1:1"), Host("base2:1"));
    const auto pinned = findUpstreamAndDownstreamPinnedOperators(
        sharedQueryPlan, globalExecutionPlan, Host("leaf1:1"), Host("base1:1"), cluster.workerCatalog->getTopology());
    sharedQueryPlan.performReOperatorPlacement(pinned.upstreamOperators, pinned.downstreamOperators);

    const QueryDecomposer decomposer(copyPtr(cluster.workerCatalog), copyPtr(cluster.sourceCatalog), copyPtr(cluster.sinkCatalog));
    const auto unit = QueryPlacementAmendmentPhase(
                          globalExecutionPlan,
                          copyPtr(cluster.workerCatalog),
                          decomposer,
                          defaultConfiguration<QueryOptimizerConfiguration>().network,
                          true,
                          PlacementAmendmentMode::Optimistic)
                          .execute(sharedQueryPlan);

    EXPECT_THAT(sharedQueryPlan.getStatus(), Eq(SharedQueryPlanStatus::Processed));
    ASSERT_THAT(unit.stateMigrations, SizeIs(1));
    const auto& migration = unit.stateMigrations.front();
    EXPECT_THAT(migration.operatorId, Eq(join));
    EXPECT_THAT(migration.oldWorker, Eq(Host("base1:1")));
    EXPECT_THAT(migration.oldPlan, Eq(oldJoinPlan));
    EXPECT_THAT(migration.newWorker, Eq(Host("cloud:1")));
    EXPECT_THAT(globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, join)->id, Eq(migration.newPlan));
    const auto oldContext
        = std::ranges::find(unit.deploymentAdditionContexts, oldJoinPlan, [](const auto& context) { return context.plan.id; });
    ASSERT_NE(oldContext, unit.deploymentAdditionContexts.end());
    EXPECT_THAT(oldContext->plan.state, Eq(DecomposedQueryPlanState::MarkedForUpdateAndDrain));
}

}
