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

#include <GlobalExecutionPlan.hpp>

#include <optional>
#include <Identifiers/Identifiers.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedTestCluster.hpp>
#include <QueryId.hpp>

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
namespace NES
{

using namespace ::testing;

class GlobalExecutionPlanTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("GlobalExecutionPlanTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        cluster.addWorker("cloud:1", {});
        cluster.addWorker("base:1", {"cloud:1"});
        cluster.addWorker("leaf:1", {"base:1"});
        cluster.addSource("src", {"a", "b"}, "leaf:1");
        cluster.addSink("snk", {"a", "b"}, "cloud:1");
    }

    DistributedTestCluster cluster;
    const DistributedQueryId queryId{"q"};
};

TEST_F(GlobalExecutionPlanTest, AddQueryCreatesOnePlanPerLocalPlan)
{
    const auto plan = cluster.optimize("SELECT * FROM src WHERE a > b INTO snk");
    GlobalExecutionPlan globalExecutionPlan;
    const auto planIds = globalExecutionPlan.addQuery(queryId, plan);

    EXPECT_TRUE(globalExecutionPlan.contains(queryId));
    EXPECT_THAT(globalExecutionPlan.getDecomposedQueryPlans(queryId), SizeIs(3));
    for (const auto& worker : {Host("leaf:1"), Host("base:1"), Host("cloud:1")})
    {
        ASSERT_THAT(planIds.at(worker), SizeIs(1));
        const auto decomposedPlans = globalExecutionPlan.getDecomposedQueryPlans(queryId, worker);
        ASSERT_THAT(decomposedPlans, SizeIs(1));
        EXPECT_THAT(decomposedPlans.front().id, Eq(planIds.at(worker).front()));
        EXPECT_THAT(decomposedPlans.front().state, Eq(DecomposedQueryPlanState::Running));
        EXPECT_THAT(globalExecutionPlan.getPlacedQueryIds(worker), ElementsAre(queryId));
    }
    EXPECT_THAT(globalExecutionPlan.getPlacedQueryIds(Host("unknown:1")), IsEmpty());

    const auto selection = getOperatorByType<SelectionLogicalOperator>(plan.getGlobalPlan()).front();
    const auto sink = getOperatorByType<SinkLogicalOperator>(plan.getGlobalPlan()).front();
    EXPECT_THAT(globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, selection.getId())->worker, Eq(Host("leaf:1")));
    EXPECT_THAT(globalExecutionPlan.findDecomposedQueryPlanHosting(queryId, sink.getId())->worker, Eq(Host("cloud:1")));

    const auto chain = globalExecutionPlan.getChannelChain(queryId, selection.getId(), sink.getId());
    ASSERT_THAT(chain, SizeIs(2));
    EXPECT_THAT(chain.at(0).upstreamPlan, Eq(planIds.at(Host("leaf:1")).front()));
    EXPECT_THAT(chain.at(0).downstreamPlan, Eq(planIds.at(Host("base:1")).front()));
    EXPECT_THAT(chain.at(1).upstreamPlan, Eq(planIds.at(Host("base:1")).front()));
    EXPECT_THAT(chain.at(1).downstreamPlan, Eq(planIds.at(Host("cloud:1")).front()));
    EXPECT_THAT(globalExecutionPlan.getChannelsOnLink(queryId, Host("leaf:1"), Host("base:1")), ElementsAre(chain.at(0)));
    EXPECT_THAT(globalExecutionPlan.getChannelsOnLink(queryId, Host("base:1"), Host("leaf:1")), IsEmpty());
}

TEST_F(GlobalExecutionPlanTest, UpdateAndRemovePlansAndChannels)
{
    const auto plan = cluster.optimize("SELECT * FROM src WHERE a > b INTO snk");
    GlobalExecutionPlan globalExecutionPlan;
    const auto planIds = globalExecutionPlan.addQuery(queryId, plan);
    const auto basePlanId = planIds.at(Host("base:1")).front();

    auto basePlan = *globalExecutionPlan.getDecomposedQueryPlan(queryId, basePlanId);
    const auto newVersion = globalExecutionPlan.getNextVersion();
    EXPECT_THAT(newVersion, Gt(basePlan.version));
    basePlan.version = newVersion;
    basePlan.state = DecomposedQueryPlanState::MarkedForMigration;
    globalExecutionPlan.addOrUpdateDecomposedQueryPlan(queryId, basePlan);
    EXPECT_THAT(globalExecutionPlan.getDecomposedQueryPlan(queryId, basePlanId)->version, Eq(newVersion));

    const auto localQueryId = LocalQueryId(generateUUID());
    globalExecutionPlan.setLocalQueryId(queryId, basePlanId, localQueryId);
    globalExecutionPlan.setState(queryId, basePlanId, DecomposedQueryPlanState::Stopped);
    EXPECT_THAT(globalExecutionPlan.getDecomposedQueryPlan(queryId, basePlanId)->localQueryId, ::testing::Optional(localQueryId));
    EXPECT_THAT(globalExecutionPlan.getDecomposedQueryPlan(queryId, basePlanId)->state, Eq(DecomposedQueryPlanState::Stopped));

    for (const auto& channel : globalExecutionPlan.getChannels(queryId))
    {
        globalExecutionPlan.removeChannel(queryId, channel.channel.id);
    }
    globalExecutionPlan.removeDecomposedQueryPlan(queryId, basePlanId);
    EXPECT_THAT(globalExecutionPlan.getChannels(queryId), IsEmpty());
    EXPECT_THAT(globalExecutionPlan.getDecomposedQueryPlan(queryId, basePlanId), Eq(std::nullopt));
    EXPECT_THAT(globalExecutionPlan.getPlacedQueryIds(Host("base:1")), IsEmpty());

    globalExecutionPlan.removeQuery(queryId);
    EXPECT_FALSE(globalExecutionPlan.contains(queryId));
}

}

/// NOLINTEND(bugprone-unchecked-optional-access)
