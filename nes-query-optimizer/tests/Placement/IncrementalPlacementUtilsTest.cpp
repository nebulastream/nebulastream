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

#include <Placement/IncrementalPlacementUtils.hpp>

#include <optional>
#include <string>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedLogicalPlan.hpp>
#include <DistributedTestCluster.hpp>
#include <ErrorHandling.hpp>
#include <GlobalExecutionPlan.hpp>
#include <QueryId.hpp>
#include <SharedQueryPlan.hpp>

namespace NES
{

using namespace ::testing;

/// Topology of the mobility scenario of ISQP: leafs connect to one of two base stations, which connect to the cloud.
///
///   leaf1 ---> base1 ---> cloud
///   leaf2 --/            /
///              base2 ---/
class IncrementalPlacementUtilsTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("IncrementalPlacementUtilsTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        cluster.addWorker("cloud:1", {});
        cluster.addWorker("base1:1", {"cloud:1"}, 1);
        cluster.addWorker("base2:1", {"cloud:1"}, 1);
        cluster.addWorker("leaf1:1", {"base1:1"}, 0);
        cluster.addWorker("leaf2:1", {"base1:1"}, 0);
        cluster.addSource("src1", {"a", "b"}, "leaf1:1");
        cluster.addSource("src2", {"a", "b"}, "leaf2:1");
        cluster.addSink("snk", {"a", "b"}, "cloud:1");
    }

    void deploy(const std::string& sql)
    {
        const auto plan = cluster.optimize(sql);
        globalExecutionPlan.addQuery(queryId, plan);
        sharedQueryPlan.emplace(queryId, plan.getGlobalPlan(), getPlacement(plan.getGlobalPlan()));
    }

    /// Moves leaf1 from base1 to base2.
    void moveLeaf1()
    {
        EXPECT_TRUE(cluster.workerCatalog->removeLink(Host("leaf1:1"), Host("base1:1")));
        cluster.workerCatalog->addLink(Host("leaf1:1"), Host("base2:1"));
    }

    [[nodiscard]] OperatorId sourceOn(const std::string& worker) const
    {
        for (const auto& source : getOperatorByType<SourceDescriptorLogicalOperator>(sharedQueryPlan->getPlan()))
        {
            if (source->getSourceDescriptor().getHost() == Host(worker))
            {
                return source.getId();
            }
        }
        throw TestException("No source on {}", worker);
    }

    template <typename T>
    [[nodiscard]] OperatorId single() const
    {
        const auto operators = getOperatorByType<T>(sharedQueryPlan->getPlan());
        if (operators.size() != 1)
        {
            throw TestException("Expected a single operator, found {}", operators.size());
        }
        return operators.front().getId();
    }

    DistributedTestCluster cluster;
    GlobalExecutionPlan globalExecutionPlan;
    std::optional<SharedQueryPlan> sharedQueryPlan;
    const DistributedQueryId queryId{"q"};
};

TEST_F(IncrementalPlacementUtilsTest, NetworkOperatorsForLink)
{
    deploy("SELECT * FROM src1 WHERE a > b INTO snk");
    const auto selection = single<SelectionLogicalOperator>();
    const auto sink = single<SinkLogicalOperator>();
    ASSERT_THAT(sharedQueryPlan->getPinnedWorker(selection), ::testing::Optional(Host("base1:1")));

    /// The source on leaf1 sends to the selection on base1, which sends to the sink in the cloud.
    EXPECT_THAT(
        findNetworkOperatorsForLink(globalExecutionPlan, queryId, Host("leaf1:1"), Host("base1:1")),
        ElementsAre(std::pair{sourceOn("leaf1:1"), selection}));
    EXPECT_THAT(
        findNetworkOperatorsForLink(globalExecutionPlan, queryId, Host("base1:1"), Host("cloud:1")),
        ElementsAre(std::pair{selection, sink}));
    EXPECT_THAT(findNetworkOperatorsForLink(globalExecutionPlan, queryId, Host("leaf2:1"), Host("base1:1")), IsEmpty());
}

/// If the downstream operator stays reachable, only the network operators between the pinned operators are replaced.
TEST_F(IncrementalPlacementUtilsTest, ReachableDownstreamOperatorStaysPinned)
{
    cluster.addWorker("leaf3:1", {"base1:1"}, 0);
    cluster.addSource("src3", {"a", "b"}, "leaf3:1");
    deploy("SELECT * FROM src3 INTO snk");
    const auto sink = single<SinkLogicalOperator>();

    /// leaf3 now reaches the cloud over base2.
    EXPECT_TRUE(cluster.workerCatalog->removeLink(Host("leaf3:1"), Host("base1:1")));
    cluster.workerCatalog->addLink(Host("leaf3:1"), Host("base2:1"));

    const auto pinned = findUpstreamAndDownstreamPinnedOperators(
        *sharedQueryPlan, globalExecutionPlan, Host("leaf3:1"), Host("base1:1"), cluster.workerCatalog->getTopology());
    EXPECT_THAT(pinned.upstreamOperators, ElementsAre(sourceOn("leaf3:1")));
    EXPECT_THAT(pinned.downstreamOperators, ElementsAre(sink));
}

/// If the downstream operator is no longer reachable, the closest reachable downstream operator is pinned and the operators in between are
/// re-placed. The other upstream operators of the re-placed operators are pinned as well.
TEST_F(IncrementalPlacementUtilsTest, UnreachableOperatorIsReplaced)
{
    deploy("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk");
    const auto unionOperator = single<UnionLogicalOperator>();
    const auto sink = single<SinkLogicalOperator>();
    ASSERT_THAT(sharedQueryPlan->getPinnedWorker(unionOperator), ::testing::Optional(Host("base1:1")));

    moveLeaf1();
    const auto pinned = findUpstreamAndDownstreamPinnedOperators(
        *sharedQueryPlan, globalExecutionPlan, Host("leaf1:1"), Host("base1:1"), cluster.workerCatalog->getTopology());
    EXPECT_THAT(pinned.upstreamOperators, UnorderedElementsAre(sourceOn("leaf1:1"), sourceOn("leaf2:1")));
    EXPECT_THAT(pinned.downstreamOperators, ElementsAre(sink));

    sharedQueryPlan->performReOperatorPlacement(pinned.upstreamOperators, pinned.downstreamOperators);
    EXPECT_THAT(sharedQueryPlan->getOperatorState(unionOperator), Eq(OperatorState::ToBeReplaced));
    EXPECT_THAT(sharedQueryPlan->getOperatorState(sink), Eq(OperatorState::Placed));
}

}
