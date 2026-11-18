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

#include <Placement/BottomUpPlacement.hpp>

#include <unordered_map>
#include <unordered_set>
#include <Identifiers/Identifiers.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Placement/IncrementalPlacementUtils.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DistributedTestCluster.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

using namespace ::testing;

class IncrementalBottomUpPlacementTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("IncrementalBottomUpPlacementTest.log", LogLevel::LOG_DEBUG); }

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

    DistributedTestCluster cluster;
};

/// After leaf1 moved to base2, the union can only be placed on a worker that both leafs reach, i.e., the cloud.
TEST_F(IncrementalBottomUpPlacementTest, PlacesFreeOperatorsOnReachableCandidates)
{
    const auto plan = cluster.optimize("SELECT * FROM src1 UNION SELECT * FROM src2 INTO snk").getGlobalPlan();
    auto fixed = getPlacement(plan);
    const auto unionOperator = getOperatorByType<UnionLogicalOperator>(plan).front().getId();
    ASSERT_THAT(fixed.at(unionOperator), Eq(Host("base1:1")));
    fixed.erase(unionOperator);

    ASSERT_TRUE(cluster.workerCatalog->removeLink(Host("leaf1:1"), Host("base1:1")));
    cluster.workerCatalog->addLink(Host("leaf1:1"), Host("base2:1"));

    const std::unordered_set candidates{Host("leaf1:1"), Host("leaf2:1"), Host("base1:1"), Host("base2:1"), Host("cloud:1")};
    const std::unordered_map<Host, Capacity> available{
        {Host("base1:1"), CapacityKind::Limited{1}},
        {Host("base2:1"), CapacityKind::Limited{1}},
        {Host("cloud:1"), CapacityKind::Unlimited{}}};
    const auto placement = BottomUpOperatorPlacer(copyPtr(cluster.workerCatalog)).placeIncrementally(plan, fixed, candidates, available);
    EXPECT_THAT(placement, UnorderedElementsAre(Pair(unionOperator, Host("cloud:1"))));

    /// Without a free slot in the cloud, the union cannot be placed.
    const std::unordered_map<Host, Capacity> noSlotInTheCloud{
        {Host("base1:1"), CapacityKind::Limited{1}}, {Host("base2:1"), CapacityKind::Limited{1}}};
    EXPECT_THROW(
        (void)BottomUpOperatorPlacer(copyPtr(cluster.workerCatalog)).placeIncrementally(plan, fixed, candidates, noSlotInTheCloud),
        Exception);

    /// Without the cloud as candidate, the union cannot be placed.
    const std::unordered_set withoutCloud{Host("leaf1:1"), Host("leaf2:1"), Host("base1:1"), Host("base2:1")};
    EXPECT_THROW(
        (void)BottomUpOperatorPlacer(copyPtr(cluster.workerCatalog)).placeIncrementally(plan, fixed, withoutCloud, available), Exception);
}

}
