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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>

namespace NES {

class TopologyIteratorTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() {

        NES::Logger::setupLogging("TopologyIteratorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TopologyIteratorTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        rootNode = TopologyNode::create(0, "localhost", 4000, 5000, 4, properties);
        mid1 = TopologyNode::create(1, "localhost", 4001, 5001, 4, properties);
        mid2 = TopologyNode::create(2, "localhost", 4002, 5002, 4, properties);
        mid3 = TopologyNode::create(3, "localhost", 4003, 5003, 4, properties);
        src1 = TopologyNode::create(4, "localhost", 4004, 5004, 4, properties);
        src2 = TopologyNode::create(5, "localhost", 4005, 5005, 4, properties);
        src3 = TopologyNode::create(6, "localhost", 4006, 5006, 4, properties);
        src4 = TopologyNode::create(7, "localhost", 4007, 5007, 4, properties);
    }

  protected:
    TopologyNodePtr rootNode, mid1, mid2, mid3, src1, src2, src3, src4;
};

/**
 * @brief Test on a linear topology
 * --- root --- mid1 --- src1
 */
TEST_F(TopologyIteratorTest, testLinearTopology) {
    TopologyPtr topology = Topology::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    int rootWorkerId = 0;
    topology->registerTopologyNode(rootWorkerId, "localhost", 4000, 5000, 4, properties);
    int middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 4, properties);
    int srcNodeId = 3;
    topology->registerTopologyNode(srcNodeId, "localhost", 4004, 5004, 4, properties);

    topology->setRootTopologyNodeId(rootWorkerId);
    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId, srcNodeId));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto rootLockedTopologyNode = topology->lockTopologyNode(rootWorkerId);

    auto bfIterator = BreadthFirstNodeIterator(rootLockedTopologyNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, bfIterator.operator*()->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId, bfIterator.operator*()->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId, bfIterator.operator*()->as<TopologyNode>()->getId());

    auto dfIterator = DepthFirstNodeIterator(rootLockedTopologyNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, dfIterator.operator*()->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId, dfIterator.operator*()->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId, dfIterator.operator*()->as<TopologyNode>()->getId());
}

/**
 * @brief Test on topology with multiple sources
 * --- root --- mid1 --- src1
 *                   \
 *                    --- src2
 */
TEST_F(TopologyIteratorTest, testMultipleSources) {
    TopologyPtr topology = Topology::create();
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    WorkerId rootWorkerId = 1;
    topology->registerTopologyNode(rootWorkerId, "localhost", 4000, 5000, 4, properties);
    WorkerId middleNodeId1 = 2;
    topology->registerTopologyNode(middleNodeId1, "localhost", 4001, 5001, 4, properties);
    WorkerId srcNodeId1 = 3;
    topology->registerTopologyNode(srcNodeId1, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId2 = 4;
    topology->registerTopologyNode(srcNodeId2, "localhost", 4004, 5004, 4, properties);

    topology->setRootTopologyNodeId(rootWorkerId);

    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, srcNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, srcNodeId2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());
    auto rootNode = topology->lockTopologyNode(rootWorkerId);
    auto bfIterator = BreadthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId2, (*bfIterator)->as<TopologyNode>()->getId());

    auto dfIterator = DepthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
}

/**
 * @brief Test on a topology with different depths on its branches
 * --- root --- mid1 --- src1
 *                   \
 *                    --- mid2 -- src2
 */
TEST_F(TopologyIteratorTest, testTopologyWithDiffernetDepths) {
    TopologyPtr topology = Topology::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    WorkerId rootWorkerId = 1;
    topology->registerTopologyNode(rootWorkerId, "localhost", 4000, 5000, 4, properties);
    WorkerId middleNodeId1 = 2;
    topology->registerTopologyNode(middleNodeId1, "localhost", 4001, 5001, 4, properties);
    WorkerId middleNodeId2 = 3;
    topology->registerTopologyNode(middleNodeId2, "localhost", 4001, 5001, 4, properties);
    WorkerId srcNodeId1 = 5;
    topology->registerTopologyNode(srcNodeId1, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId2 = 6;
    topology->registerTopologyNode(srcNodeId2, "localhost", 4004, 5004, 4, properties);

    topology->setRootTopologyNodeId(rootWorkerId);

    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, srcNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, middleNodeId2));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId2, srcNodeId2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());
    auto rootNode = topology->lockTopologyNode(rootWorkerId);
    auto bfIterator = BreadthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId2, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId2, (*bfIterator)->as<TopologyNode>()->getId());

    auto dfIterator = DepthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
}

/**
 * @brief Test on a topology with longer first branch
 * --- root --- mid1 --- mid2 -- src1
 *                   \
 *                    --- src2
 */
TEST_F(TopologyIteratorTest, testTopologyWithLongerFirstBranch) {
    TopologyPtr topology = Topology::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    WorkerId rootWorkerId = 1;
    topology->registerTopologyNode(rootWorkerId, "localhost", 4000, 5000, 4, properties);
    WorkerId middleNodeId1 = 2;
    topology->registerTopologyNode(middleNodeId1, "localhost", 4001, 5001, 4, properties);
    WorkerId middleNodeId2 = 3;
    topology->registerTopologyNode(middleNodeId2, "localhost", 4001, 5001, 4, properties);
    WorkerId srcNodeId1 = 5;
    topology->registerTopologyNode(srcNodeId1, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId2 = 6;
    topology->registerTopologyNode(srcNodeId2, "localhost", 4004, 5004, 4, properties);

    topology->setRootTopologyNodeId(rootWorkerId);

    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, middleNodeId2));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId2, srcNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, srcNodeId2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto rootNode = topology->lockTopologyNode(rootWorkerId);
    auto bfIterator = BreadthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId2, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId2, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId1, (*bfIterator)->as<TopologyNode>()->getId());

    auto dfIterator = DepthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
}

/**
 * @brief Test on a branched and merged topology
 *                       --- mid2 ---
 *                     /              \
 * --- root--- mid1 ---                --- src1
 *                     \              /
 *                       --- mid3 ---
 */
TEST_F(TopologyIteratorTest, testBranchedAndMergedTopology) {
    TopologyPtr topology = Topology::create();
    
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    WorkerId rootWorkerId = 1;
    topology->registerTopologyNode(rootWorkerId, "localhost", 4000, 5000, 4, properties);
    WorkerId middleNodeId1 = 2;
    topology->registerTopologyNode(middleNodeId1, "localhost", 4001, 5001, 4, properties);
    WorkerId middleNodeId2 = 3;
    topology->registerTopologyNode(middleNodeId2, "localhost", 4001, 5001, 4, properties);
    WorkerId middleNodeId3 = 4;
    topology->registerTopologyNode(middleNodeId3, "localhost", 4001, 5001, 4, properties);
    WorkerId srcNodeId1 = 5;
    topology->registerTopologyNode(srcNodeId1, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId2 = 6;
    topology->registerTopologyNode(srcNodeId2, "localhost", 4004, 5004, 4, properties);

    topology->setRootTopologyNodeId(rootWorkerId);

    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, middleNodeId2));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, middleNodeId3));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId2, srcNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId3, srcNodeId2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto rootNode = topology->lockTopologyNode(rootWorkerId);
    
    // BF iteration
    auto bfIterator = BreadthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId2, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId3, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId1, (*bfIterator)->as<TopologyNode>()->getId());

    auto dfIterator = DepthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId1, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId3, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId2, (*dfIterator)->as<TopologyNode>()->getId());
}

/**
 * @brief Test on a hierarchical topology
 *
 *                              --- src1
 *                            /
 *               --- mid1 ---
 *             /              \
 *            /                --- src2
 * --- root---
 *            \                --- src3
 *             \             /
 *              --- mid2 ---
 *                           \
 *                             --- src4
 */
TEST_F(TopologyIteratorTest, testWithHiearchicalTopology) {
    TopologyPtr topology = Topology::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    WorkerId rootWorkerId = 1;
    topology->registerTopologyNode(rootWorkerId, "localhost", 4000, 5000, 4, properties);
    WorkerId middleNodeId1 = 2;
    topology->registerTopologyNode(middleNodeId1, "localhost", 4001, 5001, 4, properties);
    WorkerId middleNodeId2 = 3;
    topology->registerTopologyNode(middleNodeId2, "localhost", 4001, 5001, 4, properties);
    WorkerId srcNodeId1 = 4;
    topology->registerTopologyNode(srcNodeId1, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId2 = 5;
    topology->registerTopologyNode(srcNodeId2, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId3 = 6;
    topology->registerTopologyNode(srcNodeId3, "localhost", 4004, 5004, 4, properties);
    WorkerId srcNodeId4 = 7;
    topology->registerTopologyNode(srcNodeId4, "localhost", 4004, 5004, 4, properties);

    topology->setRootTopologyNodeId(rootWorkerId);

    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(rootWorkerId, middleNodeId2));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, srcNodeId1));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId1, srcNodeId2));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId2, srcNodeId3));
    ASSERT_TRUE(topology->addTopologyNodeAsChild(middleNodeId2, srcNodeId4));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto rootNode = topology->lockTopologyNode(rootWorkerId);

    // BF iteration
    auto bfIterator = BreadthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(middleNodeId2, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId1, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId2, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId3, (*bfIterator)->as<TopologyNode>()->getId());
    ++bfIterator;
    EXPECT_EQ(srcNodeId4, (*bfIterator)->as<TopologyNode>()->getId());

    // DF iteration
    auto dfIterator = DepthFirstNodeIterator(rootNode->operator*()).begin();
    EXPECT_EQ(rootWorkerId,(*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId2,(*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId4,(*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId3,(*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(middleNodeId1,(*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId2,(*dfIterator)->as<TopologyNode>()->getId());
    ++dfIterator;
    EXPECT_EQ(srcNodeId1,(*dfIterator)->as<TopologyNode>()->getId());
}

}// namespace NES
