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

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(mid1, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src1));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src1, *bfIterator);

    auto dfIterator = DepthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src1, *dfIterator);
}

/**
 * @brief Test on topology with multiple sources
 * --- root --- mid1 --- src1
 *                   \
 *                    --- src2
 */
TEST_F(TopologyIteratorTest, testMultipleSources) {
    TopologyPtr topology = Topology::create();

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(mid1, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src1));

    success = topology->addNewTopologyNodeAsChild(mid1, src2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src2, *bfIterator);

    auto dfIterator = DepthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src1, *dfIterator);
}

/**
 * @brief Test on a topology with different depths on its branches
 * --- root --- mid1 --- src1
 *                   \
 *                    --- mid2 -- src2
 */
TEST_F(TopologyIteratorTest, testTopologyWithDiffernetDepths) {
    TopologyPtr topology = Topology::create();

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(mid1, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src1));

    success = topology->addNewTopologyNodeAsChild(mid1, mid2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(mid2));

    success = topology->addNewTopologyNodeAsChild(mid2, src2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid2->containAsChild(src2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid2, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src2, *bfIterator);

    auto dfIterator = DepthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src1, *dfIterator);
}

/**
 * @brief Test on a topology with longer first branch
 * --- root --- mid1 --- mid2 -- src1
 *                   \
 *                    --- src2
 */
TEST_F(TopologyIteratorTest, testTopologyWithLongerFirstBranch) {
    TopologyPtr topology = Topology::create();

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(mid1, mid2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(mid2));

    success = topology->addNewTopologyNodeAsChild(mid2, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid2->containAsChild(src1));

    success = topology->addNewTopologyNodeAsChild(mid1, src2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src2));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid2, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src2, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src1, *bfIterator);

    auto dfIterator = DepthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src1, *dfIterator);
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

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(mid1, mid2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(mid2));

    success = topology->addNewTopologyNodeAsChild(mid1, mid3);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(mid3));

    success = topology->addNewTopologyNodeAsChild(mid2, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid2->containAsChild(src1));

    success = topology->addNewTopologyNodeAsChild(mid3, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid3->containAsChild(src1));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid2, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid3, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src1, *bfIterator);

    auto dfIterator = DepthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid3, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid2, *dfIterator);
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

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(rootNode, mid2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid2));

    success = topology->addNewTopologyNodeAsChild(mid1, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src1));

    success = topology->addNewTopologyNodeAsChild(mid1, src2);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src2));

    success = topology->addNewTopologyNodeAsChild(mid2, src3);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid2->containAsChild(src3));

    success = topology->addNewTopologyNodeAsChild(mid2, src4);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid2->containAsChild(src4));

    NES_DEBUG("TopologyIteratorTest::testLinearTopology topology: {}", topology->toString());

    // BF iteration
    auto bfIterator = BreadthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(mid2, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src1, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src2, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src3, *bfIterator);
    ++bfIterator;
    EXPECT_EQ(src4, *bfIterator);

    // DF iteration
    auto dfIterator = DepthFirstNodeIterator(topology->getRoot()).begin();
    EXPECT_EQ(rootNode, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src4, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src3, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(mid1, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src2, *dfIterator);
    ++dfIterator;
    EXPECT_EQ(src1, *dfIterator);
}

}// namespace NES
