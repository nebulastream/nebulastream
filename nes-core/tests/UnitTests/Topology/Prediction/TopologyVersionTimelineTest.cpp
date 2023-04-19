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

#include "Common/Identifiers.hpp"
#include <NesBaseTest.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Util/Iterators/DepthFirstNodeIterator.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <Topology/Predictions/TopologyVersionTimeline.hpp>

namespace NES {

class TopologyVersionTimelineTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {

        NES::Logger::setupLogging("TopologyVersionTimelineTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TopologyVersionTimeline test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        std::map<std::string, std::any> properties;
        /*
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
         */

        rootNode = TopologyNode::create(0, "localhost", 4000, 5000, 4, properties);
        mid1 = TopologyNode::create(1, "localhost", 4001, 5001, 4, properties);
        mid2 = TopologyNode::create(2, "localhost", 4002, 5002, 4, properties);
        mid3 = TopologyNode::create(3, "localhost", 4003, 5003, 4, properties);
        src1 = TopologyNode::create(4, "localhost", 4004, 5004, 4, properties);
        src2 = TopologyNode::create(5, "localhost", 4005, 5005, 4, properties);
        src3 = TopologyNode::create(6, "localhost", 4006, 5006, 4, properties);
        src4 = TopologyNode::create(7, "localhost", 4007, 5007, 4, properties);
    }

    std::vector<TopologyNodeId> getIdVector(std::vector<NodePtr> nodes) {
        std::vector<TopologyNodeId> ids;
        ids.reserve(nodes.size());
        for (auto& node : nodes) {
            ids.push_back(node->as<TopologyNode>()->getId());
        }
        return ids;
    }

    void compareIdVectors(std::vector<NodePtr> original, std::vector<NodePtr> copy) {
        EXPECT_EQ(original.size(), copy.size());
        std::vector<TopologyNodeId> originalIds = getIdVector(original);
        std::vector<TopologyNodeId> copiedIds = getIdVector(copy);

        std::sort(originalIds.begin(), originalIds.end());
        std::sort(copiedIds.begin(), copiedIds.end());

        EXPECT_EQ(originalIds, copiedIds);
    }

    void testTopologyEquality(TopologyPtr original, TopologyPtr copy) {
        auto originalIterator = BreadthFirstNodeIterator(original->getRoot()).begin();
        auto copyIterator = BreadthFirstNodeIterator(copy->getRoot()).begin();

        while (originalIterator != NES::BreadthFirstNodeIterator::end()) {
            auto originalNode = (*originalIterator)->as<TopologyNode>();
            auto copiedNode = (*copyIterator)->as<TopologyNode>();
            NES_DEBUG2("checking original node {}", originalNode->getId());

            ASSERT_EQ(originalNode->getId(), copiedNode->getId());
            ASSERT_NE(originalNode, copiedNode);

            compareIdVectors(originalNode->getChildren(), copiedNode->getChildren());
            compareIdVectors(originalNode->getParents(), copiedNode->getParents());

            ++originalIterator;
            ++copyIterator;
        }


    }


  protected:
    TopologyNodePtr rootNode, mid1, mid2, mid3, src1, src2, src3, src4;
};

/**
 * @brief Test on a linear topology
 * --- root --- mid1 --- src1
 */
TEST_F(TopologyVersionTimelineTest, testLinearTopology) {
    TopologyPtr topology = Topology::create();

    topology->setAsRoot(rootNode);

    bool success = topology->addNewTopologyNodeAsChild(rootNode, mid1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(rootNode->containAsChild(mid1));

    success = topology->addNewTopologyNodeAsChild(mid1, src1);
    ASSERT_TRUE(success);
    ASSERT_TRUE(mid1->containAsChild(src1));

    NES_DEBUG("TopologyVersionTimelineTest::testLinearTopology topology:" << topology->toString());

    ///todo: teting
    auto topologyCopy = Experimental::TopologyPrediction::TopologyVersionTimeline::copyTopology(topology);
    testTopologyEquality(topology, topologyCopy);
}

/**
 * @brief Test on topology with multiple sources
 * --- root --- mid1 --- src1
 *                   \
 *                    --- src2
 */
TEST_F(TopologyVersionTimelineTest, testMultipleSources) {
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

    NES_DEBUG("TopologyVersionTimelineTest::testLinearTopology topology:" << topology->toString());

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
TEST_F(TopologyVersionTimelineTest, testTopologyWithDiffernetDepths) {
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

    NES_DEBUG("TopologyVersionTimelineTest::testLinearTopology topology:" << topology->toString());

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
TEST_F(TopologyVersionTimelineTest, testTopologyWithLongerFirstBranch) {
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

    NES_DEBUG("TopologyVersionTimelineTest::testLinearTopology topology:" << topology->toString());

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
TEST_F(TopologyVersionTimelineTest, testBranchedAndMergedTopology) {
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

    NES_DEBUG("TopologyVersionTimelineTest::testLinearTopology topology:" << topology->toString());

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
TEST_F(TopologyVersionTimelineTest, testWithHiearchicalTopology) {
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

    NES_DEBUG("TopologyVersionTimelineTest::testLinearTopology topology:" << topology->toString());

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
