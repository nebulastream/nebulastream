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

#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include "../../util/NesBaseTest.hpp"

namespace NES {

class TopologyPropertiesTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() { setupLogging(); }

    void SetUp() override {}

    void TearDown() override { NES_DEBUG("Tear down TopologyPropertiesTest Test."); }

  protected:
    static void setupLogging() {
        NES::setupLogging("TopologyPropertiesTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup TopologyPropertiesTest test class.");
    }
};

// test assigning topology properties
TEST_F(TopologyPropertiesTest, testAssignTopologyNodeProperties) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create a node
    auto node = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8);
    node->addNodeProperty("cores", 2);
    node->addNodeProperty("architecture", std::string("arm64"));
    node->addNodeProperty("withGPU", false);

    EXPECT_TRUE(node->getNodeProperty("cores").has_value());
    EXPECT_TRUE(node->getNodeProperty("architecture").has_value());
    EXPECT_TRUE(node->getNodeProperty("withGPU").has_value());

    EXPECT_EQ(std::any_cast<int>(node->getNodeProperty("cores")), 2);
    EXPECT_EQ(std::any_cast<std::string>(node->getNodeProperty("architecture")), "arm64");
    EXPECT_EQ(std::any_cast<bool>(node->getNodeProperty("withGPU")), false);
}

// test removing a topology properties
TEST_F(TopologyPropertiesTest, testRemoveTopologyNodeProperty) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create a node
    auto node = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8);
    node->addNodeProperty("cores", 2);

    ASSERT_TRUE(node->getNodeProperty("cores").has_value());

    node->removeNodeProperty("cores");
    EXPECT_THROW(node->getNodeProperty("cores"), NesRuntimeException);
}

// test assigning link properties
TEST_F(TopologyPropertiesTest, testAssignLinkProperty) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create src and dst nodes
    auto sourceNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8);

    grpcPort++;
    dataPort++;
    auto destinationNode = TopologyNode::create(2, "localhost", grpcPort, dataPort, 8);

    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

    sourceNode->addLinkProperty(destinationNode, linkProperty);
    destinationNode->addLinkProperty(sourceNode, linkProperty);

    // we should be able to retrieve the assigned link property
    EXPECT_NO_THROW(sourceNode->getLinkProperty(destinationNode));
    EXPECT_EQ(sourceNode->getLinkProperty(destinationNode)->bandwidth, 512u);
    EXPECT_EQ(sourceNode->getLinkProperty(destinationNode)->latency, 100u);

    EXPECT_NO_THROW(destinationNode->getLinkProperty(sourceNode));
    EXPECT_EQ(destinationNode->getLinkProperty(sourceNode)->bandwidth, 512u);
    EXPECT_EQ(destinationNode->getLinkProperty(sourceNode)->latency, 100u);
}

// test removing link properties
TEST_F(TopologyPropertiesTest, testRemovingLinkProperty) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create src and dst nodes
    auto sourceNode = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8);

    grpcPort++;
    dataPort++;
    auto destinationNode = TopologyNode::create(2, "localhost", grpcPort, dataPort, 8);

    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));

    sourceNode->addLinkProperty(destinationNode, linkProperty);
    destinationNode->addLinkProperty(sourceNode, linkProperty);

    // we should be able to retrieve the assigned link property
    ASSERT_NO_THROW(sourceNode->getLinkProperty(destinationNode));
    ASSERT_EQ(sourceNode->getLinkProperty(destinationNode)->bandwidth, 512ULL);
    ASSERT_EQ(sourceNode->getLinkProperty(destinationNode)->latency, 100ULL);

    ASSERT_NO_THROW(destinationNode->getLinkProperty(sourceNode));
    ASSERT_EQ(destinationNode->getLinkProperty(sourceNode)->bandwidth, 512ULL);
    ASSERT_EQ(destinationNode->getLinkProperty(sourceNode)->latency, 100ULL);

    sourceNode->removeLinkProperty(destinationNode);
    destinationNode->removeLinkProperty(sourceNode);

    EXPECT_THROW(sourceNode->getLinkProperty(destinationNode), NesRuntimeException);
    EXPECT_THROW(destinationNode->getLinkProperty(sourceNode), NesRuntimeException);
}

}// namespace NES
