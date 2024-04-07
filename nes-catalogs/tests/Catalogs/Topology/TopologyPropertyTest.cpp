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
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>

namespace NES {

class TopologyPropertiesTest : public Testing::BaseUnitTest {
  public:
    static void SetUpTestCase() { setupLogging(); }

  protected:
    static void setupLogging() {
        NES::Logger::setupLogging("TopologyPropertiesTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup TopologyPropertiesTest test class.");
    }
};

// test assigning topology properties
TEST_F(TopologyPropertiesTest, testAssignTopologyNodeProperties) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    // create a node
    auto node = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8, properties);
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

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    // create a node
    auto node = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8, properties);
    node->addNodeProperty("cores", 2);

    EXPECT_TRUE(node->getNodeProperty("cores").has_value());

    node->removeNodeProperty("cores");
    EXPECT_FALSE(node->getNodeProperty("cores").has_value());
}

// test assigning link properties
TEST_F(TopologyPropertiesTest, testAssignLinkProperty) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    // create src and dst nodes
    int sourceWorkerId = 1;
    topology->registerWorker(sourceWorkerId, "localhost", grpcPort, dataPort, 8, properties, 0, 0);

    grpcPort++;
    dataPort++;
    int destinationWorkerId = 2;
    topology->registerWorker(destinationWorkerId, "localhost", grpcPort, dataPort, 8, properties, 0, 0);

    topology->addTopologyNodeAsChild(destinationWorkerId, sourceWorkerId);

    topology->addLinkProperty(destinationWorkerId, sourceWorkerId, 512, 100);

    // we should be able to retrieve the assigned link property
    auto sourceNode = topology->getCopyOfTopologyNodeWithId(sourceWorkerId);
    EXPECT_NO_THROW(sourceNode->getLinkProperty(destinationWorkerId));
    EXPECT_EQ(sourceNode->getLinkProperty(destinationWorkerId)->bandwidth, 512u);
    EXPECT_EQ(sourceNode->getLinkProperty(destinationWorkerId)->latency, 100u);

    auto destinationNode = topology->getCopyOfTopologyNodeWithId(destinationWorkerId);
    EXPECT_NO_THROW(destinationNode->getLinkProperty(sourceWorkerId));
    EXPECT_EQ(destinationNode->getLinkProperty(sourceWorkerId)->bandwidth, 512u);
    EXPECT_EQ(destinationNode->getLinkProperty(sourceWorkerId)->latency, 100u);
}

// test removing link properties
TEST_F(TopologyPropertiesTest, testRemovingLinkProperty) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    // create src and dst nodes
    int sourceWorkerId = 1;
    topology->registerWorker(sourceWorkerId, "localhost", grpcPort, dataPort, 8, properties, 0, 0);

    grpcPort++;
    dataPort++;
    int destinationWorkerId = 2;
    topology->registerWorker(destinationWorkerId, "localhost", grpcPort, dataPort, 8, properties, 0, 0);
    topology->addTopologyNodeAsChild(destinationWorkerId, sourceWorkerId);
    topology->addLinkProperty(destinationWorkerId, sourceWorkerId, 512, 100);

    auto sourceNode = topology->getCopyOfTopologyNodeWithId(sourceWorkerId);

    // we should be able to retrieve the assigned link property
    ASSERT_NO_THROW(sourceNode->getLinkProperty(destinationWorkerId));
    ASSERT_EQ(sourceNode->getLinkProperty(destinationWorkerId)->bandwidth, 512ULL);
    ASSERT_EQ(sourceNode->getLinkProperty(destinationWorkerId)->latency, 100ULL);

    auto destinationNode = topology->getCopyOfTopologyNodeWithId(destinationWorkerId);
    ASSERT_NO_THROW(destinationNode->getLinkProperty(sourceWorkerId));
    ASSERT_EQ(destinationNode->getLinkProperty(sourceWorkerId)->bandwidth, 512ULL);
    ASSERT_EQ(destinationNode->getLinkProperty(sourceWorkerId)->latency, 100ULL);

    //Remove the link and associated properties
    topology->removeTopologyNodeAsChild(destinationWorkerId, sourceWorkerId);

    sourceNode = topology->getCopyOfTopologyNodeWithId(sourceWorkerId);
    destinationNode = topology->getCopyOfTopologyNodeWithId(destinationWorkerId);
    EXPECT_FALSE(sourceNode->getLinkProperty(destinationWorkerId));
    EXPECT_FALSE(destinationNode->getLinkProperty(sourceWorkerId));
}

}// namespace NES
