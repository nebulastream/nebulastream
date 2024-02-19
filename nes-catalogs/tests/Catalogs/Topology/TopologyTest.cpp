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
#include <Catalogs/Topology/PathFinder.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <cstddef>
#include <gtest/gtest.h>
#include <iostream>

using namespace NES;

/* - TopologyTest ---------------------------------------------------- */
class TopologyTest : public Testing::BaseUnitTest {
  public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase() { NES::Logger::setupLogging("NesTopologyManager.log", NES::LogLevel::LOG_DEBUG); }
};
/* - Nodes ----------------------------------------------------------------- */
/**
 * Create a new node. 
 */
TEST_F(TopologyTest, createNode) {
    uint64_t invalidId = 0;

    auto const node1Id = 1u;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    auto physicalNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources, properties);
    EXPECT_NE(physicalNode.get(), nullptr);
    EXPECT_EQ(physicalNode->toString(),
              "PhysicalNode[id=" + std::to_string(node1Id) + ", ip=" + node1Address
                  + ", resourceCapacity=" + std::to_string(resources) + ", usedResource=0]");
    EXPECT_NE(physicalNode->getId(), invalidId);
    EXPECT_EQ(physicalNode->getId(), node1Id);
    EXPECT_EQ(physicalNode->getIpAddress(), node1Address);
    EXPECT_EQ(physicalNode->getGrpcPort(), grpcPort);
    EXPECT_EQ(physicalNode->getDataPort(), dataPort);
}

///* Remove a root node. */
TEST_F(TopologyTest, removeRootNode) {
    TopologyPtr topology = Topology::create();

    WorkerId rootTopologyNodeId = topology->getRootTopologyNodeId();
    EXPECT_EQ(rootTopologyNodeId, INVALID_WORKER_NODE_ID);

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);
    bool success = topology->unregisterWorker(node1Id);
    EXPECT_TRUE(success);
}

/**
 * Remove an existing node.
 */
TEST_F(TopologyTest, removeAnExistingNode) {
    TopologyPtr topology = Topology::create();

    WorkerId rootTopologyNodeId = topology->getRootTopologyNodeId();
    EXPECT_EQ(rootTopologyNodeId, INVALID_WORKER_NODE_ID);

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);

    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);

    success = topology->unregisterWorker(node2Id);
    EXPECT_TRUE(success);
}

/**
 * Remove a non-root node.
 */
TEST_F(TopologyTest, removeNodeWithNonRootParent) {
    TopologyPtr topology = Topology::create();

    WorkerId rootTopologyNodeId = topology->getRootTopologyNodeId();
    EXPECT_EQ(rootTopologyNodeId, INVALID_WORKER_NODE_ID);

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);

    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    ASSERT_TRUE(success);

    int node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node3Id, node3Address, grpcPort, dataPort, resources, properties, 0, 0);

    bool success2 = topology->addTopologyNodeAsChild(node2Id, node3Id);
    ASSERT_TRUE(success2);

    int node4Id = 4;
    std::string node4Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node4Id, node4Address, grpcPort, dataPort, resources, properties, 0, 0);

    bool success3 = topology->addTopologyNodeAsChild(node2Id, node4Id);
    ASSERT_TRUE(success3);

    success = topology->unregisterWorker(node4Id);
    EXPECT_TRUE(success);

    TopologyNodePtr node = topology->getCopyOfTopologyNodeWithId(4u);
    EXPECT_TRUE(node == nullptr);
}

/**
 *  Remove a non-existing node.
 */
TEST_F(TopologyTest, removeNodeFromEmptyTopology) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);

    EXPECT_TRUE(topology->unregisterWorker(node1Id));
    EXPECT_FALSE(topology->nodeWithWorkerIdExists(node1Id));
}

/* Create a new link. */
TEST_F(TopologyTest, createLink) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);
    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);

    int node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node3Id, node3Address, grpcPort, dataPort, resources, properties, 0, 0);
    success = topology->addTopologyNodeAsChild(node2Id, node3Id);
    EXPECT_TRUE(success);
}

/* Create link, where a link already exists. */
TEST_F(TopologyTest, createExistingLink) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);
    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);
    success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_FALSE(success);
}

/* Remove an existing link. */
TEST_F(TopologyTest, removeLink) {

    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);
    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);
    success = topology->removeTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);
}

/* Remove a non-existing link. */
TEST_F(TopologyTest, removeNonExistingLink) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);
    bool success = topology->removeTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_FALSE(success);
}

///* - Print ----------------------------------------------------------------- */
TEST_F(TopologyTest, printGraph) {

    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // creater workers
    std::vector<WorkerId> workers;
    int resource = 4;
    for (uint32_t i = 0; i < 7; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        workers.emplace_back(i);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    int counter = 0;
    // create sensors
    std::vector<WorkerId> sensors;
    for (uint32_t i = 7; i < 23; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        sensors.emplace_back(counter);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
        counter++;
    }

    topology->setRootTopologyNodeId(workers.at(0));

    // link each worker with its neighbor
    topology->addTopologyNodeAsChild(workers.at(0), workers.at(1));
    topology->addTopologyNodeAsChild(workers.at(0), workers.at(2));

    topology->addTopologyNodeAsChild(workers.at(1), workers.at(3));
    topology->addTopologyNodeAsChild(workers.at(1), workers.at(4));

    topology->addTopologyNodeAsChild(workers.at(2), workers.at(5));
    topology->addTopologyNodeAsChild(workers.at(2), workers.at(6));

    // each worker has three sensors
    for (uint32_t i = 0; i < 15; i++) {
        if (i < 4) {
            topology->addTopologyNodeAsChild(workers.at(3), sensors.at(i));
        } else if (i < 8) {
            topology->addTopologyNodeAsChild(workers.at(4), sensors.at(i));
        } else if (i < 12) {
            topology->addTopologyNodeAsChild(workers.at(5), sensors.at(i));
        } else {
            topology->addTopologyNodeAsChild(workers.at(6), sensors.at(i));
        }
    }

    NES_INFO(" current plan from topo=");
    topology->print();
    SUCCEED();
}

TEST_F(TopologyTest, printGraphWithoutAnything) {
    TopologyPtr topology = Topology::create();

    std::string expectedResult;
    EXPECT_TRUE(topology->toString() == expectedResult);
}

/**
 * @brief Find Path between two nodes
 */
TEST_F(TopologyTest, findPathBetweenTwoNodes) {
    TopologyPtr topology = Topology::create();

    WorkerId node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    WorkerId node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);
    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);

    WorkerId node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node3Id, node3Address, grpcPort, dataPort, resources, properties, 0, 0);
    success = topology->addTopologyNodeAsChild(node2Id, node3Id);
    EXPECT_TRUE(success);

    auto startNode = topology->findAllPathBetween(node2Id, node1Id).value();

    EXPECT_TRUE(startNode->getId() == node2Id);
}

/**
 * @brief Find Path between nodes with multiple parents and children
 */
TEST_F(TopologyTest, findPathBetweenNodesWithMultipleParentsAndChildren) {

    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // creater workers
    std::vector<WorkerId> workerIds;
    int resource = 4;
    for (uint32_t i = 0; i < 10; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        workerIds.emplace_back(i);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setRootTopologyNodeId(workerIds.at(0));

    // link each worker with its neighbor
    topology->addTopologyNodeAsChild(workerIds.at(0), workerIds.at(1));
    topology->addTopologyNodeAsChild(workerIds.at(0), workerIds.at(2));

    topology->addTopologyNodeAsChild(workerIds.at(1), workerIds.at(3));
    topology->addTopologyNodeAsChild(workerIds.at(1), workerIds.at(4));

    topology->addTopologyNodeAsChild(workerIds.at(2), workerIds.at(5));
    topology->addTopologyNodeAsChild(workerIds.at(2), workerIds.at(6));

    topology->addTopologyNodeAsChild(workerIds.at(4), workerIds.at(7));
    topology->addTopologyNodeAsChild(workerIds.at(5), workerIds.at(7));

    topology->addTopologyNodeAsChild(workerIds.at(7), workerIds.at(8));
    topology->addTopologyNodeAsChild(workerIds.at(7), workerIds.at(9));

    const std::optional<TopologyNodePtr> startNode = topology->findAllPathBetween(workerIds.at(9), workerIds.at(2));

    EXPECT_TRUE(startNode.has_value());
    EXPECT_TRUE(startNode.value()->getId() == workerIds.at(9));
}

/**
 * @brief Find Path between two not connected nodes
 */
TEST_F(TopologyTest, findPathBetweenTwoNotConnectedNodes) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topology->registerWorker(node1Id, node1Address, grpcPort, dataPort, resources, properties, 0, 0);
    topology->setRootTopologyNodeId(node1Id);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node2Id, node2Address, grpcPort, dataPort, resources, properties, 0, 0);
    bool success = topology->addTopologyNodeAsChild(node1Id, node2Id);
    EXPECT_TRUE(success);

    int node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    topology->registerWorker(node3Id, node3Address, grpcPort, dataPort, resources, properties, 0, 0);
    success = topology->addTopologyNodeAsChild(node2Id, node3Id);
    EXPECT_TRUE(success);

    success = topology->removeTopologyNodeAsChild(node2Id, node3Id);
    EXPECT_TRUE(success);

    const std::optional<TopologyNodePtr> startNode = topology->findAllPathBetween(node3Id, node1Id);
    EXPECT_FALSE(startNode.has_value());
}

/**
 * @brief Find Path between multiple source and destination nodes
 */
TEST_F(TopologyTest, findPathBetweenSetOfSourceAndDestinationNodes) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // creater workers
    std::vector<WorkerId> topologyNodeIds;
    int resource = 4;
    for (uint32_t i = 0; i < 10; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        topologyNodeIds.emplace_back(i);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setRootTopologyNodeId(topologyNodeIds.at(0));

    // link each worker with its neighbor
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(1));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(2));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(3));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(4));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(5));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(6));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(4), topologyNodeIds.at(7));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(5), topologyNodeIds.at(7));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(7), topologyNodeIds.at(8));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(7), topologyNodeIds.at(9));

    std::vector<WorkerId> sourceNodes{topologyNodeIds.at(8), topologyNodeIds.at(9)};
    std::vector<WorkerId> destinationNodes{topologyNodeIds.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    TopologyNodePtr startNode1 = startNodes[0];
    EXPECT_TRUE(startNode1->getId() == topologyNodeIds.at(8));
    TopologyNodePtr startNode2 = startNodes[1];
    EXPECT_TRUE(startNode2->getId() == topologyNodeIds.at(9));
    EXPECT_TRUE(startNode2->getParents().size() == startNode1->getParents().size());
    EXPECT_TRUE(startNode2->getParents()[0]->as<TopologyNode>()->getId()
                == startNode1->getParents()[0]->as<TopologyNode>()->getId());
    TopologyNodePtr s1Parent1 = startNode1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent1->getId() == topologyNodeIds.at(7));
    TopologyNodePtr s1Parent2 = s1Parent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent2->getId() == topologyNodeIds.at(4));
    TopologyNodePtr s1Parent3 = s1Parent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent3->getId() == topologyNodeIds.at(1));
    TopologyNodePtr s1Parent4 = s1Parent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent4->getId() == topologyNodeIds.at(0));
}

/**
 * @brief Find Path between two connected nodes and select the shortest path
 */
TEST_F(TopologyTest, findPathBetweenSetOfSourceAndDestinationNodesAndSelectTheShortest) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // creater workers
    std::vector<WorkerId> topologyNodeIds;
    int resource = 4;
    for (uint32_t i = 0; i < 10; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        topologyNodeIds.emplace_back(i);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setRootTopologyNodeId(topologyNodeIds.at(0));

    // link each worker with its neighbor
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(1));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(2));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(3));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(4));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(5));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(6));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(4), topologyNodeIds.at(7));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(5), topologyNodeIds.at(7));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(7), topologyNodeIds.at(8));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(7), topologyNodeIds.at(9));

    std::vector<WorkerId> sourceNodes{topologyNodeIds.at(8)};
    std::vector<WorkerId> destinationNodes{topologyNodeIds.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    TopologyNodePtr startNode = startNodes[0];
    EXPECT_TRUE(startNode->getId() == topologyNodeIds.at(8));
    TopologyNodePtr parent1 = startNode->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(parent1->getId() == topologyNodeIds.at(7));
    TopologyNodePtr parent2 = parent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(parent2->getId() == topologyNodeIds.at(5));
    TopologyNodePtr parent3 = parent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(parent3->getId() == topologyNodeIds.at(0));
}

/**
 * @brief Tests if the path finding function find() properly ignores nodes marked for maintenance in a complex topology
 * Topology:
    PhysicalNode[id=0, ip=localhost, resourceCapacity=4, usedResource=0]
    |--PhysicalNode[id=4, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |--PhysicalNode[id=7, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=14, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=14, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=13, ip=localhost, resourceCapacity=4, usedResource=0]
    |--PhysicalNode[id=3, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |--PhysicalNode[id=7, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=14, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=14, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=13, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |--PhysicalNode[id=6, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=14, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=13, ip=localhost, resourceCapacity=4, usedResource=0]
    |--PhysicalNode[id=2, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |--PhysicalNode[id=6, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=14, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=13, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |--PhysicalNode[id=5, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=13, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=12, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |  |--PhysicalNode[id=12, ip=localhost, resourceCapacity=4, usedResource=0]
    |--PhysicalNode[id=1, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |--PhysicalNode[id=8, ip=localhost, resourceCapacity=4, usedResource=0]
    |  |  |--PhysicalNode[id=12, ip=localhost, resourceCapacity=4, usedResource=0]
*/
/*TEST_F(TopologyTest, testPathFindingWithMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // create workers
    std::vector<WorkerId> topologyNodeIds;
    int resource = 4;
    for (uint32_t i = 0; i < 15; ++i) {
        topology->registerTopologyNode(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        topologyNodeIds.emplace_back(i);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setRootTopologyNodeId(topologyNodeIds.at(0));
    //sets up Topology
    // link each worker with its neighbor
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(1));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(2));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(3));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(4));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(8));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(5));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(6));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(3), topologyNodeIds.at(6));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(3), topologyNodeIds.at(7));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(4), topologyNodeIds.at(7));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(5), topologyNodeIds.at(8));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(5), topologyNodeIds.at(9));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(6), topologyNodeIds.at(10));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(7), topologyNodeIds.at(10));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(7), topologyNodeIds.at(11));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(8), topologyNodeIds.at(12));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(9), topologyNodeIds.at(12));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(9), topologyNodeIds.at(13));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(10), topologyNodeIds.at(13));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(10), topologyNodeIds.at(14));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(11), topologyNodeIds.at(14));

    topology->print();

    std::vector<WorkerId> sourceNodes{topologyNodeIds.at(12), topologyNodeIds.at(13), topologyNodeIds.at(14)};

    std::vector<WorkerId> destinationNodes{topologyNodeIds.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    //checks if Ids of source nodes are as expected
    EXPECT_TRUE(sourceNodes[0] == topologyNodeIds[12]);
    EXPECT_TRUE(sourceNodes[1] == topologyNodeIds[13]);
    EXPECT_TRUE(sourceNodes[2] == topologyNodeIds[14]);
    //checks path from source node 12 to sink
    TopologyNodePtr firstStartNodeParent1 = startNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent1->getId() == topologyNodeIds[8]);
    TopologyNodePtr firstStartNodeParent2 = firstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent2->getId() == topologyNodeIds[1]);
    TopologyNodePtr firstStartNodeParent3 = firstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent3->getId() == topologyNodeIds[0]);
    //checks path from source node 13 to sink
    TopologyNodePtr secondStartNodeParent1 = startNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent1->getId() == topologyNodeIds[10]);
    TopologyNodePtr secondStartNodeParent2 = secondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent2->getId() == topologyNodeIds[6]);
    TopologyNodePtr secondStartNodeParent3 = secondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent3->getId() == topologyNodeIds[2]);
    TopologyNodePtr secondStartNodeParent4 = secondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent4->getId() == topologyNodeIds[0]);
    //checks path from source node 14 to sink
    TopologyNodePtr thirdStartNodeParent1 = startNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent1->getId() == topologyNodeIds[10]);
    TopologyNodePtr thirdStartNodeParent2 = thirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent2->getId() == topologyNodeIds[6]);
    TopologyNodePtr thirdStartNodeParent3 = thirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent3->getId() == topologyNodeIds[2]);
    TopologyNodePtr thirdStartNodeParent4 = thirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent4->getId() == topologyNodeIds[0]);
    //flags nodes currently on path for maintenance
    topologyNodeIds[1]->setForMaintenance(true);
    topologyNodeIds[3]->setForMaintenance(true);
    topologyNodeIds[10]->setForMaintenance(true);
    //calculate Path again
    const std::vector<TopologyNodePtr> mStartNodes = topology->findPathBetween(sourceNodes, destinationNodes);
    //checks path from source node 12 to sink
    TopologyNodePtr mFirstStartNodeParent1 = mStartNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent1->getId() == topologyNodeIds[9]);
    TopologyNodePtr mFirstStartNodeParent2 = mFirstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent2->getId() == topologyNodeIds[5]);
    TopologyNodePtr mFirstStartNodeParent3 = mFirstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent3->getId() == topologyNodeIds[2]);
    TopologyNodePtr mFirstStartNodeParent4 = mFirstStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent4->getId() == topologyNodeIds[0]);
    //checks path from source node 13 to sink
    TopologyNodePtr mSecondStartNodeParent1 = mStartNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent1->getId() == topologyNodeIds[9]);
    TopologyNodePtr mSecondStartNodeParent2 = mSecondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent2->getId() == topologyNodeIds[5]);
    TopologyNodePtr mSecondStartNodeParent3 = mSecondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent3->getId() == topologyNodeIds[2]);
    TopologyNodePtr mSecondStartNodeParent4 = mSecondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent4->getId() == topologyNodeIds[0]);
    //checks path from source node 14 to sink
    TopologyNodePtr mThirdStartNodeParent1 = mStartNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent1->getId() == topologyNodeIds[11]);
    TopologyNodePtr mThirdStartNodeParent2 = mThirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent2->getId() == topologyNodeIds[7]);
    TopologyNodePtr mThirdStartNodeParent3 = mThirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent3->getId() == topologyNodeIds[4]);
    TopologyNodePtr mThirdStartNodeParent4 = mThirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent4->getId() == topologyNodeIds[0]);
}*/

/**
 * @brief Tests if findCommonAncestor properly ignores nodes marked for maintenance
 */
TEST_F(TopologyTest, testFincCommonAncestorWithMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // create workers
    std::vector<WorkerId> topologyNodeIds;
    int resource = 4;
    for (uint32_t i = 0; i <= 6; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        topologyNodeIds.emplace_back(i + 1);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setRootTopologyNodeId(topologyNodeIds.at(0));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(1));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(2));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(3));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(4));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(5));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(4));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(5));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(3), topologyNodeIds.at(4));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(3), topologyNodeIds.at(5));

    topology->print();

    auto sourceTopologyNodeIds = {topologyNodeIds.at(4), topologyNodeIds.at(5)};
    auto destinationTopologyNodeIds = {topologyNodeIds.at(0)};

    PathFinder pathFinder(topology->getRootTopologyNodeId());

    auto copyTopology = topology->findPathBetween(sourceTopologyNodeIds, destinationTopologyNodeIds);
    auto commonAncestor = pathFinder.findCommonAncestor(copyTopology);
    EXPECT_EQ(commonAncestor->getId(), topologyNodeIds.at(1));
    topology->setForMaintenance(topologyNodeIds.at(1), true);

    copyTopology = topology->findPathBetween(sourceTopologyNodeIds, destinationTopologyNodeIds);
    commonAncestor = pathFinder.findCommonAncestor(copyTopology);
    EXPECT_EQ(commonAncestor->getId(), topologyNodeIds.at(2));
    topology->setForMaintenance(topologyNodeIds.at(2), true);

    copyTopology = topology->findPathBetween(sourceTopologyNodeIds, destinationTopologyNodeIds);
    commonAncestor = pathFinder.findCommonAncestor(copyTopology);
    EXPECT_EQ(commonAncestor->getId(), topologyNodeIds.at(3));
    topology->setForMaintenance(topologyNodeIds.at(3), true);

    copyTopology = topology->findPathBetween(sourceTopologyNodeIds, destinationTopologyNodeIds);
    commonAncestor = pathFinder.findCommonAncestor(copyTopology);
    EXPECT_TRUE(commonAncestor == nullptr);
    topology->setForMaintenance(topologyNodeIds.at(1), false);

    copyTopology = topology->findPathBetween(sourceTopologyNodeIds, destinationTopologyNodeIds);
    commonAncestor = pathFinder.findCommonAncestor(copyTopology);
    EXPECT_EQ(commonAncestor->getId(), topologyNodeIds.at(1));
}

/**
 * @brief test for expected behavior of findPathBetween in conjunction with findCommonAncestor/Child
 * as well as ignoring nodes marked for maintenance.
 */
TEST_F(TopologyTest, testPathFindingBetweenAllChildAndParentNodesOfANodeMarkedForMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    // create workers
    std::vector<WorkerId> topologyNodeIds;
    int resource = 4;
    for (uint32_t i = 0; i < 9; ++i) {
        topology->registerWorker(i, "localhost", grpcPort, dataPort, resource, properties, 0, 0);
        topologyNodeIds.emplace_back(i);
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setRootTopologyNodeId(topologyNodeIds.at(0));

    // link each worker with its neighbor
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(1));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(0), topologyNodeIds.at(2));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(3));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(1), topologyNodeIds.at(5));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(3));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(2), topologyNodeIds.at(5));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(3), topologyNodeIds.at(4));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(3), topologyNodeIds.at(6));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(4), topologyNodeIds.at(7));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(5), topologyNodeIds.at(7));
    topology->addTopologyNodeAsChild(topologyNodeIds.at(5), topologyNodeIds.at(8));

    topology->addTopologyNodeAsChild(topologyNodeIds.at(6), topologyNodeIds.at(8));

    //Idea: Subquery deployed on 5 with Child Operators on nodes 7 and 8. Parent Operators on nodes 1 and 2
    //try to find a new node onto which we could potentially migrate the subqueries on node 5.
    //this node must be reachable from node 7 and 8 as well as 1 and 2.
    //In this topology the only such node is node 3
    topology->setForMaintenance(topologyNodeIds.at(5), true);

    auto childNodes = {topologyNodeIds.at(7), topologyNodeIds.at(8)};
    auto parentNodes = {topologyNodeIds.at(1), topologyNodeIds.at(2)};

    auto copyTopology = topology->findPathBetween(childNodes, parentNodes);

    PathFinder pathFinder(topology->getRootTopologyNodeId());

    auto commonAncestor = pathFinder.findCommonAncestor(copyTopology);
    auto commonChild = pathFinder.findCommonAncestor(copyTopology);
    EXPECT_TRUE(commonAncestor->getId() == 3);
    EXPECT_TRUE(commonAncestor->getId() == commonChild->getId());

    auto path = topology->findPathBetween(childNodes, parentNodes);
    EXPECT_TRUE(path.size() != 0);
}
