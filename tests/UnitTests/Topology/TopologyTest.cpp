/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include "gtest/gtest.h"
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <cstddef>
#include <iostream>

using namespace NES;

/* - TopologyTest ---------------------------------------------------- */
class TopologyTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup NesTopologyManager test case." << std::endl;
        NES::setupLogging("NesTopologyManager.log", NES::LOG_DEBUG);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down NesTopologyManager test case." << std::endl; }
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
    auto physicalNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
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

    TopologyNodePtr root = topology->getRoot();
    EXPECT_FALSE(root);

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    auto physicalNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(physicalNode);

    bool success = topology->removePhysicalNode(physicalNode);
    EXPECT_FALSE(success);
}

/**
 * Remove an existing node.
 */
TEST_F(TopologyTest, removeAnExistingNode) {
    TopologyPtr topology = Topology::create();

    TopologyNodePtr root = topology->getRoot();
    EXPECT_FALSE(root);

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);

    bool success = topology->addNewPhysicalNodeAsChild(rootNode, childNode);
    EXPECT_TRUE(success);

    success = topology->removePhysicalNode(childNode);
    EXPECT_TRUE(success);
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
    auto physicalNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);

    EXPECT_FALSE(topology->removePhysicalNode(physicalNode));
}

/* Create a new link. */
TEST_F(TopologyTest, createLink) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode1 = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);
    bool success = topology->addNewPhysicalNodeAsChild(rootNode, childNode1);
    EXPECT_TRUE(success);
    EXPECT_TRUE(rootNode->containAsChild(childNode1));

    int node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode2 = TopologyNode::create(node3Id, node3Address, grpcPort, dataPort, resources);
    success = topology->addNewPhysicalNodeAsChild(childNode1, childNode2);
    EXPECT_TRUE(success);
    EXPECT_TRUE(childNode1->containAsChild(childNode2));
}

/* Create link, where a link already exists. */
TEST_F(TopologyTest, createExistingLink) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode1 = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);
    bool success = topology->addNewPhysicalNodeAsChild(rootNode, childNode1);
    EXPECT_TRUE(success);
    EXPECT_TRUE(rootNode->containAsChild(childNode1));

    success = topology->addNewPhysicalNodeAsChild(rootNode, childNode1);
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
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode1 = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);
    bool success = topology->addNewPhysicalNodeAsChild(rootNode, childNode1);
    EXPECT_TRUE(success);
    EXPECT_TRUE(rootNode->containAsChild(childNode1));

    success = topology->removeNodeAsChild(rootNode, childNode1);
    EXPECT_TRUE(success);
    EXPECT_FALSE(rootNode->containAsChild(childNode1));
}

/* Remove a non-existing link. */
TEST_F(TopologyTest, removeNonExistingLink) {
    TopologyPtr topology = Topology::create();

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode1 = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);
    bool success = topology->removeNodeAsChild(rootNode, childNode1);
    EXPECT_FALSE(success);
    EXPECT_FALSE(rootNode->containAsChild(childNode1));
}

///* - Print ----------------------------------------------------------------- */
TEST_F(TopologyTest, printGraph) {

    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // creater workers
    std::vector<TopologyNodePtr> workers;
    int resource = 4;
    for (uint32_t i = 0; i < 7; ++i) {
        workers.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    // create sensors
    std::vector<TopologyNodePtr> sensors;
    for (uint32_t i = 7; i < 23; ++i) {
        sensors.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(workers.at(0));

    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(workers.at(0), workers.at(1));
    topology->addNewPhysicalNodeAsChild(workers.at(0), workers.at(2));

    topology->addNewPhysicalNodeAsChild(workers.at(1), workers.at(3));
    topology->addNewPhysicalNodeAsChild(workers.at(1), workers.at(4));

    topology->addNewPhysicalNodeAsChild(workers.at(2), workers.at(5));
    topology->addNewPhysicalNodeAsChild(workers.at(2), workers.at(6));

    // each worker has three sensors
    for (uint32_t i = 0; i < 15; i++) {
        if (i < 4) {
            topology->addNewPhysicalNodeAsChild(workers.at(3), sensors.at(i));
        } else if (i >= 4 && i < 8) {
            topology->addNewPhysicalNodeAsChild(workers.at(4), sensors.at(i));
        } else if (i >= 8 && i < 12) {
            topology->addNewPhysicalNodeAsChild(workers.at(5), sensors.at(i));
        } else {
            topology->addNewPhysicalNodeAsChild(workers.at(6), sensors.at(i));
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

    int node1Id = 1;
    std::string node1Address = "localhost";
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;
    uint64_t resources = 4;
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode1 = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);
    bool success = topology->addNewPhysicalNodeAsChild(rootNode, childNode1);
    EXPECT_TRUE(success);
    EXPECT_TRUE(rootNode->containAsChild(childNode1));

    int node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode2 = TopologyNode::create(node3Id, node3Address, grpcPort, dataPort, resources);
    success = topology->addNewPhysicalNodeAsChild(childNode1, childNode2);
    EXPECT_TRUE(success);
    EXPECT_TRUE(childNode1->containAsChild(childNode2));

    const TopologyNodePtr startNode = topology->findAllPathBetween(childNode1, rootNode).value();

    EXPECT_TRUE(startNode->getId() == childNode1->getId());
}

/**
 * @brief Find Path between nodes with multiple parents and children
 */
TEST_F(TopologyTest, findPathBetweenNodesWithMultipleParentsAndChildren) {

    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // creater workers
    std::vector<TopologyNodePtr> workers;
    int resource = 4;
    for (uint32_t i = 0; i < 10; ++i) {
        workers.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(workers.at(0));

    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(workers.at(0), workers.at(1));
    topology->addNewPhysicalNodeAsChild(workers.at(0), workers.at(2));

    topology->addNewPhysicalNodeAsChild(workers.at(1), workers.at(3));
    topology->addNewPhysicalNodeAsChild(workers.at(1), workers.at(4));

    topology->addNewPhysicalNodeAsChild(workers.at(2), workers.at(5));
    topology->addNewPhysicalNodeAsChild(workers.at(2), workers.at(6));

    topology->addNewPhysicalNodeAsChild(workers.at(4), workers.at(7));
    topology->addNewPhysicalNodeAsChild(workers.at(5), workers.at(7));

    topology->addNewPhysicalNodeAsChild(workers.at(7), workers.at(8));
    topology->addNewPhysicalNodeAsChild(workers.at(7), workers.at(9));

    const std::optional<TopologyNodePtr> startNode = topology->findAllPathBetween(workers.at(9), workers.at(2));

    EXPECT_TRUE(startNode.has_value());
    EXPECT_TRUE(startNode.value()->getId() == workers.at(9)->getId());
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
    auto rootNode = TopologyNode::create(node1Id, node1Address, grpcPort, dataPort, resources);
    topology->setAsRoot(rootNode);

    int node2Id = 2;
    std::string node2Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode1 = TopologyNode::create(node2Id, node2Address, grpcPort, dataPort, resources);
    bool success = topology->addNewPhysicalNodeAsChild(rootNode, childNode1);
    EXPECT_TRUE(success);
    EXPECT_TRUE(rootNode->containAsChild(childNode1));

    int node3Id = 3;
    std::string node3Address = "localhost";
    grpcPort++;
    dataPort++;
    auto childNode2 = TopologyNode::create(node3Id, node3Address, grpcPort, dataPort, resources);
    success = topology->addNewPhysicalNodeAsChild(childNode1, childNode2);
    EXPECT_TRUE(success);
    EXPECT_TRUE(childNode1->containAsChild(childNode2));

    success = topology->removeNodeAsChild(childNode1, childNode2);
    EXPECT_TRUE(success);

    const std::optional<TopologyNodePtr> startNode = topology->findAllPathBetween(childNode2, rootNode);

    EXPECT_FALSE(startNode.has_value());
}

/**
 * @brief Find Path between multiple source and destination nodes
 */
TEST_F(TopologyTest, findPathBetweenSetOfSourceAndDestinationNodes) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // creater workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 10; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));

    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(3));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(4));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(6));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(8));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(9));

    std::vector<TopologyNodePtr> sourceNodes{topologyNodes.at(8), topologyNodes.at(9)};
    std::vector<TopologyNodePtr> destinationNodes{topologyNodes.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    TopologyNodePtr startNode1 = startNodes[0];
    EXPECT_TRUE(startNode1->getId() == topologyNodes.at(8)->getId());
    TopologyNodePtr startNode2 = startNodes[1];
    EXPECT_TRUE(startNode2->getId() == topologyNodes.at(9)->getId());
    EXPECT_TRUE(startNode2->getParents().size() == startNode1->getParents().size());
    EXPECT_TRUE(startNode2->getParents()[0]->as<TopologyNode>()->getId()
                == startNode1->getParents()[0]->as<TopologyNode>()->getId());
    TopologyNodePtr s1Parent1 = startNode1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent1->getId() == topologyNodes.at(7)->getId());
    TopologyNodePtr s1Parent2 = s1Parent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent2->getId() == topologyNodes.at(4)->getId());
    TopologyNodePtr s1Parent3 = s1Parent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent3->getId() == topologyNodes.at(1)->getId());
    TopologyNodePtr s1Parent4 = s1Parent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(s1Parent4->getId() == topologyNodes.at(0)->getId());
}

/**
 * @brief Find Path between two connected nodes and select the shortest path
 */
TEST_F(TopologyTest, findPathBetweenSetOfSourceAndDestinationNodesAndSelectTheShortest) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // creater workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 10; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));

    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(3));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(4));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(5));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(6));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(8));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(9));

    std::vector<TopologyNodePtr> sourceNodes{topologyNodes.at(8)};
    std::vector<TopologyNodePtr> destinationNodes{topologyNodes.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    TopologyNodePtr startNode = startNodes[0];
    EXPECT_TRUE(startNode->getId() == topologyNodes.at(8)->getId());
    TopologyNodePtr parent1 = startNode->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(parent1->getId() == topologyNodes.at(7)->getId());
    TopologyNodePtr parent2 = parent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(parent2->getId() == topologyNodes.at(5)->getId());
    TopologyNodePtr parent3 = parent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(parent3->getId() == topologyNodes.at(0)->getId());
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
TEST_F(TopologyTest, testPathFindingWithMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 15; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));
    //sets up Topology
    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(3));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(4));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(8));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(6));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(6));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(8));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(9));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(6), topologyNodes.at(10));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(10));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(7), topologyNodes.at(11));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(8), topologyNodes.at(12));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(9), topologyNodes.at(12));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(9), topologyNodes.at(13));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(10), topologyNodes.at(13));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(10), topologyNodes.at(14));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(11), topologyNodes.at(14));

    topology->print();

    std::vector<TopologyNodePtr> sourceNodes{topologyNodes.at(12),topologyNodes.at(13),topologyNodes.at(14)};

    std::vector<TopologyNodePtr> destinationNodes{topologyNodes.at(0)};

    const std::vector<TopologyNodePtr> startNodes = topology->findPathBetween(sourceNodes, destinationNodes);

    EXPECT_FALSE(startNodes.empty());
    EXPECT_TRUE(startNodes.size() == sourceNodes.size());

    //checks if Ids of source nodes are as expected
    EXPECT_TRUE(sourceNodes[0]->getId() == topologyNodes[12]->getId());
    EXPECT_TRUE(sourceNodes[1]->getId() == topologyNodes[13]->getId());
    EXPECT_TRUE(sourceNodes[2]->getId() == topologyNodes[14]->getId());
    //checks path from source node 12 to sink
    TopologyNodePtr firstStartNodeParent1 = startNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent1->getId() == topologyNodes[8]->getId());
    TopologyNodePtr firstStartNodeParent2 = firstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent2->getId() == topologyNodes[1]->getId());
    TopologyNodePtr firstStartNodeParent3 = firstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(firstStartNodeParent3->getId() == topologyNodes[0]->getId());
    //checks path from source node 13 to sink
    TopologyNodePtr secondStartNodeParent1 = startNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent1->getId() == topologyNodes[10]->getId());
    TopologyNodePtr secondStartNodeParent2 = secondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent2->getId() == topologyNodes[6]->getId());
    TopologyNodePtr secondStartNodeParent3 = secondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr secondStartNodeParent4 = secondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(secondStartNodeParent4->getId() == topologyNodes[0]->getId());
    //checks path from source node 14 to sink
    TopologyNodePtr thirdStartNodeParent1 = startNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent1->getId() == topologyNodes[10]->getId());
    TopologyNodePtr thirdStartNodeParent2 = thirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent2->getId() == topologyNodes[6]->getId());
    TopologyNodePtr thirdStartNodeParent3 = thirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr thirdStartNodeParent4 = thirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(thirdStartNodeParent4->getId() == topologyNodes[0]->getId());
    //flags nodes currently on path for maintenance
    topologyNodes[1]->setMaintenanceFlag(true);
    topologyNodes[3]->setMaintenanceFlag(true);
    topologyNodes[10]->setMaintenanceFlag(true);
    //calculate Path again
    const std::vector<TopologyNodePtr> mStartNodes = topology->findPathBetween(sourceNodes, destinationNodes);
    //checks path from source node 12 to sink
    TopologyNodePtr mFirstStartNodeParent1 = mStartNodes[0]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent1->getId() == topologyNodes[9]->getId());
    TopologyNodePtr mFirstStartNodeParent2 = mFirstStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent2->getId() == topologyNodes[5]->getId());
    TopologyNodePtr mFirstStartNodeParent3 = mFirstStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr mFirstStartNodeParent4 = mFirstStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mFirstStartNodeParent4->getId() == topologyNodes[0]->getId());
    //checks path from source node 13 to sink
    TopologyNodePtr mSecondStartNodeParent1 = mStartNodes[1]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent1->getId() == topologyNodes[9]->getId());
    TopologyNodePtr mSecondStartNodeParent2 = mSecondStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent2->getId() == topologyNodes[5]->getId());
    TopologyNodePtr mSecondStartNodeParent3 = mSecondStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent3->getId() == topologyNodes[2]->getId());
    TopologyNodePtr mSecondStartNodeParent4 = mSecondStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mSecondStartNodeParent4->getId() == topologyNodes[0]->getId());
    //checks path from source node 14 to sink
    TopologyNodePtr mThirdStartNodeParent1 = mStartNodes[2]->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent1->getId() == topologyNodes[11]->getId());
    TopologyNodePtr mThirdStartNodeParent2 = mThirdStartNodeParent1->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent2->getId() == topologyNodes[7]->getId());
    TopologyNodePtr mThirdStartNodeParent3 = mThirdStartNodeParent2->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent3->getId() == topologyNodes[4]->getId());
    TopologyNodePtr mThirdStartNodeParent4 = mThirdStartNodeParent3->getParents()[0]->as<TopologyNode>();
    EXPECT_TRUE(mThirdStartNodeParent4->getId() == topologyNodes[0]->getId());
}
/**
 * @brief Tests if findCommonAncestor properly ignores nodes marked for maintenance
 */
TEST_F(TopologyTest, testFincCommonAncestorWithMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 6; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(3));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(4));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(5));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(4));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(4));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(5));

    topology->print();

    auto topNodes = {topologyNodes.at(4),topologyNodes.at(5)};
    auto commonAncestor = topology->findCommonAncestor(topNodes);
    EXPECT_TRUE(commonAncestor->getId() == 1);
    topology->findNodeWithId(1)->setMaintenanceFlag(true);
    commonAncestor = topology->findCommonAncestor(topNodes);
    EXPECT_TRUE(commonAncestor->getId() == 2);
    topology->findNodeWithId(2)->setMaintenanceFlag(true);
    commonAncestor = topology->findCommonAncestor(topNodes);
    EXPECT_TRUE(commonAncestor->getId() == 3);
    topology->findNodeWithId(3)->setMaintenanceFlag(true);
    commonAncestor = topology->findCommonAncestor(topNodes);
    EXPECT_TRUE(commonAncestor == nullptr);
    topology->findNodeWithId(1)->setMaintenanceFlag(false);
    commonAncestor = topology->findCommonAncestor(topNodes);
    EXPECT_TRUE(commonAncestor->getId() == 1);
}

/**
 * @brief Tests if findCommonChild properly ignores nodes marked for maintenance
 */
TEST_F(TopologyTest, testFindCommonChildWithMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 6; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));


    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(4));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(5));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(3));


    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(4));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(3));

    topology->print();

    auto topNodes = {topologyNodes.at(1),topologyNodes.at(2)};
    auto commonChild = topology->findCommonChild(topNodes);
    EXPECT_TRUE(commonChild->getId() == 4);
    topology->findNodeWithId(4)->setMaintenanceFlag(true);
    commonChild = topology->findCommonChild(topNodes);
    EXPECT_TRUE(commonChild->getId() == 5);
    topology->findNodeWithId(5)->setMaintenanceFlag(true);
    commonChild = topology->findCommonChild(topNodes);
    EXPECT_TRUE(commonChild->getId() == 3);
    topology->findNodeWithId(3)->setMaintenanceFlag(true);
    commonChild = topology->findCommonChild(topNodes);
    EXPECT_TRUE(commonChild == nullptr);
    topology->findNodeWithId(4)->setMaintenanceFlag(false);
    commonChild = topology->findCommonChild(topNodes);
    EXPECT_TRUE(commonChild->getId() == 4);
}

/**
 * @brief test for expected behavior of findPathBetween in conjunction with findCommonAncestor/Child
 * as well as ignoring nodes marked for maintenance.
 */
TEST_F(TopologyTest, testPathFindingBetweenAllChildAndParentNodesOfANodeMarkedForMaintenance) {
    TopologyPtr topology = Topology::create();

    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create workers
    std::vector<TopologyNodePtr> topologyNodes;
    int resource = 4;
    for (uint32_t i = 0; i < 9; ++i) {
        topologyNodes.push_back(TopologyNode::create(i, "localhost", grpcPort, dataPort, resource));
        grpcPort = grpcPort + 2;
        dataPort = dataPort + 2;
    }

    topology->setAsRoot(topologyNodes.at(0));

    // link each worker with its neighbor
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(1));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(0), topologyNodes.at(2));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(3));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(1), topologyNodes.at(5));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(3));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(2), topologyNodes.at(5));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(4));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(3), topologyNodes.at(6));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(4), topologyNodes.at(7));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(7));
    topology->addNewPhysicalNodeAsChild(topologyNodes.at(5), topologyNodes.at(8));

    topology->addNewPhysicalNodeAsChild(topologyNodes.at(6), topologyNodes.at(8));

    //Idea: Subquery deployed on 5 with Child Operators on nodes 7 and 8. Parent Operators on nodes 1 and 2
    //try to find a new node onto which we could potentially migrate the subqueries on node 5.
    //this node must be reachable from node 7 and 8 as well as 1 and 2.
    //In this topology the only such node is node 3
    topology->findNodeWithId(5)->setMaintenanceFlag(true);

    auto childNodes = {topologyNodes.at(7), topologyNodes.at(8)};
    auto parentNodes = {topologyNodes.at(1),topologyNodes.at(2)};
    auto commonAncestor = topology->findCommonAncestor(childNodes);
    auto commonChild = topology->findCommonChild(parentNodes);
    EXPECT_TRUE(commonAncestor->getId() == 3);
    EXPECT_TRUE(commonAncestor->getId() == commonChild->getId());

    auto path  = topology->findPathBetween(childNodes,parentNodes);
    EXPECT_TRUE(path.size() != 0);
}
