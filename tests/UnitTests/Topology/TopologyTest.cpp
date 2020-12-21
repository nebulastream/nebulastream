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
    void SetUp() {
        std::cout << "Setup NesTopologyManager test case." << std::endl;
        NES::setupLogging("NesTopologyManager.log", NES::LOG_DEBUG);
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Tear down NesTopologyManager test case." << std::endl; }
};
/* - Serialization ----------------------------------------------------------------- */
/**
 * Create a new node. 
 */
TEST_F(TopologyTest, createNode) {
    uint64_t invalidId = 0;

    int node1Id = 1;
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

    std::string expectedResult = "";
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