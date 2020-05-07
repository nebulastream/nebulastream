#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "../../include/Topology/NESTopologyEntry.hpp"
#include "../../include/Topology/NESTopologyLink.hpp"
#include "../../include/Topology/NESTopologyPlan.hpp"
#include "../../include/Topology/NESTopologySensorNode.hpp"
#include "../../include/Topology/NESTopologyWorkerNode.hpp"
#include "../../include/Topology/NESTopologyManager.hpp"
#include <Topology/TestTopology.hpp>
#include <Topology/NESTopologyGraph.hpp>
#include "Util/CPUCapacity.hpp"
#include "Util/Logger.hpp"

using namespace NES;

/* - nesTopologyManager ---------------------------------------------------- */
class NesTopologyManagerTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup NesTopologyManager test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        std::cout << "Setup NesTopologyManager test case." << std::endl;
        NESTopologyManager::getInstance().resetNESTopologyPlan();
        NES::setupLogging("NesTopologyManager.log", NES::LOG_DEBUG);
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down NesTopologyManager test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down NesTopologyManager test class." << std::endl;
    }
};

/* - Nodes ----------------------------------------------------------------- */
/* Create a new node. */

TEST_F(NesTopologyManagerTest, createNode) {
    size_t invalid_id = INVALID_NODE_ID;

    auto workerNode = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    EXPECT_NE(workerNode.get(), nullptr);
    EXPECT_EQ(workerNode->getEntryType(), Worker);
    EXPECT_EQ(workerNode->getEntryTypeString(), "Worker");
    EXPECT_NE(workerNode->getId(), invalid_id);

    auto sensorNode = NESTopologyManager::getInstance().createNESSensorNode(
        2, "localhost", CPUCapacity::LOW);
    EXPECT_NE(sensorNode.get(), nullptr);
    EXPECT_EQ(sensorNode->getEntryType(), Sensor);
    EXPECT_EQ(sensorNode->getEntryTypeString(),
              "Sensor(" + sensorNode->getPhysicalStreamName() + ")");
    EXPECT_NE(sensorNode->getId(), invalid_id);

    EXPECT_EQ(workerNode->getId() + 1, sensorNode->getId());
}

/* Remove an existing node. */
TEST_F(NesTopologyManagerTest, removeNode) {
    auto worker_node = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    auto result_worker = NESTopologyManager::getInstance().removeNESWorkerNode(
        worker_node);
    EXPECT_TRUE(result_worker);

    auto sensor_node = NESTopologyManager::getInstance().createNESSensorNode(
        1, "localhost", CPUCapacity::LOW);
    auto result_sensor = NESTopologyManager::getInstance().removeNESSensorNode(
        sensor_node);
    EXPECT_TRUE(result_sensor);
}

/* Remove a non-existing node. */
TEST_F(NesTopologyManagerTest, removeNonExistingNode) {
    auto workerNode = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    EXPECT_TRUE(
        NESTopologyManager::getInstance().removeNESWorkerNode(workerNode));
    EXPECT_FALSE(
        NESTopologyManager::getInstance().removeNESWorkerNode(workerNode));

    auto sensorNode = NESTopologyManager::getInstance().createNESSensorNode(
        1, "localhost", CPUCapacity::LOW);
    EXPECT_TRUE(
        NESTopologyManager::getInstance().removeNESSensorNode(sensorNode));
    EXPECT_FALSE(
        NESTopologyManager::getInstance().removeNESSensorNode(sensorNode));
}

/* - Links ----------------------------------------------------------------- */
/* Create a new link. */
TEST_F(NesTopologyManagerTest, createLink) {
    auto workerNode0 = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    auto workerNode1 = NESTopologyManager::getInstance().createNESWorkerNode(
        2, "localhost", CPUCapacity::MEDIUM);
    auto worker_node_2 = NESTopologyManager::getInstance().createNESWorkerNode(
        3, "localhost", CPUCapacity::MEDIUM);
    auto workerNode3 = NESTopologyManager::getInstance().createNESWorkerNode(
        4, "localhost", CPUCapacity::MEDIUM);

    auto sensor_node_0 = NESTopologyManager::getInstance().createNESSensorNode(
        5, "localhost", CPUCapacity::LOW);
    auto sensorNode1 = NESTopologyManager::getInstance().createNESSensorNode(
        6, "localhost", CPUCapacity::LOW);

    auto link_node_node = NESTopologyManager::getInstance().createNESTopologyLink(
        workerNode0, workerNode1, 1, 1);

    size_t notExistingLinkId = NOT_EXISTING_LINK_ID;

    EXPECT_NE(link_node_node.get(), nullptr);
    EXPECT_NE(link_node_node->getId(), notExistingLinkId);
    EXPECT_EQ(link_node_node->getSourceNode().get(), workerNode0.get());
    EXPECT_EQ(link_node_node->getSourceNodeId(), workerNode0->getId());
    EXPECT_EQ(link_node_node->getDestNode().get(), workerNode1.get());
    EXPECT_EQ(link_node_node->getDestNodeId(), workerNode1->getId());
    EXPECT_EQ(link_node_node->getLinkType(), NodeToNode);
    EXPECT_EQ(link_node_node->getLinkTypeString(), "NodeToNode");

    auto linkNodeSensor = NESTopologyManager::getInstance().createNESTopologyLink(
        worker_node_2, sensor_node_0, 1, 1);
    EXPECT_NE(linkNodeSensor.get(), nullptr);
    EXPECT_NE(linkNodeSensor->getId(), notExistingLinkId);
    EXPECT_EQ(linkNodeSensor->getSourceNode().get(), worker_node_2.get());
    EXPECT_EQ(linkNodeSensor->getSourceNodeId(), worker_node_2->getId());
    EXPECT_EQ(linkNodeSensor->getDestNode().get(), sensor_node_0.get());
    EXPECT_EQ(linkNodeSensor->getDestNodeId(), sensor_node_0->getId());
    EXPECT_EQ(linkNodeSensor->getLinkType(), NodeToSensor);
    EXPECT_EQ(linkNodeSensor->getLinkTypeString(), "NodeToSensor");

    auto linkSensorNode = NESTopologyManager::getInstance().createNESTopologyLink(
        sensorNode1, workerNode3, 1, 1);
    EXPECT_NE(linkSensorNode.get(), nullptr);
    EXPECT_NE(linkSensorNode->getId(), notExistingLinkId);
    EXPECT_EQ(linkSensorNode->getSourceNode().get(), sensorNode1.get());
    EXPECT_EQ(linkSensorNode->getSourceNodeId(), sensorNode1->getId());
    EXPECT_EQ(linkSensorNode->getDestNode().get(), workerNode3.get());
    EXPECT_EQ(linkSensorNode->getDestNodeId(), workerNode3->getId());
    EXPECT_EQ(linkSensorNode->getLinkType(), SensorToNode);
    EXPECT_EQ(linkSensorNode->getLinkTypeString(), "SensorToNode");

    EXPECT_EQ(link_node_node->getId() + 1, linkNodeSensor->getId());
    EXPECT_EQ(linkNodeSensor->getId() + 1, linkSensorNode->getId());
}

/* Create link, where a link already exists. */
TEST_F(NesTopologyManagerTest, createExistingLink) {
    auto node_0 = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    auto node_1 = NESTopologyManager::getInstance().createNESWorkerNode(
        2, "localhost", CPUCapacity::MEDIUM);
    auto link = NESTopologyManager::getInstance().createNESTopologyLink(node_0,
                                                                        node_1, 1,
                                                                        1);
    EXPECT_EQ(
        link,
        NESTopologyManager::getInstance().createNESTopologyLink(node_0, node_1, 1,
                                                                1));
}

/* Remove an existing link. */
TEST_F(NesTopologyManagerTest, removeLink) {
    auto node_0 = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    auto node_1 = NESTopologyManager::getInstance().createNESWorkerNode(
        2, "localhost", CPUCapacity::MEDIUM);
    auto link = NESTopologyManager::getInstance().createNESTopologyLink(node_0,
                                                                        node_1, 1,
                                                                        1);

    EXPECT_TRUE(NESTopologyManager::getInstance().removeNESTopologyLink(link));
}

/* Remove a non-existing link. */
TEST_F(NesTopologyManagerTest, removeNonExistingLink) {
    auto node_0 = NESTopologyManager::getInstance().createNESWorkerNode(
        1, "localhost", CPUCapacity::MEDIUM);
    auto node_1 = NESTopologyManager::getInstance().createNESWorkerNode(
        2, "localhost", CPUCapacity::MEDIUM);
    auto link = NESTopologyManager::getInstance().createNESTopologyLink(node_0,
                                                                        node_1, 1,
                                                                        1);

    EXPECT_TRUE(NESTopologyManager::getInstance().removeNESTopologyLink(link));
    EXPECT_FALSE(NESTopologyManager::getInstance().removeNESTopologyLink(link));

    // What happens to a link, if one node was removed?
    // Expectation: Link is removed as well.
    link = NESTopologyManager::getInstance().createNESTopologyLink(node_0, node_1,
                                                                   1, 1);
    EXPECT_TRUE(NESTopologyManager::getInstance().removeNESWorkerNode(node_0));
    EXPECT_FALSE(NESTopologyManager::getInstance().removeNESTopologyLink(link));
}

/* - Usage Pattern --------------------------------------------------------- */
TEST_F(NesTopologyManagerTest, manyNodes) {
    // creater workers
    std::vector<std::shared_ptr<NESTopologyWorkerNode>> workers;
    for (uint32_t i = 0; i < 15; ++i) {
        workers.push_back(
            NESTopologyManager::getInstance().createNESWorkerNode(
                i, "localhost", CPUCapacity::MEDIUM));
    }

    // create sensors
    std::vector<std::shared_ptr<NESTopologySensorNode>> sensors;
    for (uint32_t i = 15; i < 45; ++i) {
        sensors.push_back(
            NESTopologyManager::getInstance().createNESSensorNode(
                i, "localhost", CPUCapacity::LOW));
    }

    // remove some workers
    for (uint32_t i = 0; i < workers.size(); i += 4) {
        NESTopologyManager::getInstance().removeNESWorkerNode(workers.at(i));
    }

    // creater some workers
    for (uint32_t i = 45; i < 50; ++i) {
        workers.push_back(
            NESTopologyManager::getInstance().createNESWorkerNode(
                i, "localhost", CPUCapacity::MEDIUM));
    }

    // remove some sensors
    for (uint32_t i = 0; i < sensors.size(); i += 3) {
        NESTopologyManager::getInstance().removeNESSensorNode(sensors.at(i));
    }

    // create some sensors
    for (uint32_t i = 50; i < 60; ++i) {
        sensors.push_back(
            NESTopologyManager::getInstance().createNESSensorNode(
                i, "localhost", CPUCapacity::LOW));
    }
}

TEST_F(NesTopologyManagerTest, manyLinks) {
    // creater workers
    std::vector<std::shared_ptr<NESTopologyWorkerNode>> workers;
    for (uint32_t i = 0; i < 15; ++i) {
        workers.push_back(
            NESTopologyManager::getInstance().createNESWorkerNode(
                i, "localhost", CPUCapacity::MEDIUM));
    }

    // create sensors
    std::vector<std::shared_ptr<NESTopologySensorNode>> sensors;
    for (uint32_t i = 15; i < 45; ++i) {
        sensors.push_back(
            NESTopologyManager::getInstance().createNESSensorNode(
                i, "localhost", CPUCapacity::LOW));
    }

    // link each worker with all other workers
    std::vector<std::shared_ptr<NESTopologyLink>> links;
    for (uint32_t i = 0; i != 15; ++i) {
        for (uint32_t j = i; j != 15; ++j) {
            if (i != j) {
                links.push_back(
                    NESTopologyManager::getInstance().createNESTopologyLink(
                        workers.at(i), workers.at(j), 1, 1));
            }
        }
    }

    // each worker has two sensors
    for (uint32_t i = 0; i != 30; ++i) {
        if (i%2 == 0) {
            // even sensor
            links.push_back(
                NESTopologyManager::getInstance().createNESTopologyLink(
                    sensors.at(i), workers.at(i/2), 1, 1));
        } else {
            // odd sensor
            links.push_back(
                NESTopologyManager::getInstance().createNESTopologyLink(
                    sensors.at(i), workers.at((i - 1)/2), 1, 1));
        }
    }

    // remove some links
    for (uint32_t i = 0; i < links.size(); i += 4) {
        EXPECT_TRUE(
            NESTopologyManager::getInstance().removeNESTopologyLink(links.at(i)));
    }
}

/* - Print ----------------------------------------------------------------- */
TEST_F(NesTopologyManagerTest, printGraph) {
    // creater workers
    std::vector<std::shared_ptr<NESTopologyWorkerNode>> workers;
    for (uint32_t i = 0; i < 7; ++i) {
        workers.push_back(
            NESTopologyManager::getInstance().createNESWorkerNode(
                i, "localhost", CPUCapacity::MEDIUM));
    }

    // create sensors
    std::vector<std::shared_ptr<NESTopologySensorNode>> sensors;
    for (uint32_t i = 7; i < 22; ++i) {
        sensors.push_back(
            NESTopologyManager::getInstance().createNESSensorNode(
                i, "localhost", CPUCapacity::LOW));
    }

    // link each worker with its neighbor
    std::vector<std::shared_ptr<NESTopologyLink>> links;
    links.push_back(
        NESTopologyManager::getInstance().createNESTopologyLink(workers.at(0),
                                                                workers.at(1), 1,
                                                                1));
    links.push_back(
        NESTopologyManager::getInstance().createNESTopologyLink(workers.at(2),
                                                                workers.at(1), 1,
                                                                1));

    links.push_back(
        NESTopologyManager::getInstance().createNESTopologyLink(workers.at(3),
                                                                workers.at(4), 1,
                                                                1));
    links.push_back(
        NESTopologyManager::getInstance().createNESTopologyLink(workers.at(5),
                                                                workers.at(4), 1,
                                                                1));

    links.push_back(
        NESTopologyManager::getInstance().createNESTopologyLink(workers.at(1),
                                                                workers.at(6), 1,
                                                                1));
    links.push_back(
        NESTopologyManager::getInstance().createNESTopologyLink(workers.at(4),
                                                                workers.at(6), 1,
                                                                1));

    // each worker has three sensors
    for (uint32_t i = 0; i != 15; ++i) {
        if (i%3 == 0) {
            links.push_back(
                NESTopologyManager::getInstance().createNESTopologyLink(
                    sensors.at(i), workers.at(i/3), 1, 1));
        } else if (i%3 == 1) {
            links.push_back(
                NESTopologyManager::getInstance().createNESTopologyLink(
                    sensors.at(i), workers.at((i - 1)/3), 1, 1));
        } else {
            links.push_back(
                NESTopologyManager::getInstance().createNESTopologyLink(
                    sensors.at(i), workers.at((i - 2)/3), 1, 1));
        }
    }

    std::cout << " current plan from topo="
              << NESTopologyManager::getInstance().getNESTopologyPlanString()
              << std::endl;
    std::string expected_result = "graph G {\n"
                                  "0[label=\"0 type=Worker\"];\n"
                                  "1[label=\"1 type=Worker\"];\n"
                                  "2[label=\"2 type=Worker\"];\n"
                                  "3[label=\"3 type=Worker\"];\n"
                                  "4[label=\"4 type=Worker\"];\n"
                                  "5[label=\"5 type=Worker\"];\n"
                                  "6[label=\"6 type=Worker\"];\n"
                                  "7[label=\"7 type=Sensor(default_physical)\"];\n"
                                  "8[label=\"8 type=Sensor(default_physical)\"];\n"
                                  "9[label=\"9 type=Sensor(default_physical)\"];\n"
                                  "10[label=\"10 type=Sensor(default_physical)\"];\n"
                                  "11[label=\"11 type=Sensor(default_physical)\"];\n"
                                  "12[label=\"12 type=Sensor(default_physical)\"];\n"
                                  "13[label=\"13 type=Sensor(default_physical)\"];\n"
                                  "14[label=\"14 type=Sensor(default_physical)\"];\n"
                                  "15[label=\"15 type=Sensor(default_physical)\"];\n"
                                  "16[label=\"16 type=Sensor(default_physical)\"];\n"
                                  "17[label=\"17 type=Sensor(default_physical)\"];\n"
                                  "18[label=\"18 type=Sensor(default_physical)\"];\n"
                                  "19[label=\"19 type=Sensor(default_physical)\"];\n"
                                  "20[label=\"20 type=Sensor(default_physical)\"];\n"
                                  "21[label=\"21 type=Sensor(default_physical)\"];\n"
                                  "0--1 [label=\"0\"];\n"
                                  "2--1 [label=\"1\"];\n"
                                  "3--4 [label=\"2\"];\n"
                                  "5--4 [label=\"3\"];\n"
                                  "1--6 [label=\"4\"];\n"
                                  "4--6 [label=\"5\"];\n"
                                  "7--0 [label=\"6\"];\n"
                                  "8--0 [label=\"7\"];\n"
                                  "9--0 [label=\"8\"];\n"
                                  "10--1 [label=\"9\"];\n"
                                  "11--1 [label=\"10\"];\n"
                                  "12--1 [label=\"11\"];\n"
                                  "13--2 [label=\"12\"];\n"
                                  "14--2 [label=\"13\"];\n"
                                  "15--2 [label=\"14\"];\n"
                                  "16--3 [label=\"15\"];\n"
                                  "17--3 [label=\"16\"];\n"
                                  "18--3 [label=\"17\"];\n"
                                  "19--4 [label=\"18\"];\n"
                                  "20--4 [label=\"19\"];\n"
                                  "21--4 [label=\"20\"];\n"
                                  "}\n";

    cout << "expected_result=" << expected_result << endl;

    EXPECT_EQ(NESTopologyManager::getInstance().getNESTopologyPlanString(),
              expected_result);
}

TEST_F(NesTopologyManagerTest, printGraphWithoutEdges) {

    // creater workers
    std::vector<std::shared_ptr<NESTopologyWorkerNode>> workers;
    for (uint32_t i = 0; i < 7; ++i) {
        workers.push_back(
            NESTopologyManager::getInstance().createNESWorkerNode(
                i, "localhost", CPUCapacity::MEDIUM));
    }

    // create sensors
    std::vector<std::shared_ptr<NESTopologySensorNode>> sensors;
    for (uint32_t i = 7; i < 22; ++i) {
        sensors.push_back(
            NESTopologyManager::getInstance().createNESSensorNode(
                i, "localhost", CPUCapacity::LOW));
    }

    std::cout << NESTopologyManager::getInstance().getNESTopologyPlanString()
              << std::endl;

    std::string expected_result = "graph G {\n"
                                  "0[label=\"0 type=Worker\"];\n"
                                  "1[label=\"1 type=Worker\"];\n"
                                  "2[label=\"2 type=Worker\"];\n"
                                  "3[label=\"3 type=Worker\"];\n"
                                  "4[label=\"4 type=Worker\"];\n"
                                  "5[label=\"5 type=Worker\"];\n"
                                  "6[label=\"6 type=Worker\"];\n"
                                  "7[label=\"7 type=Sensor(default_physical)\"];\n"
                                  "8[label=\"8 type=Sensor(default_physical)\"];\n"
                                  "9[label=\"9 type=Sensor(default_physical)\"];\n"
                                  "10[label=\"10 type=Sensor(default_physical)\"];\n"
                                  "11[label=\"11 type=Sensor(default_physical)\"];\n"
                                  "12[label=\"12 type=Sensor(default_physical)\"];\n"
                                  "13[label=\"13 type=Sensor(default_physical)\"];\n"
                                  "14[label=\"14 type=Sensor(default_physical)\"];\n"
                                  "15[label=\"15 type=Sensor(default_physical)\"];\n"
                                  "16[label=\"16 type=Sensor(default_physical)\"];\n"
                                  "17[label=\"17 type=Sensor(default_physical)\"];\n"
                                  "18[label=\"18 type=Sensor(default_physical)\"];\n"
                                  "19[label=\"19 type=Sensor(default_physical)\"];\n"
                                  "20[label=\"20 type=Sensor(default_physical)\"];\n"
                                  "21[label=\"21 type=Sensor(default_physical)\"];\n"
                                  "}\n";

    EXPECT_TRUE(
        NESTopologyManager::getInstance().getNESTopologyPlanString()
            == expected_result);
}

TEST_F(NesTopologyManagerTest, printGraphWithoutAnything) {

    std::string expected_result = "graph G {\n}\n";
    EXPECT_TRUE(
        NESTopologyManager::getInstance().getNESTopologyPlanString()
            == expected_result);
}

/* ------------------------------------------------------------------------- */
/* - NesTopologyGraph ------------------------------------------------------ */
class NesTopologyGraphTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup NesTopologyGraph test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        std::cout << "Setup NesTopologyGraph test case." << std::endl;
        nes_graph = std::make_shared<NESTopologyGraph>();
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup NesTopologyGraph test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down NesTopologyGraph test class." << std::endl;
    }
    std::shared_ptr<NESTopologyGraph> nes_graph;
};

/* - Vertices -------------------------------------------------------------- */
TEST_F(NesTopologyGraphTest, addVertex) {
    auto worker_node = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(worker_node));

    auto sensor_node = std::make_shared<NESTopologySensorNode>(1, "addr2");
    EXPECT_TRUE(nes_graph->addVertex(sensor_node));
}

TEST_F(NesTopologyGraphTest, addExistingVertex) {
    auto worker_node = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(worker_node));
    EXPECT_FALSE(nes_graph->addVertex(worker_node));
}

TEST_F(NesTopologyGraphTest, removeVertex) {
    auto worker_node = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(worker_node));

    auto sensor_node = std::make_shared<NESTopologySensorNode>(1, "addr2");
    EXPECT_TRUE(nes_graph->addVertex(sensor_node));

    EXPECT_TRUE(nes_graph->removeVertex(worker_node->getId()));
    EXPECT_TRUE(nes_graph->removeVertex(sensor_node->getId()));
}

TEST_F(NesTopologyGraphTest, removeNonExistingVertex) {
    auto worker_node = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    nes_graph->addVertex(worker_node);

    EXPECT_TRUE(nes_graph->removeVertex(worker_node->getId()));
    EXPECT_FALSE(nes_graph->removeVertex(worker_node->getId()));
}

/* - Edges ----------------------------------------------------------------- */
TEST_F(NesTopologyGraphTest, addEdge) {
    auto workerNode = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(workerNode));

    auto sensorNode = std::make_shared<NESTopologySensorNode>(1, "addr2");
    EXPECT_TRUE(nes_graph->addVertex(sensorNode));

    auto link_0 = std::make_shared<NESTopologyLink>(0, sensorNode, workerNode, 1,
                                                    1);
    auto link_1 = std::make_shared<NESTopologyLink>(1, workerNode, sensorNode, 1,
                                                    1);

    EXPECT_TRUE(nes_graph->addEdge(link_0));
    EXPECT_TRUE(nes_graph->addEdge(link_1));
}

TEST_F(NesTopologyGraphTest, addExistingEdge) {
    auto workerNode = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(workerNode));

    auto sensorNode = std::make_shared<NESTopologySensorNode>(1, "addr2");
    EXPECT_TRUE(nes_graph->addVertex(sensorNode));

    auto link_0 = std::make_shared<NESTopologyLink>(0, sensorNode, workerNode, 1,
                                                    1);
    auto link_1 = std::make_shared<NESTopologyLink>(1, sensorNode, workerNode, 1,
                                                    1);
    EXPECT_TRUE(nes_graph->addEdge(link_0));
    EXPECT_FALSE(nes_graph->addEdge(link_0));
    EXPECT_FALSE(nes_graph->addEdge(link_1));
}

TEST_F(NesTopologyGraphTest, addInvalidEdge) {
    auto workerNode = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(workerNode));

    auto sensorNode = std::make_shared<NESTopologySensorNode>(1, "addr2");

    auto link_0 = std::make_shared<NESTopologyLink>(0, workerNode, sensorNode, 1,
                                                    1);
    EXPECT_FALSE(nes_graph->addEdge(link_0));
}

TEST_F(NesTopologyGraphTest, removeEdge) {
    auto workerNode = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(workerNode));

    auto sensorNode = std::make_shared<NESTopologySensorNode>(1, "addr2");
    EXPECT_TRUE(nes_graph->addVertex(sensorNode));

    auto link_0 = std::make_shared<NESTopologyLink>(0, sensorNode, workerNode, 1,
                                                    1);
    nes_graph->addEdge(link_0);

    EXPECT_TRUE(nes_graph->removeEdge(link_0->getId()));
}

TEST_F(NesTopologyGraphTest, removeNonExistingEdge) {
    auto workerNode = std::make_shared<NESTopologyWorkerNode>(0, "addr1");
    EXPECT_TRUE(nes_graph->addVertex(workerNode));

    auto sensorNode = std::make_shared<NESTopologySensorNode>(1, "addr2");
    EXPECT_TRUE(nes_graph->addVertex(sensorNode));

    auto link_0 = std::make_shared<NESTopologyLink>(0, sensorNode, workerNode, 1,
                                                    1);
    EXPECT_TRUE(nes_graph->addEdge(link_0));

    EXPECT_TRUE(nes_graph->removeEdge(link_0->getId()));
    EXPECT_FALSE(nes_graph->removeEdge(link_0->getId()));
}

TEST_F(NesTopologyGraphTest, getExampleTopologyAsJson) {
    NESTopologyManager& topologyManager = NESTopologyManager::getInstance();
    createExampleTopology();
    const json::value& treeJson = topologyManager.getNESTopologyGraphAsJson();
    EXPECT_TRUE(treeJson.size() > 0);
}
