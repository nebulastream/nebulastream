#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "Topology/FogTopologyEntry.hpp"
#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologyManager.hpp"
#include "Topology/FogTopologyPlan.hpp"
#include "Topology/FogTopologySensorNode.hpp"
#include "Topology/FogTopologyWorkerNode.hpp"

using namespace iotdb;

/* ------------------------------------------------------------------------- */
/* - FogTopologyManager ---------------------------------------------------- */
class FogTopologyManagerTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup FogTopologyManager test case." << std::endl;
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Tear down FogTopologyManager test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyManager test class." << std::endl; }

  //@Tobias please reactive the tests and find the error :)
};

/* - Nodes ----------------------------------------------------------------- */
/* Create a new node. */

TEST_F(FogTopologyManagerTest, create_node) {
  auto worker_node = FogTopologyManager::getInstance().createFogWorkerNode();
  EXPECT_NE(worker_node.get(), nullptr);
  EXPECT_EQ(worker_node->getEntryType(), Worker);
  EXPECT_EQ(worker_node->getEntryTypeString(), "Worker");
  EXPECT_NE(worker_node->getId(), INVALID_NODE_ID);

  auto sensor_node = FogTopologyManager::getInstance().createFogSensorNode();
  EXPECT_NE(sensor_node.get(), nullptr);
  EXPECT_EQ(sensor_node->getEntryType(), Sensor);
  EXPECT_EQ(sensor_node->getEntryTypeString(), "Sensor");
  EXPECT_NE(sensor_node->getId(), INVALID_NODE_ID);

  EXPECT_EQ(worker_node->getId() + 1, sensor_node->getId());
}
#if 0
/* Remove an existing node. */
TEST_F(FogTopologyManagerTest, remove_node) {
  auto worker_node = FogTopologyManager::getInstance().createFogWorkerNode();
  auto result_worker = FogTopologyManager::getInstance().removeFogWorkerNode(worker_node);
  EXPECT_TRUE(result_worker);

  auto sensor_node = FogTopologyManager::getInstance().createFogSensorNode();
  auto result_sensor = FogTopologyManager::getInstance().removeFogSensorNode(sensor_node);
  EXPECT_TRUE(result_sensor);
}

/* Remove a non existing node. */
TEST_F(FogTopologyManagerTest, remove_non_existing_node) {
  auto worker_node = FogTopologyManager::getInstance().createFogWorkerNode();
  auto result_worker = FogTopologyManager::getInstance().removeFogWorkerNode(worker_node);
  result_worker = FogTopologyManager::getInstance().removeFogWorkerNode(worker_node);
  EXPECT_FALSE(result_worker);

  auto sensor_node = FogTopologyManager::getInstance().createFogSensorNode();
  auto result_sensor = FogTopologyManager::getInstance().removeFogSensorNode(sensor_node);
  result_sensor = FogTopologyManager::getInstance().removeFogSensorNode(sensor_node);
  EXPECT_FALSE(result_sensor);
}

/* - Links ----------------------------------------------------------------- */
/* Create a new link. */
TEST_F(FogTopologyManagerTest, create_link) {
  auto worker_node_0 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto worker_node_1 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto worker_node_2 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto worker_node_3 = FogTopologyManager::getInstance().createFogWorkerNode();

  auto sensor_node_0 = FogTopologyManager::getInstance().createFogSensorNode();
  auto sensor_node_1 = FogTopologyManager::getInstance().createFogSensorNode();

  auto link_node_node = FogTopologyManager::getInstance().createFogNodeLink(worker_node_0, worker_node_1);
  EXPECT_NE(link_node_node.get(), nullptr);
  EXPECT_NE(link_node_node->getId(), NOT_EXISTING_LINK_ID);
  EXPECT_EQ(link_node_node->getSourceNode().get(), worker_node_0.get());
  EXPECT_EQ(link_node_node->getSourceNodeId(), worker_node_0->getId());
  EXPECT_EQ(link_node_node->getDestNode().get(), worker_node_1.get());
  EXPECT_EQ(link_node_node->getDestNodeId(), worker_node_1->getId());
  EXPECT_EQ(link_node_node->getLinkType(), NodeToNode);
  EXPECT_EQ(link_node_node->getLinkTypeString(), "NodeToNode");

  auto link_node_sensor = FogTopologyManager::getInstance().createFogNodeLink(worker_node_2, sensor_node_0);
  EXPECT_NE(link_node_sensor.get(), nullptr);
  EXPECT_NE(link_node_sensor->getId(), NOT_EXISTING_LINK_ID);
  EXPECT_EQ(link_node_sensor->getSourceNode().get(), worker_node_2.get());
  EXPECT_EQ(link_node_sensor->getSourceNodeId(), worker_node_2->getId());
  EXPECT_EQ(link_node_sensor->getDestNode().get(), sensor_node_0.get());
  EXPECT_EQ(link_node_sensor->getDestNodeId(), sensor_node_0->getId());
  EXPECT_EQ(link_node_sensor->getLinkType(), NodeToSensor);
  EXPECT_EQ(link_node_sensor->getLinkTypeString(), "NodeToSensor");

  auto link_sensor_node = FogTopologyManager::getInstance().createFogNodeLink(sensor_node_1, worker_node_3);
  EXPECT_NE(link_sensor_node.get(), nullptr);
  EXPECT_NE(link_sensor_node->getId(), NOT_EXISTING_LINK_ID);
  EXPECT_EQ(link_sensor_node->getSourceNode().get(), sensor_node_1.get());
  EXPECT_EQ(link_sensor_node->getSourceNodeId(), sensor_node_1->getId());
  EXPECT_EQ(link_sensor_node->getDestNode().get(), worker_node_3.get());
  EXPECT_EQ(link_sensor_node->getDestNodeId(), worker_node_3->getId());
  EXPECT_EQ(link_sensor_node->getLinkType(), SensorToNode);
  EXPECT_EQ(link_sensor_node->getLinkTypeString(), "SensorToNode");

  EXPECT_EQ(link_node_node->getId() + 1, link_node_sensor->getId());
  EXPECT_EQ(link_node_sensor->getId() + 1, link_sensor_node->getId());
}

/* Create link, where a link already exists. */
TEST_F(FogTopologyManagerTest, create_existing_link) {
  auto node_0 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto node_1 = FogTopologyManager::getInstance().createFogWorkerNode();
  FogTopologyManager::getInstance().createFogNodeLink(node_0, node_1);
  EXPECT_DEATH(FogTopologyManager::getInstance().createFogNodeLink(node_0, node_1), "");
}

/* Remove an existing link. */
TEST_F(FogTopologyManagerTest, remove_link) {
  auto node_0 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto node_1 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto link = FogTopologyManager::getInstance().createFogNodeLink(node_0, node_1);

  auto result_link = FogTopologyManager::getInstance().removeFogNodeLink(link);
  EXPECT_TRUE(result_link);
}

/* Remove a non existing link. */
TEST_F(FogTopologyManagerTest, remove_non_existing_link) {
  auto node_0 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto node_1 = FogTopologyManager::getInstance().createFogWorkerNode();
  auto link = FogTopologyManager::getInstance().createFogNodeLink(node_0, node_1);

  auto result_link = FogTopologyManager::getInstance().removeFogNodeLink(link);
  result_link = FogTopologyManager::getInstance().removeFogNodeLink(link);
  EXPECT_FALSE(result_link);

  // What happens to a link, if one node was removed?
  // Expectation: Link is removed as well.
  link = FogTopologyManager::getInstance().createFogNodeLink(node_0, node_1);
  auto result_node = FogTopologyManager::getInstance().removeFogWorkerNode(node_0);
  EXPECT_TRUE(result_node);
  result_link = FogTopologyManager::getInstance().removeFogNodeLink(link);
  EXPECT_FALSE(result_link);
}

/* - Usage Pattern --------------------------------------------------------- */
TEST_F(FogTopologyManagerTest, many_nodes) {
  // creater workers
  std::vector<std::shared_ptr<FogTopologyWorkerNode>> workers;
  for (uint32_t i = 0; i != 15; ++i) {
    workers.push_back(FogTopologyManager::getInstance().createFogWorkerNode());
  }

  // create sensors
  std::vector<std::shared_ptr<FogTopologySensorNode>> sensors;
  for (uint32_t i = 0; i != 30; ++i) {
    sensors.push_back(FogTopologyManager::getInstance().createFogSensorNode());
  }

  // remove some workers
  for (uint32_t i = 0; i < workers.size(); i += 4) {
    FogTopologyManager::getInstance().removeFogWorkerNode(workers.at(i));
  }

  // creater some workers
  for (uint32_t i = 0; i != 5; ++i) {
    workers.push_back(FogTopologyManager::getInstance().createFogWorkerNode());
  }

  // remove some sensors
  for (uint32_t i = 0; i < sensors.size(); i += 3) {
    FogTopologyManager::getInstance().removeFogSensorNode(sensors.at(i));
  }

  // create some sensors
  for (uint32_t i = 0; i != 10; ++i) {
    sensors.push_back(FogTopologyManager::getInstance().createFogSensorNode());
  }
}

TEST_F(FogTopologyManagerTest, many_links) {

  // creater workers
  std::vector<std::shared_ptr<FogTopologyWorkerNode>> workers;
  for (uint32_t i = 0; i != 15; ++i) {
    workers.push_back(FogTopologyManager::getInstance().createFogWorkerNode());
  }

  // create sensors
  std::vector<std::shared_ptr<FogTopologySensorNode>> sensors;
  for (uint32_t i = 0; i != 30; ++i) {
    sensors.push_back(FogTopologyManager::getInstance().createFogSensorNode());
  }

  // link each worker with all other workers
  std::vector<std::shared_ptr<FogTopologyLink>> links;
  for (uint32_t i = 0; i != 15; ++i) {
    for (uint32_t j = 0; j != 15; ++j) {
      if (i != j) {
        links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(i), workers.at(j)));
      }
    }
  }

  // each worker has two sensors
  for (uint32_t i = 0; i != 30; ++i) {
    if (i % 2 == 0) {
      // even sensor
      links.push_back(FogTopologyManager::getInstance().createFogNodeLink(sensors.at(i), workers.at(i / 2)));
    } else {
      // odd sensor
      links.push_back(FogTopologyManager::getInstance().createFogNodeLink(sensors.at(i), workers.at((i - 1) / 2)));
    }
  }

  // remove some links
  for (uint32_t i = 0; i < links.size(); i += 4) {
    EXPECT_TRUE(FogTopologyManager::getInstance().removeFogNodeLink(links.at(i)));
  }
}

/* - Print ----------------------------------------------------------------- */
TEST_F(FogTopologyManagerTest, print_graph) {
  // creater workers
  std::vector<std::shared_ptr<FogTopologyWorkerNode>> workers;
  for (uint32_t i = 0; i != 7; ++i) {
    workers.push_back(FogTopologyManager::getInstance().createFogWorkerNode());
  }

  // create sensors
  std::vector<std::shared_ptr<FogTopologySensorNode>> sensors;
  for (uint32_t i = 0; i != 15; ++i) {
    sensors.push_back(FogTopologyManager::getInstance().createFogSensorNode());
  }

  // link each worker with its neighbor
  std::vector<std::shared_ptr<FogTopologyLink>> links;
  links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(0), workers.at(1)));
  links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(2), workers.at(1)));

  links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(3), workers.at(4)));
  links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(5), workers.at(4)));

  links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(1), workers.at(6)));
  links.push_back(FogTopologyManager::getInstance().createFogNodeLink(workers.at(4), workers.at(6)));

  // each worker has three sensors
  for (uint32_t i = 0; i != 15; ++i) {
    if (i % 3 == 0) {
      links.push_back(FogTopologyManager::getInstance().createFogNodeLink(sensors.at(i), workers.at(i / 3)));
    } else if (i % 3 == 1) {
      links.push_back(FogTopologyManager::getInstance().createFogNodeLink(sensors.at(i), workers.at((i - 1) / 3)));
    } else {
      links.push_back(FogTopologyManager::getInstance().createFogNodeLink(sensors.at(i), workers.at((i - 2) / 3)));
    }
  }

  std::string expected_result =
      "graph G {\n0[label=\"0 type=Worker\"];\n1[label=\"1 type=Worker\"];\n"
      "2[label=\"2 type=Worker\"];\n3[label=\"3 type=Worker\"];\n"
      "4[label=\"4 type=Worker\"];\n5[label=\"5 type=Worker\"];\n"
      "6[label=\"6 type=Worker\"];\n7[label=\"7 type=Sensor\"];\n"
      "8[label=\"8 type=Sensor\"];\n9[label=\"9 type=Sensor\"];\n"
      "10[label=\"10 type=Sensor\"];\n11[label=\"11 type=Sensor\"];\n"
      "12[label=\"12 type=Sensor\"];\n13[label=\"13 type=Sensor\"];\n"
      "14[label=\"14 type=Sensor\"];\n15[label=\"15 type=Sensor\"];\n"
      "16[label=\"16 type=Sensor\"];\n17[label=\"17 type=Sensor\"];\n"
      "18[label=\"18 type=Sensor\"];\n19[label=\"19 type=Sensor\"];\n"
      "20[label=\"20 type=Sensor\"];\n21[label=\"21 type=Sensor\"];\n"
      "0--1 [label=\"248\"];\n2--1 [label=\"249\"];\n3--4 [label=\"250\"];\n5--4 [label=\"251\"];\n"
      "1--6 [label=\"252\"];\n4--6 [label=\"253\"];\n7--0 [label=\"254\"];\n8--0 [label=\"255\"];\n"
      "9--0 [label=\"256\"];\n10--1 [label=\"257\"];\n11--1 [label=\"258\"];\n12--1 [label=\"259\"];\n"
      "13--2 [label=\"260\"];\n14--2 [label=\"261\"];\n15--2 [label=\"262\"];\n16--3 [label=\"263\"];\n"
      "17--3 [label=\"264\"];\n18--3 [label=\"265\"];\n19--4 [label=\"266\"];\n20--4 [label=\"267\"];\n"
      "21--4 [label=\"268\"];\n}\n";

  // std::cout << FogTopologyManager::getInstance().getTopologyPlanString() << std::endl;
  // std::cout << expected_result << std::endl;

  EXPECT_TRUE(FogTopologyManager::getInstance().getTopologyPlanString() == expected_result);
}

TEST_F(FogTopologyManagerTest, print_graph_without_edges) {
  // creater workers
  std::vector<std::shared_ptr<FogTopologyWorkerNode>> workers;
  for (uint32_t i = 0; i != 7; ++i) {
    workers.push_back(FogTopologyManager::getInstance().createFogWorkerNode());
  }

  // create sensors
  std::vector<std::shared_ptr<FogTopologySensorNode>> sensors;
  for (uint32_t i = 0; i != 15; ++i) {
    sensors.push_back(FogTopologyManager::getInstance().createFogSensorNode());
  }

  std::string expected_result = "graph G {\n0[label=\"0 type=Worker\"];\n1[label=\"1 type=Worker\"];\n"
                                "2[label=\"2 type=Worker\"];\n3[label=\"3 type=Worker\"];\n"
                                "4[label=\"4 type=Worker\"];\n5[label=\"5 type=Worker\"];\n"
                                "6[label=\"6 type=Worker\"];\n7[label=\"7 type=Sensor\"];\n"
                                "8[label=\"8 type=Sensor\"];\n9[label=\"9 type=Sensor\"];\n"
                                "10[label=\"10 type=Sensor\"];\n11[label=\"11 type=Sensor\"];\n"
                                "12[label=\"12 type=Sensor\"];\n13[label=\"13 type=Sensor\"];\n"
                                "14[label=\"14 type=Sensor\"];\n15[label=\"15 type=Sensor\"];\n"
                                "16[label=\"16 type=Sensor\"];\n17[label=\"17 type=Sensor\"];\n"
                                "18[label=\"18 type=Sensor\"];\n19[label=\"19 type=Sensor\"];\n"
                                "20[label=\"20 type=Sensor\"];\n21[label=\"21 type=Sensor\"];\n}\n";

  // std::cout << FogTopologyManager::getInstance().getTopologyPlanString() << std::endl;
  // std::cout << expected_result << std::endl;

  EXPECT_TRUE(FogTopologyManager::getInstance().getTopologyPlanString() == expected_result);
}

TEST_F(FogTopologyManagerTest, print_graph_without_anything) {
  std::string expected_result = "graph G {\n}\n";

  // std::cout << FogTopologyManager::getInstance().getTopologyPlanString() << std::endl;
  // std::cout << expected_result << std::endl;

  EXPECT_TRUE(FogTopologyManager::getInstance().getTopologyPlanString() == expected_result);
}

/* ------------------------------------------------------------------------- */
/* - FogTopologyGraph ------------------------------------------------------ */
class FogTopologyGraphTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyGraph test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup FogTopologyGraph test case." << std::endl;
    fog_graph = std::make_shared<FogGraph>();
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyGraph test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyGraph test class." << std::endl; }
  std::shared_ptr<FogGraph> fog_graph;
};

/* - Vertices -------------------------------------------------------------- */
TEST_F(FogTopologyGraphTest, add_vertex) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  fog_graph->addVertex(sensor_node);
}

TEST_F(FogTopologyGraphTest, add_existing_vertex) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);
  EXPECT_DEATH(fog_graph->addVertex(worker_node), "");

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(0);
  EXPECT_DEATH(fog_graph->addVertex(sensor_node), "");
}

TEST_F(FogTopologyGraphTest, remove_vertex) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  fog_graph->addVertex(sensor_node);

  EXPECT_TRUE(fog_graph->removeVertex(worker_node->getId()));
  EXPECT_TRUE(fog_graph->removeVertex(sensor_node->getId()));
}

TEST_F(FogTopologyGraphTest, remove_non_existing_vertex) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  EXPECT_TRUE(fog_graph->removeVertex(worker_node->getId()));
  EXPECT_FALSE(fog_graph->removeVertex(worker_node->getId()));

  EXPECT_FALSE(fog_graph->removeVertex(INVALID_NODE_ID));
}

/* - Edges ----------------------------------------------------------------- */
TEST_F(FogTopologyGraphTest, add_edge) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  fog_graph->addVertex(sensor_node);

  auto link_0 = std::make_shared<FogTopologyLink>(sensor_node, worker_node);
  auto link_1 = std::make_shared<FogTopologyLink>(worker_node, sensor_node);

  fog_graph->addEdge(link_0);
  fog_graph->addEdge(link_1);
}

TEST_F(FogTopologyGraphTest, add_existing_edge) {

  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  fog_graph->addVertex(sensor_node);

  auto link_0 = std::make_shared<FogTopologyLink>(sensor_node, worker_node);

  fog_graph->addEdge(link_0);
  EXPECT_DEATH(fog_graph->addEdge(link_0), "");
}

TEST_F(FogTopologyGraphTest, add_invalid_edge) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  // node not added to graph

  auto link_0 = std::make_shared<FogTopologyLink>(worker_node, sensor_node);
  EXPECT_DEATH(fog_graph->addEdge(link_0), "");
}

TEST_F(FogTopologyGraphTest, remove_edge) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  fog_graph->addVertex(sensor_node);

  auto link_0 = std::make_shared<FogTopologyLink>(sensor_node, worker_node);
  fog_graph->addEdge(link_0);

  EXPECT_TRUE(fog_graph->removeEdge(link_0->getId()));
}

TEST_F(FogTopologyGraphTest, remove_non_existing_edge) {
  auto worker_node = std::make_shared<FogTopologyWorkerNode>();
  worker_node->setId(0);
  fog_graph->addVertex(worker_node);

  auto sensor_node = std::make_shared<FogTopologySensorNode>();
  sensor_node->setId(1);
  fog_graph->addVertex(sensor_node);

  auto link_0 = std::make_shared<FogTopologyLink>(sensor_node, worker_node);
  fog_graph->addEdge(link_0);

  EXPECT_TRUE(fog_graph->removeEdge(link_0->getId()));
  EXPECT_FALSE(fog_graph->removeEdge(link_0->getId()));
}
#endif
