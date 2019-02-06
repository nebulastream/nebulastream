#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "Topology/FogTopologyEntry.hpp"
#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologyManager.hpp"
#include "Topology/FogTopologyPlan.hpp"
#include "Topology/FogTopologySensor.hpp"
#include "Topology/FogTopologyWorkerNode.hpp"

/* ------------------------------------------------------------------------- */
/* - FogTopologyManager ---------------------------------------------------- */
class FogTopologyManagerTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup FogTopologyManager test case." << std::endl;
    topology_manager = std::make_shared<FogTopologyManager>();
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyManager test class." << std::endl; }

  std::shared_ptr<FogTopologyManager> topology_manager;
};

/* - Nodes ----------------------------------------------------------------- */
/* Create a new node. */
TEST_F(FogTopologyManagerTest, create_node) {
  auto worker_node = topology_manager->createFogWorkerNode();
  EXPECT_NE(worker_node.get(), nullptr);
  EXPECT_EQ(worker_node->getEntryType(), Worker);
  EXPECT_EQ(worker_node->getEntryTypeString(), "Worker");
  EXPECT_NE(worker_node->getId(), INVALID_NODE_ID);

  auto sensor_node = topology_manager->createFogSensorNode();
  EXPECT_NE(sensor_node.get(), nullptr);
  EXPECT_EQ(sensor_node->getEntryType(), Sensor);
  EXPECT_EQ(sensor_node->getEntryTypeString(), "Sensor");
  EXPECT_NE(sensor_node->getId(), INVALID_NODE_ID);

  EXPECT_EQ(worker_node->getId() + 1, sensor_node->getId());
}

/* Remove an existing node. */
TEST_F(FogTopologyManagerTest, remove_node) {
  auto worker_node = topology_manager->createFogWorkerNode();
  auto result_worker = topology_manager->removeFogWorkerNode(worker_node);
  EXPECT_TRUE(result_worker);

  auto sensor_node = topology_manager->createFogSensorNode();
  auto result_sensor = topology_manager->removeFogSensorNode(sensor_node);
  EXPECT_TRUE(result_sensor);
}

/* Remove a non existing node. */
TEST_F(FogTopologyManagerTest, remove_non_existing_node) {
  auto worker_node = topology_manager->createFogWorkerNode();
  auto result_worker = topology_manager->removeFogWorkerNode(worker_node);
  result_worker = topology_manager->removeFogWorkerNode(worker_node);
  EXPECT_FALSE(result_worker);

  auto sensor_node = topology_manager->createFogSensorNode();
  auto result_sensor = topology_manager->removeFogSensorNode(sensor_node);
  result_sensor = topology_manager->removeFogSensorNode(sensor_node);
  EXPECT_FALSE(result_sensor);
}

/* - Links ----------------------------------------------------------------- */
/* Create a new link. */
TEST_F(FogTopologyManagerTest, create_link) {
  auto worker_node_0 = topology_manager->createFogWorkerNode();
  auto worker_node_1 = topology_manager->createFogWorkerNode();
  auto worker_node_2 = topology_manager->createFogWorkerNode();
  auto worker_node_3 = topology_manager->createFogWorkerNode();

  auto sensor_node_0 = topology_manager->createFogSensorNode();
  auto sensor_node_1 = topology_manager->createFogSensorNode();

  auto link_node_node = topology_manager->createFogNodeLink(worker_node_0->getId(), worker_node_1->getId());
  EXPECT_NE(link_node_node.get(), nullptr);
  EXPECT_NE(link_node_node->getId(), NOT_EXISTING_LINK_ID);
  EXPECT_EQ(link_node_node->getSourceNodeId(), worker_node_0->getId());
  EXPECT_EQ(link_node_node->getDestNodeId(), worker_node_1->getId());
  // EXPECT_EQ(link_node_node->getLinkType(), NodeToNode);          TODO? not supported yet
  // EXPECT_EQ(link_node_node->getLinkTypeString(), "NodeToNode");  TODO? not supported yet

  auto link_node_sensor = topology_manager->createFogNodeLink(worker_node_2->getId(), sensor_node_0->getId());
  EXPECT_NE(link_node_sensor.get(), nullptr);
  EXPECT_NE(link_node_sensor->getId(), NOT_EXISTING_LINK_ID);
  EXPECT_EQ(link_node_sensor->getSourceNodeId(), worker_node_2->getId());
  EXPECT_EQ(link_node_sensor->getDestNodeId(), sensor_node_0->getId());
  // EXPECT_EQ(link_node_sensor->getLinkType(), NodeToSensor);          TODO? not supported yet
  // EXPECT_EQ(link_node_sensor->getLinkTypeString(), "NodeToSensor");  TODO? not supported yet

  auto link_sensor_node = topology_manager->createFogNodeLink(sensor_node_1->getId(), worker_node_3->getId());
  EXPECT_NE(link_sensor_node.get(), nullptr);
  EXPECT_NE(link_sensor->getId(), NOT_EXISTING_LINK_ID);
  EXPECT_EQ(link_sensor_node->getSourceNodeId(), sensor_node_1->getId());
  EXPECT_EQ(link_sensor_node->getDestNodeId(), worker_node_3->getId());
  // EXPECT_EQ(link_node_sensor->getLinkType(), SensorToNode);          TODO? not supported yet
  // EXPECT_EQ(link_node_sensor->getLinkTypeString(), "SensorToNode");  TODO? not supported yet

  EXPECT_EQ(link_node_node->getId() + 1, link_node_sensor->getId());
  EXPECT_EQ(link_node_sensor->getId() + 1, link_sensor_node->getId());
}

/* Create link, where a link already exists. */
TEST_F(FogTopologyManagerTest, create_existing_link) {
  auto node_0 = topology_manager->createFogWorkerNode();
  auto node_1 = topology_manager->createFogWorkerNode();
  topology_manager->createFogNodeLink(node_0->getId(), node_1->getId());
  EXPECT_ANY_THROW(topology_manager->createFogNodeLink(node_0->getId(), node_1->getId()););
}

/* Create a link for nodes, that do not exist. */
TEST_F(FogTopologyManagerTest, create_invalid_link) {
  auto node_0 = topology_manager->createFogWorkerNode();
  EXPECT_ANY_THROW(topology_manager->createFogNodeLink(node_0->getId(), node_0->getId()););
  EXPECT_ANY_THROW(topology_manager->createFogNodeLink(INVALID_NODE_ID, node_0->getId()););
  EXPECT_ANY_THROW(topology_manager->createFogNodeLink(node_0->getId(), INVALID_NODE_ID););
  EXPECT_ANY_THROW(topology_manager->createFogNodeLink(INVALID_NODE_ID, INVALID_NODE_ID););
}

/* Remove an existing link. */
TEST_F(FogTopologyManagerTest, remove_link) {
  auto node_0 = topology_manager->createFogWorkerNode();
  auto node_1 = topology_manager->createFogWorkerNode();
  auto link = topology_manager->createFogNodeLink(node_0->getId(), node_1->getId());

  auto result_link = topology_manager->removeFogNodeLink(link);
  EXPECT_TRUE(result_link);
}

/* Remove a non existing link. */
TEST_F(FogTopologyManagerTest, remove_non_existing_link) {
  auto node_0 = topology_manager->createFogWorkerNode();
  auto node_1 = topology_manager->createFogWorkerNode();
  auto link = topology_manager->createFogNodeLink(node_0->getId(), node_1->getId());

  auto result_link = topology_manager->removeFogNodeLink(link);
  result_link = topology_manager->removeFogNodeLink(link);
  EXPECT_FALSE(result_link);

  // What happens to a link, if one node was removed?
  // Expectation: Link is removed as well.
  link = topology_manager->createFogNodeLink(node_0->getId(), node_1->getId());
  auto result_node = topology_manager->removeFogWorkerNode(node_0);
  EXPECT_TRUE(result_node);
  result_link = topology_manager->removeFogNodeLink(link);
  EXPECT_FALSE(result_link);
}

/* - Usage Pattern --------------------------------------------------------- */
TEST_F(FogTopologyManagerTest, many_nodes) {
  // creater workers
  std::vector<std::shared_ptr<FogTopologyWorkerNode>> workers;
  for (uint32_t i = 0; i != 15; ++i) {
    workers.push_back(topology_manager->createFogWorkerNode());
  }

  // create sensors
  std::vector<std::shared_ptr<FogTopologySensor>> sensors;
  for (uint32_t i = 0; i != 30; ++i) {
    sensors.push_back(topology_manager->createFogSensorNode());
  }

  // remove some workers
  for (uint32_t i = 0; i < workers.size(); i += 4) {
    topology_manager->removeFogWorkerNode(workers.at(i));
  }

  // creater some workers
  for (uint32_t i = 0; i != 5; ++i) {
    workers.push_back(topology_manager->createFogWorkerNode());
  }

  // remove some sensors
  for (uint32_t i = 0; i < sensors.size(); i += 3) {
    topology_manager->removeFogSensorNode(sensors.at(i));
  }

  // create some sensors
  for (uint32_t i = 0; i != 10; ++i) {
    sensors.push_back(topology_manager->createFogSensorNode());
  }
}

TEST_F(FogTopologyManagerTest, many_links) {

  // creater workers
  std::vector<std::shared_ptr<FogTopologyWorkerNode>> workers;
  for (uint32_t i = 0; i != 15; ++i) {
    workers.push_back(topology_manager->createFogWorkerNode());
  }

  // create sensors
  std::vector<std::shared_ptr<FogTopologySensor>> sensors;
  for (uint32_t i = 0; i != 30; ++i) {
    sensors.push_back(topology_manager->createFogSensorNode());
  }

  // link each worker with all other workers
  std::vector<std::shared_ptr<FogTopologyLink>> links;
  for (uint32_t i = 0; i != 15; ++i) {
    for (uint32_t j = 0; j != 15; ++j) {
      if (i != j) {
        links.push_back(topology_manager->createFogNodeLink(workers.at(i)->getId(), workers.at(j)->getId()));
      }
    }
  }

  // each worker has two sensors
  for (uint32_t i = 0; i != 30; ++i) {
    if (i % 2 == 0) {
      // even sensor
      links.push_back(topology_manager->createFogNodeLink(sensors.at(i)->getId(), workers.at(i / 2)->getId()));
    } else {
      // odd sensor
      links.push_back(topology_manager->createFogNodeLink(sensors.at(i)->getId(), workers.at((i - 1) / 2)->getId()));
    }
  }

  // remove some links
  for (uint32_t i = 0; i < links.size(); i += 4) {
    EXPECT_TRUE(topology_manager->removeFogNodeLink(links.at(i)));
  }
}

/* ------------------------------------------------------------------------- */
/* - FogTopologyGraph ------------------------------------------------------ */
class FogTopologyGraphTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyGraph test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup FogTopologyGraph test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyGraph test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyGraph test class." << std::endl; }
};

/* - Vertices -------------------------------------------------------------- */
TEST_F(FogTopologyGraphTest, add_vertex) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, add_existing_vertex) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, remove_vertex) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, remove_non_existing_vertex) { EXPECT_EQ(true, true); }

/* - Edges ----------------------------------------------------------------- */
TEST_F(FogTopologyGraphTest, add_edge) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, add_existing_edge) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, add_invalid_edge) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, remove_edge) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, remove_non_existing_edge) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, remove_invalid_edge) { EXPECT_EQ(true, true); }

/* - Print ----------------------------------------------------------------- */
TEST_F(FogTopologyGraphTest, print_graph) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, print_graph_without_edges) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, print_graph_without_nodes) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, print_graph_without_anything) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, print_graph_many_vertices) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, print_graph_many_edges) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyGraphTest, print_big_graph) { EXPECT_EQ(true, true); }
