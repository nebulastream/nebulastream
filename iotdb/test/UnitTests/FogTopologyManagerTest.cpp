#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "Topology/FogTopologyManager.hpp"
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
  EXPECT_EQ(worker_node->getEntryType(), Sensor);
  EXPECT_EQ(worker_node->getEntryTypeString(), "Sensor");
  EXPECT_NE(sensor_node->getId(), INVALID_NODE_ID);

  EXPECT_EQ(worker_node->getId() + 1, sensor_node->getId());
}

/* Remove an existing node. */
TEST_F(FogTopologyManagerTest, remove_node) {
  auto worker_node = topology_manager->createFogWorkerNode();
  auto result_worker = topology_manager->removeFogWorkerNode(sensor_node);
  EXPECT_EQ(result_worker, true);

  auto sensor_node = topology_manager->createFogSensorNode();
  auto result_sensor topology_manager->removeFogSensorNode(worker_node);
  EXPECT_EQ(result_sensor, true);
}

/* Remove a non existing node. */
TEST_F(FogTopologyManagerTest, remove_non_existing_node) {
  auto worker_node = topology_manager->createFogWorkerNode();
  auto result_worker = topology_manager->removeFogWorkerNode(sensor_node);
  result_worker = topology_manager->removeFogWorkerNode(sensor_node);
  EXPECT_EQ(result_worker, false);

  auto sensor_node = topology_manager->createFogSensorNode();
  auto result_sensor topology_manager->removeFogSensorNode(worker_node);
  result_sensor topology_manager->removeFogSensorNode(worker_node);
  EXPECT_EQ(result_sensor, false);
}

/* - Links ----------------------------------------------------------------- */
/* Create a new link. */
TEST_F(FogTopologyManagerTest, create_link) { EXPECT_EQ(true, true); }

/* Create link, where a link already exists. */
TEST_F(FogTopologyManagerTest, create_existing_link) { EXPECT_EQ(true, true); }

/* Create a link for nodes, that do not exist. */
TEST_F(FogTopologyManagerTest, create_invalid_link) { EXPECT_EQ(true, true); }

/* Remove an existing link. */
TEST_F(FogTopologyManagerTest, remove_link) { EXPECT_EQ(true, true); }

/* Remove a non existing link. */
TEST_F(FogTopologyManagerTest, remove_non_existing_link) { EXPECT_EQ(true, true); }

/* - Usage Pattern --------------------------------------------------------- */
TEST_F(FogTopologyManagerTest, many_nodes) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, many_edges) { EXPECT_EQ(true, true); }

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
