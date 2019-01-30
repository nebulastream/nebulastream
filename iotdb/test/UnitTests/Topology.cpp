#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "Topology/FogTopologyLink.hpp"
#include "Topology/FogTopologyManager.hpp"
#include "Topology/FogTopologyNode.hpp"
#include "Topology/FogTopologyPlan.hpp"

/* ------------------------------------------------------------------------- */
/* - FogTopologyLink ------------------------------------------------------- */

class FogTopologyLinkTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() override { std::cout << "Setup FogTopologyLink tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() override { std::cout << "Setup FogTopologyLink test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() override { std::cout << "Setup FogTopologyLink test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() override { std::cout << "Tear down FogTopologyLink test class." << std::endl; }
};

TEST_F(FogTopologyLinkTest, create_link) {
  EXPECT_NO_THROW(auto link_N2N = std::make_unique<FogTopologyLink>(0, 1, NodeToNode););
  EXPECT_NE(link_N2N, nullptr);

  EXPECT_NO_THROW(auto link_N2S = std::make_unique<FogTopologyLink>(1, 2, NodeToSensor););
  EXPECT_NE(link_N2S, nullptr);

  EXPECT_NO_THROW(auto link_S2N = std::make_unique<FogTopologyLink>(2, 3, SensorToNode););
  EXPECT_NE(link_S2N, nullptr);

  // ToDo: There could be a check at construction time, if the nodes exist and the type is matching
  // Maybe do not pass an ID but a smart pointer to the nodes.
}

TEST_F(FogTopologyLinkTest, manipulate_link_id) {
  auto link_N2N = std::make_unique<FogTopologyLink>(0, 1, NodeToNode);
  auto link_N2S = std::make_unique<FogTopologyLink>(1, 2, NodeToSensor);
  auto link_S2N = std::make_unique<FogTopologyLink>(2, 3, SensorToNode);

  // Link ID is set to incremental values
  EXPECT_EQ(link_N2N->getLinkID() + 1, link_N2S->getLinkID());
  EXPECT_EQ(link_N2S->getLinkID() + 1, link_S2N->getLinkID());

  // Set link ID and check result
  link_N2N->setLinkID(100);
  EXPECT_EQ(link_N2N->getLinkID(), 100);

  link_N2S->setLinkID(200);
  EXPECT_EQ(link_N2S->getLinkID(), 200);

  link_S2N->setLinkID(300);
  EXPECT_EQ(link_S2N->getLinkID(), 300);

  // There should be no two links with same id
  EXPECT_ANY_THROW(link_S2N->setLinkID(200););
}

TEST_F(FogTopologyLinkTest, get_node_ids) {
  auto link_N2N = std::make_unique<FogTopologyLink>(0, 1, NodeToNode);
  auto link_N2S = std::make_unique<FogTopologyLink>(1, 2, NodeToSensor);
  auto link_S2N = std::make_unique<FogTopologyLink>(2, 3, SensorToNode);

  EXPECT_EQ(link_N2N->getSourceNodeId(), 0);
  EXPECT_EQ(link_N2N->getDestNodeId(), 1);

  EXPECT_EQ(link_N2S->getSourceNodeId(), 1);
  EXPECT_EQ(link_N2S->getDestNodeId(), 2);

  EXPECT_EQ(link_S2N->getSourceNodeId(), 2);
  EXPECT_EQ(link_S2N->getDestNodeId(), 3);
}

/* ------------------------------------------------------------------------- */
/* - FogTopologyNode ------------------------------------------------------- */

class FogTopologyNodeTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() override { std::cout << "Setup FogTopologyNode tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() override { std::cout << "Setup FogTopologyNode test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() override { std::cout << "Setup FogTopologyNode test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() override { std::cout << "Tear down FogTopologyNode test class." << std::endl; }
};

TEST_F(FogTopologyNodeTest, create_node) {
  EXPECT_NO_THROW(auto node = std::make_unique<FogTopologyNode>(););
  EXPECT_NE(node, nullptr);
}

TEST_F(FogTopologyNodeTest, manipulate_node_id) {

  auto node = std::make_unique<FogTopologyNode>();

  // Node is constructed with invalid id.
  EXPECT_EQ(node->getNodeId(), INVALID_NODE_ID);

  // Set id for note
  node->setNodeId(200);
  EXPECT_EQ(node->getNodeId(), 200);

  // There should be no two nodes with same id
  auto node_2 = std::make_unique<FogTopologyNode>();
  EXPECT_ANY_THROW(node_2->setNodeId(200););
}

/* ------------------------------------------------------------------------- */
/* - FogTopologySensor ----------------------------------------------------- */

class FogTopologySensorTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() override { std::cout << "Setup FogTopologySensor tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() override { std::cout << "Setup FogTopologySensor test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() override { std::cout << "Setup FogTopologySensor test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() override { std::cout << "Tear down FogTopologySensor test class." << std::endl; }
};

TEST_F(FogTopologySensorTest, create_sensor) {
  EXPECT_NO_THROW(auto sensor = std::make_unique<FogTopologySensor>(););
  EXPECT_NE(sensor, nullptr);
}

TEST_F(FogTopologySensorTest, manipulate_sensor_id) {

  auto sensor = std::make_unique<FogTopologySensor>();

  // Sensor is constructed with invalid id.
  EXPECT_EQ(sensor->getSensorId(), INVALID_NODE_ID);

  // Set id for sensor
  sensor->setSensorId(200);
  EXPECT_EQ(sensor->getSensorId(), 200);

  // There should be no two sensors with same id
  auto sensor_2 = std::make_unique<FogTopologySensor>();
  EXPECT_ANY_THROW(sensor_2->setSensorId(200););
}

/* ------------------------------------------------------------------------- */
/* - FogTopologyPlan ------------------------------------------------------- */

class FogTopologyPlanTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() override { std::cout << "Setup FogTopologyPlan tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() override {

    std::cout << "Setup FogTopologyPlan test case." << std::endl;

    sensor_1 = std::make_shared<FogTopologySensor>();
    sensor_2 = std::make_shared<FogTopologySensor>();
    sensor_3 = std::make_shared<FogTopologySensor>();
    sensor_4 = std::make_shared<FogTopologySensor>();

    sensor_1->setSensorId(1);
    sensor_2->setSensorId(2);
    sensor_3->setSensorId(3);
    sensor_4->setSensorId(4);

    node_1 = std::make_shared<FogTopologyNode>();
    node_2 = std::make_shared<FogTopologyNode>();
    node_3 = std::make_shared<FogTopologyNode>();
    node_4 = std::make_shared<FogTopologyNode>();

    node_1->setNodeId(1);
    node_2->setNodeId(2);
    node_3->setNodeId(3);
    node_4->setNodeId(4);
  }

  /* Will be called before a test is executed. */
  void TearDown() override { std::cout << "Setup FogTopologyPlan test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() override { std::cout << "Tear down FogTopologyPlan test class." << std::endl; }

  FogTopologyNodePtr node_1, node_2, node_3, node_4;
  FogTopologySensorPtr sensor_1, sensor_2, sensor_3, sensor_4;
};

/* - Graph part -------------------- */
TEST_F(FogTopologyPlanTest, graph_create) {

  EXPECT_NO_THROW(auto graph = std::make_unique<Graph>(3););
  EXPECT_NE(graph, nullptr);

  EXPECT_NO_THROW(auto graph_min = std::make_unique<Graph>(0););
  EXPECT_NE(graph_min, nullptr);

  EXPECT_NO_THROW(auto graph_max = std::make_unique<Graph>(MAX_NUMBER_OF_NODES););
  EXPECT_NE(graph_max, nullptr);

  EXPECT_ANY_THROW(auto graph_max = std::make_unique<Graph>(MAX_NUMBER_OF_NODES + 1););

  EXPECT_ANY_THROW(auto graph_max = std::make_unique<Graph>(-1););
}

TEST_F(FogTopologyPlanTest, graph_add_node) {
  auto graph = std::make_unique<Graph>(MAX_NUMBER_OF_NODES - 1);
  EXPECT_NO_THROW(graph->addNode(););
  EXPECT_ANY_THROW(graph->addNode(););
}

TEST_F(FogTopologyPlanTest, graph_add_link) {
  // create graph
  auto graph = std::make_unique<Graph>(4);
  EXPECT_NO_THROW(graph->addLink(1, 2, 1););

  // adding a link, that already exists
  EXPECT_ANY_THROW(graph->addLink(1, 2, 1);); // nodes & id are identical
  EXPECT_ANY_THROW(graph->addLink(1, 2, 2);); // nodes are identical
  EXPECT_ANY_THROW(graph->addLink(1, 3, 1);); // id is identical

  // adding a link for nodes, that do not exist
  EXPECT_ANY_THROW(graph->addLink(1, 6, 3);); // right node
  EXPECT_ANY_THROW(graph->addLink(6, 2, 4);); // left node
  EXPECT_ANY_THROW(graph->addLink(6, 7, 5);); // both

  // self-assignment
  EXPECT_ANY_THROW(graph->addLink(2, 2, 6););
}

TEST_F(FogTopologyPlanTest, graph_get_link) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, graph_remove_link) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, graph_remove_row_and_col) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, graph_print) {

  // empty and naked graph
  auto graph = std::make_unique<Graph>(4);
  std::cout << graph->to_string();

  // add a node
  graph->addNode();
  std::cout << graph->to_string();

  // add a link
  graph->addLink(1, 2, 1);
  std::cout << graph->to_string();

  // adding a node
  graph->addNode();
  std::cout << graph->to_string();

  // remove a link
  graph->addNode();
  std::cout << graph->to_string();
}

/* - Plan part -------------------- */
TEST_F(FogTopologyPlanTest, plan_create) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_print) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_add_node) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_remove_node) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_list_nodes) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_add_sensor) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_remove_sensor) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, plan_list_sensor) { EXPECT_EQ(true, true); }

/* ------------------------------------------------------------------------- */
/* - FogTopologyManager ---------------------------------------------------- */

class FogTopologyManagerTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() override { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() override { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() override { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() override { std::cout << "Tear down FogTopologyManager test class." << std::endl; }
};

TEST_F(FogTopologyManagerTest, create_manager) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, add_node) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, add_sensor) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, add_link) { EXPECT_EQ(true, true); }
