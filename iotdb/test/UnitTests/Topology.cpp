#include <iostream>

#include <Topology/FogToplogyLink.hpp>
#include <Topology/FogToplogyManager.hpp>
#include <Topology/FogToplogyNode.hpp>
#include <Topology/FogToplogyPlan.hpp>

/* ------------------------------------------------------------------------- */
/* - FogTopologyLink ------------------------------------------------------- */

class FogTopologyLink : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyLink tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup FogTopologyLink test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyLink test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyLink test class." << std::endl; }
};

TEST_F(FogTopologyLink, create_link) {
  auto link_N2N = std::make_unique<FogToplogyLink>(0, 1, NodeToNode);
  ASSERT_NE(link_N2N, nulllptr);

  auto link_N2S = std::make_unique<FogToplogyLink>(1, 2, NodeToSensor);
  ASSERT_NE(link_N2S, nulllptr);

  auto link_S2N = std::make_unique<FogToplogyLink>(2, 3, SensorToNode);
  ASSERT_NE(link_S2N, nulllptr);

  // ToDo: There could be a check at construction time, if the nodes exist and the type is matching
  // Maybe do not pass an ID but a smart pointer to the nodes.
}

TEST_F(FogTopologyLink, manipulate_link_id) {
  auto link_N2N = std::make_unique<FogToplogyLink>(0, 1, NodeToNode);
  auto link_N2S = std::make_unique<FogToplogyLink>(1, 2, NodeToSensor);
  auto link_S2N = std::make_unique<FogToplogyLink>(2, 3, SensorToNode);

  // Link ID is set to incremental values
  ASSERT_EQ(link_N2N->getLinkID() + 1, link_N2S->getLinkID());
  ASSERT_EQ(link_N2S->getLinkID() + 1, link_S2N->getLinkID());

  // Set link ID and check result
  link_N2N->setLinkID(1);
  ASSERT_EQ(link_N2N->getLinkID(), 1);

  link_N2S->setLinkID(2);
  ASSERT_EQ(link_N2S->getLinkID(), 2);

  link_S2N->setLinkID(3);
  ASSERT_EQ(link_S2N->getLinkID(), 3);

  // There should be no two links with same id
  // ToDo: Check not implemented yet
}

TEST_F(FogTopologyLink, get_node_ids) {
  auto link_N2N = std::make_unique<FogToplogyLink>(0, 1, NodeToNode);
  auto link_N2S = std::make_unique<FogToplogyLink>(1, 2, NodeToSensor);
  auto link_S2N = std::make_unique<FogToplogyLink>(2, 3, SensorToNode);

  ASSERT_EQ(link_N2N->getSourceNodeID(), 0);
  ASSERT_EQ(link_N2N->getDestNodeID(), 1);

  ASSERT_EQ(link_N2S->getSourceNodeID(), 1);
  ASSERT_EQ(link_N2S->getDestNodeID(), 2);

  ASSERT_EQ(link_S2N->getSourceNodeID(), 2);
  ASSERT_EQ(link_S2N->getDestNodeID(), 3);
}

/* ------------------------------------------------------------------------- */
/* - FogTopologyNode ------------------------------------------------------- */

class FogTopologyNode : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyNode tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup FogTopologyNode test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyNode test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyNode test class." << std::endl; }
};

TEST_F(FogTopologyNode, create_node) {

  auto node = std::make_unique<FogToplogyNode>();
  ASSERT_NE(node, nulllptr);
}

TEST_F(FogTopologyNode, manipulate_node_id) {

  auto node = std::make_unique<FogToplogyNode>();

  // Node is constructed with invalid id.
  ASSERT_EQ(node->getNodeId(), INVALID_NODE_ID);

  // Set id for note
  node->setNodeId(1);
  ASSERT_EQ(node->getNodeId(), 1);

  // There should be no two nodes with same id
  // ToDo: Check not implemented yet
}

/* ------------------------------------------------------------------------- */
/* - FogTopologySensor ----------------------------------------------------- */

class FogTopologySensor : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologySensor tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup FogTopologySensor test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologySensor test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologySensor test class." << std::endl; }
};

TEST_F(FogTopologySensor, create_sensor) {

  auto sensor = std::make_unique<FogToplogySensor>();
  ASSERT_NE(sensor, nulllptr);
}

TEST_F(FogTopologyNode, manipulate_sensor_id) {

  auto sensor = std::make_unique<FogToplogySensor>();

  // Sensor is constructed with invalid id.
  ASSERT_EQ(node->getNodeId(), INVALID_NODE_ID);

  // Set id for sensor
  node->setSensorId(1);
  ASSERT_EQ(sensor->getSensorId(), 1);

  // There should be no two Sensors with same id
  // ToDo: Check not implemented yet
}

/* ------------------------------------------------------------------------- */
/* - FogTopologyManager ---------------------------------------------------- */

class FogTopologyManager : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down test class." << std::endl; }
};

TEST_F(FogTopologyManager, FirstTest) { ASSERT_EQ(true, true); }

/* ------------------------------------------------------------------------- */
/* - FogTopologyPlan ------------------------------------------------------- */

class FogTopologyPlan : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup Tests Class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down test class." << std::endl; }
};

TEST_F(FogTopologyPlan, FirstTest) { ASSERT_EQ(true, true); }
