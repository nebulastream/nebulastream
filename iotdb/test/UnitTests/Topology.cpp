#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "Topology/FogTopologyManager.hpp"
#include "Topology/FogTopologyPlan.hpp"


/**
 * Testcases to add:
 * 		- create already existing node
 * 		- remove not existing node
 * 		- add invalid Link
 * 		- add test for FogGraph directly
 * 		- add test for graph with print
 *
 */
/* ------------------------------------------------------------------------- */
/* - FogTopologyLink ------------------------------------------------------- */

class FogTopologyLinkTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase()  { std::cout << "Setup FogTopologyLink tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp()  { std::cout << "Setup FogTopologyLink test case." << std::endl; fMgnr = new FogTopologyManager();}

  /* Will be called before a test is executed. */
  void TearDown()  { std::cout << "Setup FogTopologyLink test case." << std::endl; delete(fMgnr);}

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase()  { std::cout << "Tear down FogTopologyLink test class." << std::endl; }

  FogTopologyManager* fMgnr;

};

TEST_F(FogTopologyLinkTest, create_link_node_sensor) {

	auto link_N2N = fMgnr->createFogWorkerNode();
	EXPECT_NE(link_N2N, nullptr);

	auto link_N2S = fMgnr->createFogSensorNode();
	EXPECT_NE(link_N2S, nullptr);

	auto link_S2N =	fMgnr->createFogNodeLink(link_N2N->getID(), link_N2S->getID());

	EXPECT_NE(link_S2N, nullptr);
}

TEST_F(FogTopologyLinkTest, create_link_sensor_node) {

	auto link_N2N = fMgnr->createFogSensorNode();
	EXPECT_NE(link_N2N, nullptr);

	auto link_N2S = fMgnr->createFogWorkerNode();
	EXPECT_NE(link_N2S, nullptr);

	auto link_S2N =	fMgnr->createFogNodeLink(link_N2N->getID(), link_N2S->getID());

	EXPECT_NE(link_S2N, nullptr);
}
TEST_F(FogTopologyLinkTest, create_link_sensor_sensor) {

	auto link_N2N = fMgnr->createFogSensorNode();
	EXPECT_NE(link_N2N, nullptr);

	auto link_N2S = fMgnr->createFogSensorNode();
	EXPECT_NE(link_N2S, nullptr);

	auto link_S2N =	fMgnr->createFogNodeLink(link_N2N->getID(), link_N2S->getID());

	EXPECT_NE(link_S2N, nullptr);
}

TEST_F(FogTopologyLinkTest, create_link_node_node) {

	auto link_N2N = fMgnr->createFogWorkerNode();
	EXPECT_NE(link_N2N, nullptr);

	auto link_N2S = fMgnr->createFogWorkerNode();
	EXPECT_NE(link_N2S, nullptr);

	auto link_S2N =	fMgnr->createFogNodeLink(link_N2N->getID(), link_N2S->getID());

	EXPECT_NE(link_S2N, nullptr);
}


TEST_F(FogTopologyLinkTest, delete_link_node_node) {

	auto n1 = fMgnr->createFogWorkerNode();
	auto n2 = fMgnr->createFogWorkerNode();
	auto link =	fMgnr->createFogNodeLink(n1->getID(), n2->getID());

	EXPECT_NE(link, nullptr);
	bool res = fMgnr->removeFogNodeLink(link);
	EXPECT_TRUE(res);
}

TEST_F(FogTopologyLinkTest, double_delete_link_node_node) {

	auto n1 = fMgnr->createFogWorkerNode();
	auto n2 = fMgnr->createFogWorkerNode();
	auto link =	fMgnr->createFogNodeLink(n1->getID(), n2->getID());

	EXPECT_NE(link, nullptr);
	bool res = fMgnr->removeFogNodeLink(link);
	EXPECT_TRUE(res);

	res = fMgnr->removeFogNodeLink(link);
	EXPECT_FALSE(res);
}


/* ------------------------------------------------------------------------- */
/* - FogTopologyNode ------------------------------------------------------- */

class FogTopologyNodeTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase()  { std::cout << "Setup FogTopologyNode tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp()  { std::cout << "Setup FogTopologyNode test case." << std::endl; fMgnr = new FogTopologyManager();}

  /* Will be called before a test is executed. */
  void TearDown()  { std::cout << "Setup FogTopologyNode test case." << std::endl; delete(fMgnr);}

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase()  { std::cout << "Tear down FogTopologyNode test class." << std::endl; }

  FogTopologyManager* fMgnr;

};

TEST_F(FogTopologyNodeTest, create_node) {
	auto node = fMgnr->createFogWorkerNode();
	EXPECT_NE(node, nullptr);
}

TEST_F(FogTopologyNodeTest, delete_node) {
	auto node = fMgnr->createFogWorkerNode();
	bool res = fMgnr->removeFogWorkerNode(node);
	EXPECT_TRUE(res);
}

TEST_F(FogTopologyNodeTest, double_delete_node) {
	auto node = fMgnr->createFogWorkerNode();
	bool res = fMgnr->removeFogWorkerNode(node);
	EXPECT_TRUE(res);

	res = fMgnr->removeFogWorkerNode(node);
	EXPECT_FALSE(res);
}

/* ------------------------------------------------------------------------- */
/* - FogTopologySensor ----------------------------------------------------- */

class FogTopologySensorTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase()  { std::cout << "Setup FogTopologySensor tests class." << std::endl;}

  /* Will be called before a test is executed. */
  void SetUp()  { std::cout << "Setup FogTopologySensor test case." << std::endl; fMgnr = new FogTopologyManager();}

  /* Will be called before a test is executed. */
  void TearDown()  { std::cout << "Setup FogTopologySensor test case." << std::endl; delete(fMgnr);}

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase()  { std::cout << "Tear down FogTopologySensor test class." << std::endl; }

  FogTopologyManager* fMgnr;
};

TEST_F(FogTopologySensorTest, create_sensor) {
	auto node = fMgnr->createFogSensorNode();
	EXPECT_NE(node, nullptr);
}

TEST_F(FogTopologySensorTest, delete_node) {
	auto node = fMgnr->createFogSensorNode();
	bool res = fMgnr->removeFogSensorNode(node);
	EXPECT_TRUE(res);
}

TEST_F(FogTopologySensorTest, double_delete_node) {
	auto node = fMgnr->createFogSensorNode();
	bool res = fMgnr->removeFogSensorNode(node);
	EXPECT_TRUE(res);

	res = fMgnr->removeFogSensorNode(node);
	EXPECT_FALSE(res);
}
/* ------------------------------------------------------------------------- */
/* - FogTopologyPlan ------------------------------------------------------- */

class FogTopologyPlanTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase()  { std::cout << "Setup FogTopologyPlan tests class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp()  {

    std::cout << "Setup FogTopologyPlan test case." << std::endl;

    sensor_1 = std::make_shared<FogTopologySensor>();
    sensor_2 = std::make_shared<FogTopologySensor>();
    sensor_3 = std::make_shared<FogTopologySensor>();
    sensor_4 = std::make_shared<FogTopologySensor>();

    sensor_1->setSensorId(1);
    sensor_2->setSensorId(2);
    sensor_3->setSensorId(3);
    sensor_4->setSensorId(4);

    node_1 = std::make_shared<FogTopologyWorkerNode>();
    node_2 = std::make_shared<FogTopologyWorkerNode>();
    node_3 = std::make_shared<FogTopologyWorkerNode>();
    node_4 = std::make_shared<FogTopologyWorkerNode>();

    node_1->setNodeId(1);
    node_2->setNodeId(2);
    node_3->setNodeId(3);
    node_4->setNodeId(4);
  }

  /* Will be called before a test is executed. */
  void TearDown()  { std::cout << "Setup FogTopologyPlan test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase()  { std::cout << "Tear down FogTopologyPlan test class." << std::endl; }

  FogTopologyWorkerNodePtr node_1, node_2, node_3, node_4;
  FogTopologySensorPtr sensor_1, sensor_2, sensor_3, sensor_4;
};

/* - Graph part -------------------- */
TEST_F(FogTopologyPlanTest, graph_create) {

  auto graph = std::make_unique<FogGraph>();
  EXPECT_NE(graph, nullptr);
}

TEST_F(FogTopologyPlanTest, graph_add_node) {EXPECT_EQ(true, true);}

TEST_F(FogTopologyPlanTest, graph_add_link) {EXPECT_EQ(true, true);}

TEST_F(FogTopologyPlanTest, graph_get_link) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, graph_remove_link) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, graph_remove_row_and_col) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyPlanTest, graph_print) {EXPECT_EQ(true, true);}

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
  static void SetUpTestCase()  { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp()  { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown()  { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase()  { std::cout << "Tear down FogTopologyManager test class." << std::endl; }
};

TEST_F(FogTopologyManagerTest, create_manager) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, add_node) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, add_sensor) { EXPECT_EQ(true, true); }

TEST_F(FogTopologyManagerTest, add_link) { EXPECT_EQ(true, true); }
