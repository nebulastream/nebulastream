#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#include "Topology/FogTopologyManager.hpp"
#include "Topology/FogTopologyPlan.hpp"

/* ------------------------------------------------------------------------- */
/* - FogTopologyManager ---------------------------------------------------- */
class FogTopologyManagerTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyManager test class." << std::endl; }
};

/* - Nodes ----------------------------------------------------------------- */
/* Create a new node. */
TEST_F(FogTopologyManagerTest, create_node) { EXPECT_EQ(true, true); }

/* Create an already existing node. */
TEST_F(FogTopologyManagerTest, create_existing_node) { EXPECT_EQ(true, true); }

/* Remove an existing node. */
TEST_F(FogTopologyManagerTest, remove_node) { EXPECT_EQ(true, true); }

/* Remove a non existing node. */
TEST_F(FogTopologyManagerTest, remove_non_existing_node) { EXPECT_EQ(true, true); }

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
  static void SetUpTestCase() { std::cout << "Setup FogTopologyManager test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyManager test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyManager test class." << std::endl; }
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
