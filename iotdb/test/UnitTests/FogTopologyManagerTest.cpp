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
