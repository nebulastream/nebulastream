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

TEST_F(FogTopologyLink, FirstTest) { ASSERT_EQ(); }

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
/* - FogTopologyNode ------------------------------------------------------- */

class FogTopologyNode : public testing::Test {
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

TEST_F(FogTopologyNode, FirstTest) { ASSERT_EQ(true, true); }

/* ------------------------------------------------------------------------- */
/* - FogTopologySensor ----------------------------------------------------- */

class FogTopologySensor : public testing::Test {
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

TEST_F(FogTopologySensor, FirstTest) { ASSERT_EQ(true, true); }

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
