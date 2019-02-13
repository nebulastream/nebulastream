#include <gtest/gtest.h>
#include <zmq.hpp>

#ifndef TESTING
#define TESTING
#endif // TESTING

#include <Runtime/ZmqSource.hpp>

class RuntimeDataSourceTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup RuntimeDataSourceTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup RuntimeDataSourceTest test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup RuntimeDataSourceTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down RuntimeDataSourceTest test class." << std::endl; }
};

TEST_F(RuntimeDataSourceTest, foo) { EXPECT_TRUE(true); }
