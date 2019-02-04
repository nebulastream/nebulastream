#include <cstddef>
#include <iostream>
#include <memory>

#include "gtest/gtest.h"

#ifndef TESTING
#define TESTING
#endif // TESTING
#include "../../iotClient.cpp"
#include "../../iotServer.cpp"

/* ------------------------------------------------------------------------- */
/* - IoT-DB Server --------------------------------------------------------- */
class IotServerTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup IotServerTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup IotServerTest test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup IotServerTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down IotServerTest test class." << std::endl; }
};

/* ------------------------------------------------------------------------- */
/* - IoT-DB Client --------------------------------------------------------- */
class IotClientTest : public testing::Test {
public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup IotClientTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() { std::cout << "Setup IotClientTest test case." << std::endl; }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup IotClientTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down IotClientTest test class." << std::endl; }
};