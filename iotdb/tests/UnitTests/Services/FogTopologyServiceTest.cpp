
#include <gtest/gtest.h>
#include <Services/FogTopologyService.hpp>
#include <cpprest/json.h>

using namespace iotdb;
using namespace web;

//FIXME: Classes are disabled because of bad memory allocation in InputQuery creation
class FogTopologyServiceTest : public testing::Test {
 public:
  /* Will be called before any test in this class are executed. */
  static void SetUpTestCase() { std::cout << "Setup FogTopologyServiceTest test class." << std::endl; }

  /* Will be called before a test is executed. */
  void SetUp() {
    std::cout << "Setup FogTopologyServiceTest test case." << std::endl;
  }

  /* Will be called before a test is executed. */
  void TearDown() { std::cout << "Setup FogTopologyServiceTest test case." << std::endl; }

  /* Will be called after all tests in this class are finished. */
  static void TearDownTestCase() { std::cout << "Tear down FogTopologyServiceTest test class." << std::endl; }

  FogTopologyService fogTopologyService;
};

/* Test Fog topology service create plan for valid query string for  */
TEST_F(FogTopologyServiceTest, DISABLED_create_fog_execution_plan_for_valid_query_using_bottomup) {

  std::stringstream code;
  code << "Schema schema = Schema::create().addField(\"test\",INT32);" << std::endl;
  code << "Stream testStream = Stream(\"temperature1\",schema);" << std::endl;
  code << "return InputQuery::from(testStream).filter(testStream[\"test\"]==5)" << std::endl
      << ".writeToZmq(schema, \"localhost\", 10);" << std::endl;
  const json::value &plan = fogTopologyService.getExecutionPlanAsJson(code.str(), "BottomUp");
  EXPECT_TRUE(plan.size() != 0);
}

/* Test Fog topology service create plan for valid query string for  */
TEST_F(FogTopologyServiceTest, DISABLED_create_fog_execution_plan_for_valid_query_using_topdown) {

  std::stringstream code;
  code << "Schema schema = Schema::create().addField(\"test\",INT32);" << std::endl;
  code << "Stream testStream = Stream(\"temperature1\",schema);" << std::endl;
  code << "return InputQuery::from(testStream).filter(testStream[\"test\"]==5)" << std::endl
       << ".writeToZmq(schema, \"localhost\", 10);" << std::endl;
  const json::value &plan = fogTopologyService.getExecutionPlanAsJson(code.str(), "TopDown");
  EXPECT_TRUE(plan.size() != 0);
}

/* Test Fog topology service create plan for invalid query string for  */
TEST_F(FogTopologyServiceTest, create_fog_execution_plan_for_invalid_query) {

  try {
    std::stringstream code;
    code << "" << std::endl;
    fogTopologyService.getExecutionPlanAsJson(code.str(), "BottomUp");
    FAIL();
  } catch (...) {
    //TODO: We need to look into exception handling soon enough
    SUCCEED();
  }
}

/* Test Fog topology service create plan for invalid optimization strategy */
TEST_F(FogTopologyServiceTest, DISABLED_create_fog_execution_plan_for_invalid_optimization_strategy) {

  try {
    std::stringstream code;
    code << "Schema schema = Schema::create().addField(\"test\",INT32);" << std::endl;
    code << "Stream testStream = Stream(\"test-stream\",schema);" << std::endl;
    code << "return InputQuery::from(testStream).filter(testStream[\"test\"]==5)" << std::endl
        << ".writeToZmq(schema, \"localhost\", 10);" << std::endl;

    fogTopologyService.getExecutionPlanAsJson(code.str(), "random");
    FAIL();
  } catch (...) {
    //TODO: We need to look into exception handling soon enough
    SUCCEED();
  }
}