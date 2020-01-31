#include <gtest/gtest.h>
#include <Services/OptimizerService.hpp>
#include <Services/QueryService.hpp>
#include <Topology/NESTopologyManager.hpp>
#include <Topology/TestTopology.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>

using namespace NES;
using namespace web;

//FIXME: Classes are disabled because of bad memory allocation in InputQuery creation
class OptimizerServiceTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup OptimizerServiceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        queryServicePtr = QueryService::getInstance();
        optimizerServicePtr = OptimizerService::getInstance();
        setupLogging();
        createExampleTopology();
        std::cout << "Setup OptimizerServiceTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Setup OptimizerServiceTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down OptimizerServiceTest test class." << std::endl;
    }

    QueryServicePtr queryServicePtr;
    OptimizerServicePtr optimizerServicePtr;

  protected:
    static void setupLogging() {
        // create PatternLayout
        log4cxx::LayoutPtr layoutPtr(
            new log4cxx::PatternLayout(
                "%d{MMM dd yyyy HH:mm:ss} %c:%L [%-5t] [%p] : %m%n"));

        // create FileAppender
        LOG4CXX_DECODE_CHAR(fileName, "EngineTest.log");
        log4cxx::FileAppenderPtr file(
            new log4cxx::FileAppender(layoutPtr, fileName));

        // create ConsoleAppender
        log4cxx::ConsoleAppenderPtr console(
            new log4cxx::ConsoleAppender(layoutPtr));

        // set log level
        NESLogger->setLevel(log4cxx::Level::getDebug());
        //    logger->setLevel(log4cxx::Level::getInfo());

        // add appenders and other will inherit the settings
        NESLogger->addAppender(file);
        NESLogger->addAppender(console);
    }

};

/* Test nes topology service create plan for valid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_valid_query_using_bottomup) {

    std::stringstream code;
    code << "InputQuery::from(temperature)"
         << ".filter(temperature[\"id\"]==5)" << std::endl
         << ".writeToZmq(temperature, \"localhost\", 10);" << std::endl;

    const InputQueryPtr& inputQuery = queryServicePtr->getInputQueryFromQueryString(
        code.str());
    const json::value& plan = optimizerServicePtr->getExecutionPlanAsJson(inputQuery,
                                                                         "BottomUp");
    EXPECT_TRUE(plan.size() != 0);
}
/* Test nes topology service create plan for valid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_valid_query_using_topdown) {

    std::stringstream code;
    code
        << "InputQuery::from(temperature).filter(temperature[\"value\"]==5)"
        << std::endl << ".writeToZmq(temperature, \"localhost\", 10);"
        << std::endl;
    const InputQueryPtr& inputQuery = queryServicePtr->getInputQueryFromQueryString(
        code.str());
    const json::value& plan = optimizerServicePtr->getExecutionPlanAsJson(inputQuery,
                                                                         "BottomUp");
    EXPECT_TRUE(plan.size() != 0);
}

/* Test nes topology service create plan for invalid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_invalid_query) {

    try {
        std::stringstream code;
        code << "" << std::endl;
        queryServicePtr->getInputQueryFromQueryString(code.str());
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }
}

/* Test nes topology service create plan for invalid optimization strategy */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_invalid_optimization_strategy) {

    try {
        std::stringstream code;
        code
            << "InputQuery::from(temperature134).filter(temperature134[\"id\"]==5)"
            << ".writeToZmq(temperature134, \"localhost\", 10);"
            << std::endl;

        const InputQueryPtr& inputQuery = queryServicePtr->getInputQueryFromQueryString(
            code.str());
        const json::value& plan = optimizerServicePtr->getExecutionPlanAsJson(
            inputQuery, "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }

    try {
        //test wrong stream in filter
        std::stringstream code;
        code
            << "InputQuery::from(temperature1).filter(testStream[\"wrong_field\"]==5)"
            << std::endl << ".writeToZmq(temperature1, \"localhost\", 10);"
            << std::endl;

        const InputQueryPtr& inputQuery = queryServicePtr->getInputQueryFromQueryString(
            code.str());
        const json::value& plan = optimizerServicePtr->getExecutionPlanAsJson(
            inputQuery, "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }

    try {
        //test wrong field in filter
        std::stringstream code;
        code
            << "InputQuery::from(temperature1).filter(temperature1[\"wrong_field\"]==5)"
            << std::endl << ".writeToZmq(temperature1, \"localhost\", 10);"
            << std::endl;

        const InputQueryPtr& inputQuery = queryServicePtr->getInputQueryFromQueryString(
            code.str());
        const json::value& plan = optimizerServicePtr->getExecutionPlanAsJson(
            inputQuery, "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }

    try {
        //test wrong stream in writeToZmq
        std::stringstream code;
        code
            << "InputQuery::from(temperature1).filter(temperature1[\"id\"]==5)"
            << std::endl << ".writeToZmq(temperature331, \"localhost\", 10);"
            << std::endl;

        const InputQueryPtr& inputQuery = queryServicePtr->getInputQueryFromQueryString(
            code.str());
        const json::value& plan = optimizerServicePtr->getExecutionPlanAsJson(
            inputQuery, "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }
}
