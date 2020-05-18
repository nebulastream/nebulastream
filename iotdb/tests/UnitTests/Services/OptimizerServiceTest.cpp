#include <gtest/gtest.h>
#include <Services/OptimizerService.hpp>
#include <Services/QueryService.hpp>
#include <Topology/TopologyManager.hpp>
#include <Topology/TestTopology.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Util/Logger.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Services/OptimizerService.hpp>

using namespace NES;
using namespace web;

class OptimizerServiceTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup OptimizerServiceTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("OptimizerServiceTest.log", NES::LOG_DEBUG);
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
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
};

/* Test nes topology service create plan for valid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_valid_query_using_bottomup) {
    TopologyManagerPtr topologyManager = std::shared_ptr<TopologyManager>();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>();

    std::stringstream code;
    code << "InputQuery::from(temperature)" << ".filter(temperature[\"id\"]==5)"
         << std::endl << ".writeToZmq(temperature, \"localhost\", 10);"
         << std::endl;

    const InputQueryPtr& inputQuery = queryService->getInputQueryFromQueryString(code.str());
    const json::value& plan = optimizerService->getExecutionPlanAsJson(
        inputQuery, "BottomUp");
    EXPECT_TRUE(plan.size() != 0);
}
/* Test nes topology service create plan for valid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_valid_query_using_topdown) {
    TopologyManagerPtr topologyManager = std::shared_ptr<TopologyManager>();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>();

    std::stringstream code;
    code << "InputQuery::from(temperature).filter(temperature[\"value\"]==5)"
         << std::endl << ".writeToZmq(temperature, \"localhost\", 10);"
         << std::endl;
    const InputQueryPtr& inputQuery = queryService->getInputQueryFromQueryString(code.str());
    const json::value& plan = optimizerService->getExecutionPlanAsJson(
        inputQuery, "BottomUp");
    EXPECT_TRUE(plan.size() != 0);
}

/* Test nes topology service create plan for invalid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_invalid_query) {
    TopologyManagerPtr topologyManager = std::shared_ptr<TopologyManager>();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>();

    try {
        std::stringstream code;
        code << "" << std::endl;
        queryService->getInputQueryFromQueryString(code.str());
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }
}

/* Test nes topology service create plan for invalid optimization strategy */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_invalid_optimization_strategy) {
    TopologyManagerPtr topologyManager = std::shared_ptr<TopologyManager>();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>();

    try {
        std::stringstream code;
        code << "InputQuery::from(temperature134).filter(temperature134[\"id\"]==5)"
             << ".writeToZmq(temperature134, \"localhost\", 10);" << std::endl;

        const InputQueryPtr& inputQuery = queryService->getInputQueryFromQueryString(code.str());
        const json::value& plan = optimizerService->getExecutionPlanAsJson(
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

        const InputQueryPtr& inputQuery = queryService->getInputQueryFromQueryString(code.str());
        const json::value& plan = optimizerService->getExecutionPlanAsJson(
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

        const InputQueryPtr& inputQuery = queryService->getInputQueryFromQueryString(code.str());
        const json::value& plan = optimizerService->getExecutionPlanAsJson(
            inputQuery, "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }

    try {
        //test wrong stream in writeToZmq
        std::stringstream code;
        code << "InputQuery::from(temperature1).filter(temperature1[\"id\"]==5)"
             << std::endl << ".writeToZmq(temperature331, \"localhost\", 10);"
             << std::endl;

        const InputQueryPtr& inputQuery = queryService->getInputQueryFromQueryString(code.str());
        const json::value& plan = optimizerService->getExecutionPlanAsJson(
            inputQuery, "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }
}
