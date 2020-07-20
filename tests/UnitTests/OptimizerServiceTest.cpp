#include <Catalogs/StreamCatalog.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/OptimizerService.hpp>
#include <Services/QueryService.hpp>
#include <Topology/TestTopology.hpp>
#include <Topology/TopologyManager.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

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
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>(streamCatalog);

    std::stringstream code;
    code << "Query::from(\"temperature\")"
         << ".filter(Attribute(\"id\")==5)" << std::endl
         << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));";

    const QueryPtr query = queryService->getQueryFromQueryString(code.str());
    const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "BottomUp");
    EXPECT_TRUE(plan.size() != 0);
}

/* Test nes topology service create plan for valid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_valid_query_using_topdown) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>(streamCatalog);

    std::stringstream code;
    code << "Query::from(\"temperature\").filter(Attribute(\"value\")==5)" << std::endl
         << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));";
    const QueryPtr query = queryService->getQueryFromQueryString(code.str());

    const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "TopDown");
    EXPECT_TRUE(plan.size() != 0);
}

/* Test nes topology service create plan for invalid query string for  */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_invalid_query) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>(streamCatalog);

    try {
        std::stringstream code;
        code << "" << std::endl;
        queryService->getQueryFromQueryString(code.str());
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }
}

/* Test nes topology service create plan for invalid optimization strategy */
TEST_F(OptimizerServiceTest, create_nes_execution_plan_for_invalid_optimization_strategy) {
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    OptimizerServicePtr optimizerService = std::make_shared<OptimizerService>(topologyManager, streamCatalog, globalExecutionPlan);
    createExampleTopology(streamCatalog, topologyManager);
    QueryServicePtr queryService = std::make_shared<QueryService>(streamCatalog);

    try {
        std::stringstream code;
        code << "Query::from(\"temperature134\").filter(Attribute(\"id\")==5)"
             << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));" << std::endl;

        const QueryPtr query = queryService->getQueryFromQueryString(code.str());
        const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }

    try {
        //test wrong field in filter
        std::stringstream code;
        code
            << "Query::from(temperature1).filter(Attribute(\"wrong_field\")==5)"
            << std::endl
            << ".sink(ZmqSinkDescriptor::create(\"localhost\", 10));"
            << std::endl;

        const QueryPtr query = queryService->getQueryFromQueryString(code.str());
        const std::string plan = optimizerService->getExecutionPlanAsString(query->getQueryPlan(), "BottomUp");
        FAIL();
    } catch (...) {
        //TODO: We need to look into exception handling soon enough
        SUCCEED();
    }
}
