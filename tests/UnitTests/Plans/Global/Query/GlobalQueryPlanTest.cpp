#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/GlobalQueryNode.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace NES;

class GlobalQueryPlanTest : public testing::Test {

  public:
    static void SetUpTestCase() {
        setupLogging("GlobalQueryPlanTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup GlobalQueryPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {}

    /* Will be called before a test is executed. */
    void TearDown() override {
        NES_INFO("Setup GlobalQueryPlanTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down GlobalQueryPlanTest test class.");
    }
};

/**
 * @brief This test is for creation of a global query plan
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlan) {
    try {
        GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
        SUCCEED();
    } catch (Exception ex) {
        FAIL();
    }
}

/**
 * @brief This test is for creation of a global query plan and adding a query plan with empty query id
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanAndAddingQueryPlanWithEmptyId) {
    try {
        QueryPlanPtr queryPlan = QueryPlan::create();
        GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
        globalQueryPlan->addQueryPlan(queryPlan);
        FAIL();
    } catch (Exception ex) {
        SUCCEED();
    }
}

/**
 * @brief This test is for creation of a global query plan and extracting a list of global query nodes with specific
 * type of operators
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanAndGetAllNewGlobalQueryNodesWithATypeOfLogicalOperator) {

    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").merge(&subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    plan->setQueryId("Q1");
    globalQueryPlan->addQueryPlan(plan);
    std::vector<GlobalQueryNodePtr> logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();

    NES_DEBUG("GlobalQueryNodeTest: Empty global query node should return true when asked if it is empty");
    EXPECT_TRUE(logicalSinkNodes.size() == 1);
}

/**
 * @brief This test is for creation of a global query plan and adding and removing the query plan
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanAndAddAndRemoveQuery) {

    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("default_logical").filter(lessExpression);
    auto query = Query::from("default_logical").merge(&subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    plan->setQueryId("Q1");
    globalQueryPlan->addQueryPlan(plan);
    std::vector<GlobalQueryNodePtr> logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    NES_DEBUG("GlobalQueryPlanTest: should 1 global query node with logical sink");
    EXPECT_TRUE(logicalSinkNodes.size() == 1);
    globalQueryPlan->removeQuery("Q1");
    logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    NES_DEBUG("GlobalQueryPlanTest: Should return empty global query nodes");
    EXPECT_TRUE(logicalSinkNodes.empty());
}
