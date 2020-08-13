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
        NES_DEBUG("GlobalQueryPlanTest: create an empty global query plan");
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

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without id to the global query plan");
    QueryPlanPtr queryPlan = QueryPlan::create();
    //Assert
    EXPECT_THROW(globalQueryPlan->addQueryPlan(queryPlan), Exception);
}

/**
 * @brief This test is for creation of a global query plan and extracting a list of global query nodes with specific
 * type of operators
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanAndGetAllNewGlobalQueryNodesWithATypeOfLogicalOperator) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    plan->setQueryId(1);
    globalQueryPlan->addQueryPlan(plan);

    //Assert
    NES_DEBUG("GlobalQueryPlanTest: A global query node containing operator of type SinkLogicalOperatorNode should be returned");
    std::vector<GlobalQueryNodePtr> logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    EXPECT_TRUE(logicalSinkNodes.size() == 1);
}

/**
 * @brief This test is for creation of a global query plan and adding query plan twice
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanByAddingSameQueryPlanTwice) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding same query plan twice to the global query plan");
    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    auto plan = query.getQueryPlan();
    plan->setQueryId(1);
    globalQueryPlan->addQueryPlan(plan);
    //Assert
    EXPECT_THROW(globalQueryPlan->addQueryPlan(plan), Exception);
}

/**
 * @brief This test is for creation of a global query plan and adding multiple query plans
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanAndAddMultipleQueries) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    plan1->setQueryId(1);
    globalQueryPlan->addQueryPlan(plan1);

    //Assert
    std::vector<GlobalQueryNodePtr> logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    NES_DEBUG("GlobalQueryPlanTest: should return 1 global query node with logical sink operator");
    EXPECT_TRUE(logicalSinkNodes.size() == 1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the global query plan");
    auto query2 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    plan2->setQueryId(2);
    globalQueryPlan->addQueryPlan(plan2);

    //Assert
    NES_DEBUG("GlobalQueryPlanTest: should return 2 global query node with logical sink");
    logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    EXPECT_TRUE(logicalSinkNodes.size() == 2);
}

/**
 * @brief This test is for creation of a global query plan and adding and removing the query plan
 */
TEST_F(GlobalQueryPlanTest, testNewGlobalQueryPlanAndAddAndRemoveQuery) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto lessExpression = Attribute("field_1") <= 10;
    auto query = Query::from("default_logical").filter(lessExpression).sink(PrintSinkDescriptor::create());
    auto plan = query.getQueryPlan();
    plan->setQueryId(1);
    globalQueryPlan->addQueryPlan(plan);

    //Assert
    NES_DEBUG("GlobalQueryPlanTest: should 1 global query node with logical sink");
    std::vector<GlobalQueryNodePtr> logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    EXPECT_TRUE(logicalSinkNodes.size() == 1);

    NES_DEBUG("GlobalQueryPlanTest: Removing the query plan for the query with id Q1");
    globalQueryPlan->removeQuery(1);

    //Assert
    NES_DEBUG("GlobalQueryPlanTest: Should return empty global query nodes");
    logicalSinkNodes = globalQueryPlan->getAllNewGlobalQueryNodesWithOperatorType<SinkLogicalOperatorNode>();
    EXPECT_TRUE(logicalSinkNodes.empty());
}
