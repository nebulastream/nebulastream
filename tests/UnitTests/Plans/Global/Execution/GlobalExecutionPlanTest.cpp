#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <API/Query.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

using namespace NES;

class GlobalExecutionPlanTest : public testing::Test {

  public:
    /* Will be called before a test is executed. */
    void SetUp() {
        setupLogging("GlobalExecutionPlanTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup GlobalExecutionPlanTest test case.");
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        NES_INFO("Setup GlobalExecutionPlanTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down GlobalExecutionPlanTest test class.");
    }
};

/**
 * @brief This test is for validating different behaviour for an empty global execution plan
 */
TEST_F(GlobalExecutionPlanTest, testCreateEmptyGlobalExecutionPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan);
    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};
    expectedPlan["executionNodes"] = web::json::value::array();

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node without any plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithoutAnyPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    globalExecutionPlan->addExecutionNode(executionNode);

    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan);
    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};
    expectedPlan["executionNodes"] = web::json::value::array();

    web::json::value expectedExecutionNode{};
    expectedExecutionNode["executionNodeId"] = web::json::value::number(executionNode->getId());
    expectedExecutionNode["topologyNodeId"] = web::json::value::number(executionNode->getTopologyNode()->getId());
    expectedExecutionNode["topologyNodeIpAddress"] = web::json::value::string(executionNode->getTopologyNode()->getIpAddress());
    expectedExecutionNode["ScheduledQueries"] = web::json::value::array();

    expectedPlan["executionNodes"] = web::json::value::array({expectedExecutionNode});
    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with one plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithOnePlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    QueryId queryId = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId = UtilityFunctions::getNextQuerySubPlanId();
    plan->setQueryId(queryId);
    plan->setQuerySubPlanId(querySubPlanId);
    executionNode->addNewQuerySubPlan(queryId, plan);

    globalExecutionPlan->addExecutionNode(executionNode);

    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan);
    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};
    expectedPlan["executionNodes"] = web::json::value::array();

    web::json::value expectedExecutionNode{};
    expectedExecutionNode["executionNodeId"] = web::json::value::number(executionNode->getId());
    expectedExecutionNode["topologyNodeId"] = web::json::value::number(executionNode->getTopologyNode()->getId());
    expectedExecutionNode["topologyNodeIpAddress"] = web::json::value::string(executionNode->getTopologyNode()->getIpAddress());

    web::json::value scheduledQueries{};
    scheduledQueries["queryId"] = web::json::value::number(queryId);
    web::json::value scheduledSubQuery{};
    scheduledSubQuery["operator"] = web::json::value::string("SOURCE(OP-1)=>SINK(OP-2)");
    scheduledSubQuery["querySubPlanId"] = web::json::value::number(querySubPlanId);
    scheduledQueries["querySubPlans"] = web::json::value::array({scheduledSubQuery});
    expectedExecutionNode["ScheduledQueries"] = web::json::value::array({scheduledQueries});

    expectedPlan["executionNodes"] = web::json::value::array({expectedExecutionNode});
    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = UtilityFunctions::getNextQuerySubPlanId();
    plan1->setQueryId(queryId);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode->addNewQuerySubPlan(queryId, plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    QuerySubPlanId querySubPlanId2 = UtilityFunctions::getNextQuerySubPlanId();
    plan2->setQueryId(queryId);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode->addNewQuerySubPlan(queryId, plan2);

    globalExecutionPlan->addExecutionNode(executionNode);

    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan);
    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};
    expectedPlan["executionNodes"] = web::json::value::array();

    web::json::value expectedExecutionNode{};
    expectedExecutionNode["executionNodeId"] = web::json::value::number(executionNode->getId());
    expectedExecutionNode["topologyNodeId"] = web::json::value::number(executionNode->getTopologyNode()->getId());
    expectedExecutionNode["topologyNodeIpAddress"] = web::json::value::string(executionNode->getTopologyNode()->getIpAddress());

    web::json::value scheduledQueries{};
    scheduledQueries["queryId"] = web::json::value::number(queryId);
    std::vector<web::json::value> scheduledSubQueries;
    web::json::value scheduledSubQuery1{};
    scheduledSubQuery1["operator"] = web::json::value::string("SOURCE(OP-1)=>SINK(OP-2)");
    scheduledSubQuery1["querySubPlanId"] = web::json::value::number(querySubPlanId1);
    scheduledSubQueries.push_back(scheduledSubQuery1);
    web::json::value scheduledSubQuery2{};
    scheduledSubQuery2["operator"] = web::json::value::string("SOURCE(OP-3)=>SINK(OP-4)");
    scheduledSubQuery2["querySubPlanId"] = web::json::value::number(querySubPlanId2);
    scheduledSubQueries.push_back(scheduledSubQuery2);
    scheduledQueries["querySubPlans"] = web::json::value::array(scheduledSubQueries);
    expectedExecutionNode["ScheduledQueries"] = web::json::value::array({scheduledQueries});

    expectedPlan["executionNodes"] = web::json::value::array({expectedExecutionNode});
    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan for different queries
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlanForDifferentqueries) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = UtilityFunctions::getNextQuerySubPlanId();
    plan1->setQueryId(queryId1);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode->addNewQuerySubPlan(queryId1, plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    QuerySubPlanId querySubPlanId2 = UtilityFunctions::getNextQuerySubPlanId();
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    plan2->setQueryId(queryId2);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode->addNewQuerySubPlan(queryId2, plan2);

    globalExecutionPlan->addExecutionNode(executionNode);

    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan, queryId1);
    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};
    expectedPlan["executionNodes"] = web::json::value::array();

    web::json::value expectedExecutionNode{};
    expectedExecutionNode["executionNodeId"] = web::json::value::number(executionNode->getId());
    expectedExecutionNode["topologyNodeId"] = web::json::value::number(executionNode->getTopologyNode()->getId());
    expectedExecutionNode["topologyNodeIpAddress"] = web::json::value::string(executionNode->getTopologyNode()->getIpAddress());

    web::json::value scheduledQueries{};
    scheduledQueries["queryId"] = web::json::value::number(queryId1);
    std::vector<web::json::value> scheduledSubQueries;
    web::json::value scheduledSubQuery1{};
    scheduledSubQuery1["operator"] = web::json::value::string("SOURCE(OP-1)=>SINK(OP-2)");
    scheduledSubQuery1["querySubPlanId"] = web::json::value::number(querySubPlanId1);
    scheduledSubQueries.push_back(scheduledSubQuery1);
//    web::json::value scheduledSubQuery2{};
//    scheduledSubQuery2["operator"] = web::json::value::string("SOURCE(OP-3)=>SINK(OP-4)");
//    scheduledSubQuery2["querySubPlanId"] = web::json::value::number(querySubPlanId2);
//    scheduledSubQueries.push_back(scheduledSubQuery2);
    scheduledQueries["querySubPlans"] = web::json::value::array(scheduledSubQueries);
    expectedExecutionNode["ScheduledQueries"] = web::json::value::array({scheduledQueries});

    expectedPlan["executionNodes"] = web::json::value::array({expectedExecutionNode});
    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with 4 plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithFourPlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topologyNode);

    //query sub plans for query 1
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor11 = PrintSinkDescriptor::create();
    auto query11 = Query::from("default_logical").sink(printSinkDescriptor11);
    auto plan11 = query11.getQueryPlan();
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId11 = UtilityFunctions::getNextQuerySubPlanId();
    plan11->setQueryId(queryId1);
    plan11->setQuerySubPlanId(querySubPlanId11);
    executionNode->addNewQuerySubPlan(queryId1, plan11);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor12 = PrintSinkDescriptor::create();
    auto query12 = Query::from("default_logical").sink(printSinkDescriptor12);
    auto plan12 = query12.getQueryPlan();
    QuerySubPlanId querySubPlanId12 = UtilityFunctions::getNextQuerySubPlanId();
    plan12->setQueryId(queryId1);
    plan12->setQuerySubPlanId(querySubPlanId12);
    executionNode->addNewQuerySubPlan(queryId1, plan12);

    //query sub plans for query 2
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor21 = PrintSinkDescriptor::create();
    auto query21 = Query::from("default_logical").sink(printSinkDescriptor21);
    auto plan21 = query21.getQueryPlan();
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId21 = UtilityFunctions::getNextQuerySubPlanId();
    plan21->setQueryId(queryId2);
    plan21->setQuerySubPlanId(querySubPlanId21);
    executionNode->addNewQuerySubPlan(queryId2, plan21);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor22 = PrintSinkDescriptor::create();
    auto query22 = Query::from("default_logical").sink(printSinkDescriptor22);
    auto plan22 = query22.getQueryPlan();
    QuerySubPlanId querySubPlanId22 = UtilityFunctions::getNextQuerySubPlanId();
    plan22->setQueryId(queryId2);
    plan22->setQuerySubPlanId(querySubPlanId22);
    executionNode->addNewQuerySubPlan(queryId2, plan22);

    globalExecutionPlan->addExecutionNode(executionNode);

    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan);
    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};
    expectedPlan["executionNodes"] = web::json::value::array();

    web::json::value expectedExecutionNode{};
    expectedExecutionNode["executionNodeId"] = web::json::value::number(executionNode->getId());
    expectedExecutionNode["topologyNodeId"] = web::json::value::number(executionNode->getTopologyNode()->getId());
    expectedExecutionNode["topologyNodeIpAddress"] = web::json::value::string(executionNode->getTopologyNode()->getIpAddress());

    web::json::value scheduledSubQueries1{};
    scheduledSubQueries1["queryId"] = web::json::value::number(queryId1);
    std::vector<web::json::value> scheduledSubQueriesFor1;
    web::json::value scheduledSubQuery11{};
    scheduledSubQuery11["operator"] = web::json::value::string("SOURCE(OP-1)=>SINK(OP-2)");
    scheduledSubQuery11["querySubPlanId"] = web::json::value::number(querySubPlanId11);
    scheduledSubQueriesFor1.push_back(scheduledSubQuery11);
    web::json::value scheduledSubQuery12{};
    scheduledSubQuery12["operator"] = web::json::value::string("SOURCE(OP-3)=>SINK(OP-4)");
    scheduledSubQuery12["querySubPlanId"] = web::json::value::number(querySubPlanId12);
    scheduledSubQueriesFor1.push_back(scheduledSubQuery12);
    scheduledSubQueries1["querySubPlans"] = web::json::value::array(scheduledSubQueriesFor1);

    web::json::value scheduledSubQueries2{};
    scheduledSubQueries2["queryId"] = web::json::value::number(queryId2);
    std::vector<web::json::value> scheduledSubQueriesFor2;
    web::json::value scheduledSubQuery21{};
    scheduledSubQuery21["operator"] = web::json::value::string("SOURCE(OP-5)=>SINK(OP-6)");
    scheduledSubQuery21["querySubPlanId"] = web::json::value::number(querySubPlanId21);
    scheduledSubQueriesFor2.push_back(scheduledSubQuery21);
    web::json::value scheduledSubQuery22{};
    scheduledSubQuery22["operator"] = web::json::value::string("SOURCE(OP-7)=>SINK(OP-8)");
    scheduledSubQuery22["querySubPlanId"] = web::json::value::number(querySubPlanId22);
    scheduledSubQueriesFor2.push_back(scheduledSubQuery22);
    scheduledSubQueries2["querySubPlans"] = web::json::value::array(scheduledSubQueriesFor2);

    expectedExecutionNode["ScheduledQueries"] = web::json::value::array({scheduledSubQueries1, scheduledSubQueries2});

    expectedPlan["executionNodes"] = web::json::value::array({expectedExecutionNode});
    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with two execution nodes with one plan each
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithTwoExecutionNodesEachWithOnePlan) {

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();

    //create execution node
    TopologyNodePtr topologyNode1 = TopologyNode::create(UtilityFunctions::getNextNodeId(), "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode1 = ExecutionNode::createExecutionNode(topologyNode1);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId1 = UtilityFunctions::getNextQuerySubPlanId();
    plan1->setQueryId(queryId1);
    plan1->setQuerySubPlanId(querySubPlanId1);
    executionNode1->addNewQuerySubPlan(queryId1, plan1);

    //create execution node
    TopologyNodePtr topologyNode2 = TopologyNode::create(UtilityFunctions::getNextNodeId(), "localhost", 3200, 3300, 10);
    const ExecutionNodePtr executionNode2 = ExecutionNode::createExecutionNode(topologyNode2);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    QueryId queryId2 = UtilityFunctions::getNextQueryId();
    QuerySubPlanId querySubPlanId2 = UtilityFunctions::getNextQuerySubPlanId();
    plan2->setQueryId(queryId2);
    plan2->setQuerySubPlanId(querySubPlanId2);
    executionNode2->addNewQuerySubPlan(queryId2, plan2);

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);

    const json::value& actualPlan = UtilityFunctions::getExecutionPlanAsJson(globalExecutionPlan);

    NES_INFO("Actual query plan " << actualPlan);

    web::json::value expectedPlan{};

    web::json::value expectedExecutionNode1{};
    expectedExecutionNode1["executionNodeId"] = web::json::value::number(executionNode1->getId());
    expectedExecutionNode1["topologyNodeId"] = web::json::value::number(executionNode1->getTopologyNode()->getId());
    expectedExecutionNode1["topologyNodeIpAddress"] = web::json::value::string(executionNode1->getTopologyNode()->getIpAddress());

    web::json::value scheduledQueries1{};
    scheduledQueries1["queryId"] = web::json::value::number(queryId1);
    web::json::value scheduledSubQuery1{};
    scheduledSubQuery1["operator"] = web::json::value::string("SOURCE(OP-1)=>SINK(OP-2)");
    scheduledSubQuery1["querySubPlanId"] = web::json::value::number(querySubPlanId1);
    scheduledQueries1["querySubPlans"] = web::json::value::array({scheduledSubQuery1});
    expectedExecutionNode1["ScheduledQueries"] = web::json::value::array({scheduledQueries1});

    web::json::value expectedExecutionNode2{};
    expectedExecutionNode2["executionNodeId"] = web::json::value::number(executionNode2->getId());
    expectedExecutionNode2["topologyNodeId"] = web::json::value::number(executionNode2->getTopologyNode()->getId());
    expectedExecutionNode2["topologyNodeIpAddress"] = web::json::value::string(executionNode2->getTopologyNode()->getIpAddress());

    web::json::value scheduledQueries2{};
    scheduledQueries2["queryId"] = web::json::value::number(queryId2);
    web::json::value scheduledSubQuery2{};
    scheduledSubQuery2["operator"] = web::json::value::string("SOURCE(OP-3)=>SINK(OP-4)");
    scheduledSubQuery2["querySubPlanId"] = web::json::value::number(querySubPlanId2);
    scheduledQueries2["querySubPlans"] = web::json::value::array({scheduledSubQuery2});
    expectedExecutionNode2["ScheduledQueries"] = web::json::value::array({scheduledQueries2});

    expectedPlan["executionNodes"] = web::json::value::array({expectedExecutionNode1, expectedExecutionNode2});
    ASSERT_EQ(expectedPlan, actualPlan);
}