/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/QueryMerger/SyntaxBasedEqualQueryMergerRule.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryMetaData.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
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
    void TearDown() override { NES_INFO("Setup GlobalQueryPlanTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down GlobalQueryPlanTest test class."); }
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
    auto listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 1);
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
    NES_DEBUG("GlobalQueryPlanTest: should return 1 global query node with logical sink operator");
    auto listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the global query plan");
    auto query2 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    plan2->setQueryId(2);
    globalQueryPlan->addQueryPlan(plan2);

    //Assert
    NES_DEBUG("GlobalQueryPlanTest: should return 2 global query node with logical sink");
    listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 2);
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
    auto listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 1);

    NES_DEBUG("GlobalQueryPlanTest: Removing the query plan for the query with id Q1");
    globalQueryPlan->removeQuery(1);

    //Assert
    NES_DEBUG("GlobalQueryPlanTest: Should return empty global query nodes");
    listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 1);
    EXPECT_TRUE(listOfGQMsToDeploy[0]->isEmpty());
}

/**
 * @brief This test is for creation of a global query plan by adding multiple query plans and computing the Meta Data information
 */
TEST_F(GlobalQueryPlanTest, testUpdateMetaDataInformationForGlobalQueryPlanWithMultipleQueries) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    plan1->setQueryId(queryId1);
    globalQueryPlan->addQueryPlan(plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the global query plan");
    auto query2 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    plan2->setQueryId(queryId2);
    globalQueryPlan->addQueryPlan(plan2);

    //Assert To check if Global Query Plan is created properly
    //Get MetaData information
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaData = globalQueryPlan->getSharedQueryMetaDataToDeploy();

    //Assert
    EXPECT_TRUE(sharedQueryMetaData.size() == 2);
    auto queryIdToSinkGQNs1 = sharedQueryMetaData[0]->getQueryIdToSinkGQNMap();
    EXPECT_EQ(queryIdToSinkGQNs1.size(), 1);
    EXPECT_TRUE(queryIdToSinkGQNs1.find(queryId1) != queryIdToSinkGQNs1.end());
    auto queryIdToSinkGQNs2 = sharedQueryMetaData[1]->getQueryIdToSinkGQNMap();
    EXPECT_EQ(queryIdToSinkGQNs2.size(), 1);
    EXPECT_TRUE(queryIdToSinkGQNs2.find(queryId2) != queryIdToSinkGQNs1.end());
}

/**
 * @brief This test is for updating Meta Data information after applying L0 merging
 */
TEST_F(GlobalQueryPlanTest, testUpdateMetaDataInformationForGlobalQueryPlanWithMultipleQueriesAfterL0Merge) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto lessExpression = Attribute("field_1") <= 10;
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    plan1->setQueryId(queryId1);
    globalQueryPlan->addQueryPlan(plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the global query plan");
    auto query2 = Query::from("default_logical").filter(lessExpression).sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    plan2->setQueryId(queryId2);
    globalQueryPlan->addQueryPlan(plan2);

    //Assert To check if Global Query Plan is created properly
    NES_DEBUG("GlobalQueryPlanTest: should return 2 global query node with logical sink");
    auto listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 2);

    //Apply L0 query merger rule
    SyntaxBasedEqualQueryMergerRulePtr l0MergerRule = SyntaxBasedEqualQueryMergerRule::create();
    l0MergerRule->apply(globalQueryPlan);

    //Get MetaData information
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaData = globalQueryPlan->getSharedQueryMetaDataToDeploy();

    //Assert
    EXPECT_TRUE(sharedQueryMetaData.size() == 1);
    auto queryIdToSinkGQNsMap = sharedQueryMetaData[0]->getQueryIdToSinkGQNMap();
    EXPECT_TRUE(queryIdToSinkGQNsMap.find(queryId1) != queryIdToSinkGQNsMap.end());
    EXPECT_TRUE(queryIdToSinkGQNsMap.find(queryId2) != queryIdToSinkGQNsMap.end());
}

/**
 * @brief This test is for updating Meta Data information after applying L0 merging on 3 queries where 2 are identical
 */
TEST_F(GlobalQueryPlanTest, testUpdateMetaDataInformationForGlobalQueryPlanWithMixedQueriesAfterL0Merge) {

    NES_DEBUG("GlobalQueryPlanTest: creating an empty global query plan");
    GlobalQueryPlanPtr globalQueryPlan = GlobalQueryPlan::create();
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("car").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    QueryId queryId1 = PlanIdGenerator::getNextQueryId();
    plan1->setQueryId(queryId1);
    globalQueryPlan->addQueryPlan(plan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the global query plan");
    auto query2 = Query::from("car").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    QueryId queryId2 = PlanIdGenerator::getNextQueryId();
    plan2->setQueryId(queryId2);
    globalQueryPlan->addQueryPlan(plan2);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the global query plan");
    auto lessExpression = Attribute("field_1") <= 10;
    auto query3 = Query::from("car").filter(lessExpression).sink(printSinkDescriptor);
    auto plan3 = query3.getQueryPlan();
    QueryId queryId3 = PlanIdGenerator::getNextQueryId();
    plan3->setQueryId(queryId3);
    globalQueryPlan->addQueryPlan(plan3);

    //Assert To check if Global Query Plan is created properly
    NES_DEBUG("GlobalQueryPlanTest: should return 3 global query metadata");
    auto listOfGQMsToDeploy = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    EXPECT_TRUE(listOfGQMsToDeploy.size() == 3);

    //Apply L0 query merger rule
    SyntaxBasedEqualQueryMergerRulePtr l0MergerRule = SyntaxBasedEqualQueryMergerRule::create();
    l0MergerRule->apply(globalQueryPlan);

    //Get MetaData information
    std::vector<SharedQueryMetaDataPtr> sharedQueryMetaData = globalQueryPlan->getSharedQueryMetaDataToDeploy();

    //Assert
    EXPECT_TRUE(sharedQueryMetaData.size() == 2);
    auto queryIdToSinkGQNMap1 = sharedQueryMetaData[0]->getQueryIdToSinkGQNMap();
    EXPECT_TRUE(queryIdToSinkGQNMap1.find(queryId1) != queryIdToSinkGQNMap1.end());
    EXPECT_TRUE(queryIdToSinkGQNMap1.find(queryId2) != queryIdToSinkGQNMap1.end());

    globalQueryPlan->removeQuery(queryId1);
    //Get MetaData information
    sharedQueryMetaData = globalQueryPlan->getSharedQueryMetaDataToDeploy();
    queryIdToSinkGQNMap1 = sharedQueryMetaData[0]->getQueryIdToSinkGQNMap();
    EXPECT_TRUE(queryIdToSinkGQNMap1.find(queryId1) == queryIdToSinkGQNMap1.end());
    EXPECT_TRUE(queryIdToSinkGQNMap1.find(queryId2) != queryIdToSinkGQNMap1.end());

    auto queryIdToSinkGQNMap2 = sharedQueryMetaData[1]->getQueryIdToSinkGQNMap();
    EXPECT_EQ(queryIdToSinkGQNMap2.size(), 1);
    EXPECT_TRUE(queryIdToSinkGQNMap2.find(queryId3) != queryIdToSinkGQNMap1.end());
}
