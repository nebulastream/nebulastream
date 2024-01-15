/*
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

#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <gtest/gtest.h>

using namespace NES;

class GlobalExecutionPlanTest : public Testing::BaseUnitTest {

  public:
    /* Will be called before a test is executed. */
    static void SetUpTestCase() {
        Logger::setupLogging("GlobalExecutionPlanTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GlobalExecutionPlanTest test case.");
    }
};

/**
 * @brief This test is for validating different behaviour for an empty global execution plan
 */
TEST_F(GlobalExecutionPlanTest, testCreateEmptyGlobalExecutionPlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    std::string actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan;

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node without any plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithoutAnyPlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10, properties);
    const auto executionNode = Optimizer::ExecutionNode::createExecutionNode(topologyNode);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    std::string actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId()) + ")\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with one plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithOnePlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10, properties);
    const auto executionNode = Optimizer::ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("car");
    auto query = Query::from("truck").unionWith(subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    SharedQueryId sharedQueryId = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan = DecomposedQueryPlan::create(decomposedQueryPlanId, sharedQueryId);
    for (const auto& rootOperator : plan->getRootOperators()) {
        decomposedQueryPlan->addRootOperator(rootOperator);
    }
    decomposedQueryPlan->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId, decomposedQueryPlan);

    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string actualPlan = globalExecutionPlan->getAsString();

    NES_INFO("GlobalExecutionPlanTest: Actual plan: \n{}", actualPlan);

    NES_INFO("GlobalExecutionPlanTest: queryPlan.toString(): \n{}", plan->toString());

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |  |--"
        + plan->getRootOperators()[0]->getChildren()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |  |--"
        + plan->getRootOperators()[0]->getChildren()[0]->getChildren()[1]->toString() + "\n";
    NES_INFO("\n {}", expectedPlan);
    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10, properties);
    const auto& executionNode = Optimizer::ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 = DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId);
    for (const auto& rootOperator : plan1->getRootOperators()) {
        decomposedQueryPlan1->addRootOperator(rootOperator);
    }
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId, decomposedQueryPlan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    DecomposedQueryPlanId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 = DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId);
    for (const auto& rootOperator : plan2->getRootOperators()) {
        decomposedQueryPlan2->addRootOperator(rootOperator);
    }
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId, decomposedQueryPlan2);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId1)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId2)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan2->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan for different queryIdAndCatalogEntryMapping
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlanForDifferentqueries) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10, properties);
    const auto& executionNode = Optimizer::ExecutionNode::createExecutionNode(topologyNode);

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 = DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId1);
    for (const auto& rootOperator : plan1->getRootOperators()) {
        decomposedQueryPlan1->addRootOperator(rootOperator);
    }
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId1, decomposedQueryPlan1);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 = DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId2);
    for (const auto& rootOperator : plan2->getRootOperators()) {
        decomposedQueryPlan2->addRootOperator(rootOperator);
    }
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId2, decomposedQueryPlan2);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId1) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId1)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId2) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId2)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan2->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with single execution node with 4 plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithFourPlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //create execution node
    TopologyNodePtr topologyNode = TopologyNode::create(1, "localhost", 3200, 3300, 10, properties);
    const auto& executionNode = Optimizer::ExecutionNode::createExecutionNode(topologyNode);

    //query sub plans for query 1
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor11 = PrintSinkDescriptor::create();
    auto query11 = Query::from("default_logical").sink(printSinkDescriptor11);
    auto plan11 = query11.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId11 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan11 = DecomposedQueryPlan::create(decomposedQueryPlanId11, sharedQueryId1);
    for (const auto& rootOperator : plan11->getRootOperators()) {
        decomposedQueryPlan11->addRootOperator(rootOperator);
    }
    decomposedQueryPlan11->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId1, decomposedQueryPlan11);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor12 = PrintSinkDescriptor::create();
    auto query12 = Query::from("default_logical").sink(printSinkDescriptor12);
    auto plan12 = query12.getQueryPlan();
    DecomposedQueryPlanId decomposedQueryPlanId12 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan12 = DecomposedQueryPlan::create(decomposedQueryPlanId12, sharedQueryId1);
    for (const auto& rootOperator : plan12->getRootOperators()) {
        decomposedQueryPlan12->addRootOperator(rootOperator);
    }
    decomposedQueryPlan12->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId1, decomposedQueryPlan12);

    //query sub plans for query 2
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor21 = PrintSinkDescriptor::create();
    auto query21 = Query::from("default_logical").sink(printSinkDescriptor21);
    auto plan21 = query21.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId21 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan21 = DecomposedQueryPlan::create(decomposedQueryPlanId21, sharedQueryId2);
    for (const auto& rootOperator : plan21->getRootOperators()) {
        decomposedQueryPlan21->addRootOperator(rootOperator);
    }
    decomposedQueryPlan21->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId2, decomposedQueryPlan21);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor22 = PrintSinkDescriptor::create();
    auto query22 = Query::from("default_logical").sink(printSinkDescriptor22);
    auto plan22 = query22.getQueryPlan();
    DecomposedQueryPlanId decomposedQueryPlanId22 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan22 = DecomposedQueryPlan::create(decomposedQueryPlanId22, sharedQueryId2);
    for (const auto& rootOperator : plan22->getRootOperators()) {
        decomposedQueryPlan22->addRootOperator(rootOperator);
    }
    decomposedQueryPlan22->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode->registerNewDecomposedQueryPlan(sharedQueryId2, decomposedQueryPlan22);

    globalExecutionPlan->addExecutionNode(executionNode);
    globalExecutionPlan->addExecutionNodeAsRoot(executionNode);

    const std::string& actualPlan = globalExecutionPlan->getAsString();

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId1) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId11)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan11->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan11->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId1) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId12)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan12->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan12->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId2) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId21)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan21->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan21->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId2) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId22)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan22->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan22->getRootOperators()[0]->getChildren()[0]->toString() + "\n";
    NES_INFO("Actual query plan \n{}", actualPlan);

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with two execution nodes with one plan each
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithTwoExecutionNodesEachWithOnePlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //create execution node
    uint64_t node1Id = 1;
    TopologyNodePtr topologyNode1 = TopologyNode::create(node1Id, "localhost", 3200, 3300, 10, properties);
    auto executionNode1 = Optimizer::ExecutionNode::createExecutionNode(topologyNode1);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 = DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId1);
    for (const auto& rootOperator : plan1->getRootOperators()) {
        decomposedQueryPlan1->addRootOperator(rootOperator);
    }
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode1->registerNewDecomposedQueryPlan(sharedQueryId1, decomposedQueryPlan1);

    //create execution node
    uint64_t node2Id = 2;
    TopologyNodePtr topologyNode2 = TopologyNode::create(node2Id, "localhost", 3200, 3300, 10, properties);
    auto executionNode2 = Optimizer::ExecutionNode::createExecutionNode(topologyNode2);

    //Add parent child relationship among topology nodes
    topologyNode1->addChild(topologyNode2);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 = DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId2);
    for (const auto& rootOperator : plan2->getRootOperators()) {
        decomposedQueryPlan2->addRootOperator(rootOperator);
    }
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode2->registerNewDecomposedQueryPlan(sharedQueryId2, decomposedQueryPlan2);

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);

    const std::string& actualPlan = globalExecutionPlan->getAsString();

    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode1->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode1->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId1) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId1)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + std::to_string(executionNode2->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode2->getTopologyNode()->getId())
        + ")\n"
          "|  | QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId2) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId2)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |--"
        + plan2->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}

/**
 * @brief This test is for validating behaviour for a global execution plan with nested execution node with one plan for different queryIdAndCatalogEntryMapping
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithTwoExecutionNodesEachWithOnePlanToString) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    //Create topology nodes
    uint64_t node1Id = 1;
    TopologyNodePtr topologyNode1 = TopologyNode::create(node1Id, "localhost", 3200, 3300, 10, properties);
    uint64_t node2Id = 2;
    TopologyNodePtr topologyNode2 = TopologyNode::create(node2Id, "localhost", 3200, 3300, 10, properties);
    uint64_t node3Id = 3;
    TopologyNodePtr topologyNode3 = TopologyNode::create(node3Id, "localhost", 3200, 3300, 10, properties);
    uint64_t node4Id = 4;
    TopologyNodePtr topologyNode4 = TopologyNode::create(node4Id, "localhost", 3200, 3300, 10, properties);

    //Add parent child relationship
    topologyNode1->addChild(topologyNode2);
    topologyNode2->addChild(topologyNode3);
    topologyNode3->addChild(topologyNode4);

    //create execution node 1
    const auto executionNode1 = Optimizer::ExecutionNode::createExecutionNode(topologyNode1);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 = DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId1);
    for (const auto& rootOperator : plan1->getRootOperators()) {
        decomposedQueryPlan1->addRootOperator(rootOperator);
    }
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode1->registerNewDecomposedQueryPlan(sharedQueryId1, decomposedQueryPlan1);

    //create execution node 2
    const auto executionNode2 = Optimizer::ExecutionNode::createExecutionNode(topologyNode2);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 = DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId2);
    for (const auto& rootOperator : plan2->getRootOperators()) {
        decomposedQueryPlan2->addRootOperator(rootOperator);
    }
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode2->registerNewDecomposedQueryPlan(sharedQueryId2, decomposedQueryPlan2);

    //create execution node 3
    const auto executionNode3 = Optimizer::ExecutionNode::createExecutionNode(topologyNode3);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query3 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan3 = query3.getQueryPlan();
    SharedQueryId sharedQueryId3 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId3 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan3 = DecomposedQueryPlan::create(decomposedQueryPlanId3, sharedQueryId3);
    for (const auto& rootOperator : plan3->getRootOperators()) {
        decomposedQueryPlan3->addRootOperator(rootOperator);
    }
    decomposedQueryPlan3->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode3->registerNewDecomposedQueryPlan(sharedQueryId3, decomposedQueryPlan3);

    globalExecutionPlan->addExecutionNode(executionNode1);
    globalExecutionPlan->addExecutionNode(executionNode2);
    globalExecutionPlan->addExecutionNode(executionNode3);

    //create execution node 4
    const auto executionNode4 = Optimizer::ExecutionNode::createExecutionNode(topologyNode4);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto query4 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan4 = query4.getQueryPlan();
    SharedQueryId sharedQueryId4 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryPlanId decomposedQueryPlanId4 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan4 = DecomposedQueryPlan::create(decomposedQueryPlanId4, sharedQueryId4);
    for (const auto& rootOperator : plan4->getRootOperators()) {
        decomposedQueryPlan4->addRootOperator(rootOperator);
    }
    decomposedQueryPlan4->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    executionNode4->registerNewDecomposedQueryPlan(sharedQueryId4, decomposedQueryPlan4);

    globalExecutionPlan->addExecutionNode(executionNode4);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + std::to_string(executionNode1->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode1->getTopologyNode()->getId())
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId1) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId1)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + std::to_string(executionNode2->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode2->getTopologyNode()->getId())
        + ")\n"
          "|  | QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId2) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId2)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |--"
        + plan2->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |--ExecutionNode(id:"
        + std::to_string(executionNode3->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode3->getTopologyNode()->getId())
        + ")\n"
          "|  |  | QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId3) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId3)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  |  "
        + plan3->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |  |--"
        + plan3->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |  |--ExecutionNode(id:"
        + std::to_string(executionNode4->getId())
        + ", ip:localhost, topologyId:" + std::to_string(executionNode4->getTopologyNode()->getId())
        + ")\n"
          "|  |  |  | QuerySubPlan(SharedQueryId:"
        + std::to_string(sharedQueryId4) + ", DecomposedQueryPlanId:" + std::to_string(decomposedQueryPlanId4)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  |  |  "
        + plan4->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |  |  |--"
        + plan4->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}
