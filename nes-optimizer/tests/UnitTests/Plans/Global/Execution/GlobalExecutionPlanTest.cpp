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
#include <Catalogs/Topology/Topology.hpp>
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
 * @brief This test is for validating behaviour for a global execution plan with single execution node with one plan
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithOnePlan) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto topology = Topology::create();
    auto topologyNodeId = WorkerId(1);
    topology->registerWorker(topologyNodeId, "localhost", 3200, 3300, 10, properties, 0, 0);

    //create execution node

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto subQuery = Query::from("car");
    auto query = Query::from("truck").unionWith(subQuery).sink(printSinkDescriptor);
    auto plan = query.getQueryPlan();
    SharedQueryId sharedQueryId = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryId = PlanIdGenerator::getNextDecomposedQueryPlanId();
    DecomposedQueryPlanVersion version = 1;
    auto decomposedQueryPlan =
        DecomposedQueryPlan::create(decomposedQueryId, sharedQueryId, topologyNodeId, plan->getRootOperators());
    decomposedQueryPlan->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    decomposedQueryPlan->setVersion(version);
    auto lockedTopologyNode = topology->lockTopologyNode(topologyNodeId);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan);
    globalExecutionPlan->addExecutionNodeAsRoot(topologyNodeId);

    const std::string actualPlan = globalExecutionPlan->getAsString();

    NES_INFO("GlobalExecutionPlanTest: Actual plan: \n{}", actualPlan);

    NES_INFO("GlobalExecutionPlanTest: queryPlan.toString(): \n{}", plan->toString());

    std::string expectedPlan = "ExecutionNode(id:" + topologyNodeId.toString()
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId.toString() + ", DecomposedQueryId:" + decomposedQueryId.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
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

    auto topology = Topology::create();
    auto topologyNodeId = WorkerId(1);
    topology->registerWorker(topologyNodeId, "localhost", 3200, 3300, 10, properties, 0, 0);
    DecomposedQueryPlanVersion version = 0;

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 =
        DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId, topologyNodeId, plan1->getRootOperators());
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    DecomposedQueryId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 =
        DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId, topologyNodeId, plan2->getRootOperators());
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);
    auto lockedTopologyNode = topology->lockTopologyNode(topologyNodeId);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    globalExecutionPlan->addExecutionNodeAsRoot(topologyNodeId);

    //create execution node
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan1);
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan2);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + topologyNodeId.toString()
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId1.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
                + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId2.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
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
 * @brief This test is for validating behaviour for a global execution plan with single execution node with two plan for different shared queries
 */
TEST_F(GlobalExecutionPlanTest, testGlobalExecutionPlanWithSingleExecutionNodeWithTwoPlanForDifferentqueries) {

    auto globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto topology = Topology::create();
    auto topologyNodeId = WorkerId(1);
    topology->registerWorker(topologyNodeId, "localhost", 3200, 3300, 10, properties, 0, 0);
    DecomposedQueryPlanVersion version = 0;

    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor1 = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor1);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 =
        DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId1, topologyNodeId, plan1->getRootOperators());
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor2 = PrintSinkDescriptor::create();
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor2);
    auto plan2 = query2.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 =
        DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId2, topologyNodeId, plan2->getRootOperators());
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    auto lockedTopologyNode = topology->lockTopologyNode(topologyNodeId);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    globalExecutionPlan->addExecutionNodeAsRoot(topologyNodeId);

    //create execution node
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan1);
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan2);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + topologyNodeId.toString()
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId1.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId1.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId2.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId2.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
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

    auto topology = Topology::create();
    auto topologyNodeId = WorkerId(1);
    topology->registerWorker(topologyNodeId, "localhost", 3200, 3300, 10, properties, 0, 0);
    DecomposedQueryPlanVersion version = 0;

    //query sub plans for query 1
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor11 = PrintSinkDescriptor::create();
    auto query11 = Query::from("default_logical").sink(printSinkDescriptor11);
    auto plan11 = query11.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId11 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan11 =
        DecomposedQueryPlan::create(decomposedQueryPlanId11, sharedQueryId1, topologyNodeId, plan11->getRootOperators());
    decomposedQueryPlan11->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor12 = PrintSinkDescriptor::create();
    auto query12 = Query::from("default_logical").sink(printSinkDescriptor12);
    auto plan12 = query12.getQueryPlan();
    DecomposedQueryId decomposedQueryPlanId12 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan12 =
        DecomposedQueryPlan::create(decomposedQueryPlanId12, sharedQueryId1, topologyNodeId, plan12->getRootOperators());
    decomposedQueryPlan12->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    //query sub plans for query 2
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the execution node");
    auto printSinkDescriptor21 = PrintSinkDescriptor::create();
    auto query21 = Query::from("default_logical").sink(printSinkDescriptor21);
    auto plan21 = query21.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId21 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan21 =
        DecomposedQueryPlan::create(decomposedQueryPlanId21, sharedQueryId2, topologyNodeId, plan21->getRootOperators());
    decomposedQueryPlan21->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    NES_DEBUG("GlobalQueryPlanTest: Adding another query plan to the execution node");
    auto printSinkDescriptor22 = PrintSinkDescriptor::create();
    auto query22 = Query::from("default_logical").sink(printSinkDescriptor22);
    auto plan22 = query22.getQueryPlan();
    DecomposedQueryId decomposedQueryPlanId22 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan22 =
        DecomposedQueryPlan::create(decomposedQueryPlanId22, sharedQueryId2, topologyNodeId, plan22->getRootOperators());
    decomposedQueryPlan22->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    auto lockedTopologyNode = topology->lockTopologyNode(topologyNodeId);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode);
    globalExecutionPlan->addExecutionNodeAsRoot(topologyNodeId);

    //create execution node
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan11);
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan12);
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan21);
    globalExecutionPlan->addDecomposedQueryPlan(topologyNodeId, decomposedQueryPlan22);
    const std::string& actualPlan = globalExecutionPlan->getAsString();

    std::string expectedPlan = "ExecutionNode(id:" + topologyNodeId.toString()
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId1.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId11.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan11->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan11->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId1.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId12.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan12->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan12->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId2.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId21.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan21->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan21->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId2.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId22.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
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
    auto topology = Topology::create();
    auto node1Id = WorkerId(1);
    topology->registerWorker(node1Id, "localhost", 3200, 3300, 10, properties, 0, 0);
    //create execution node
    auto node2Id = WorkerId(2);
    topology->registerWorker(node2Id, "localhost", 3200, 3300, 10, properties, 0, 0);
    //Add parent child relationship among topology nodes
    topology->addTopologyNodeAsChild(node1Id, node2Id);
    DecomposedQueryPlanVersion version = 0;

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 =
        DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId1, node1Id, plan1->getRootOperators());
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 =
        DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId2, node2Id, plan2->getRootOperators());
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    auto lockedTopologyNode1 = topology->lockTopologyNode(node1Id);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode1);
    auto lockedTopologyNode2 = topology->lockTopologyNode(node2Id);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode2);

    //create execution node
    globalExecutionPlan->addDecomposedQueryPlan(node1Id, decomposedQueryPlan1);
    globalExecutionPlan->addDecomposedQueryPlan(node2Id, decomposedQueryPlan2);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + node1Id.toString()
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId1.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId1.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + node2Id.toString()
        + ")\n"
          "|  | QuerySubPlan(SharedQueryId:"
        + sharedQueryId2.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId2.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
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
    auto topology = Topology::create();
    auto node1Id = WorkerId(1);
    topology->registerWorker(node1Id, "localhost", 3200, 3300, 10, properties, 0, 0);
    auto node2Id = WorkerId(2);
    topology->registerWorker(node2Id, "localhost", 3200, 3300, 10, properties, 0, 0);
    auto node3Id = WorkerId(3);
    topology->registerWorker(node3Id, "localhost", 3200, 3300, 10, properties, 0, 0);
    auto node4Id = WorkerId(4);
    topology->registerWorker(node4Id, "localhost", 3200, 3300, 10, properties, 0, 0);

    DecomposedQueryPlanVersion version = 0;

    //Add parent child relationship
    topology->removeTopologyNodeAsChild(node1Id, node3Id);
    topology->removeTopologyNodeAsChild(node1Id, node4Id);
    topology->addTopologyNodeAsChild(node2Id, node3Id);
    topology->addTopologyNodeAsChild(node3Id, node4Id);

    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto printSinkDescriptor = PrintSinkDescriptor::create();
    auto query1 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan1 = query1.getQueryPlan();
    SharedQueryId sharedQueryId1 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId1 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan1 =
        DecomposedQueryPlan::create(decomposedQueryPlanId1, sharedQueryId1, node1Id, plan1->getRootOperators());
    decomposedQueryPlan1->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    //create execution node 2
    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query2 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan2 = query2.getQueryPlan();
    SharedQueryId sharedQueryId2 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId2 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan2 =
        DecomposedQueryPlan::create(decomposedQueryPlanId2, sharedQueryId2, node2Id, plan2->getRootOperators());
    decomposedQueryPlan2->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    //create execution node 3
    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan without to the global query plan");
    auto query3 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan3 = query3.getQueryPlan();
    SharedQueryId sharedQueryId3 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId3 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan3 =
        DecomposedQueryPlan::create(decomposedQueryPlanId3, sharedQueryId3, node3Id, plan3->getRootOperators());
    decomposedQueryPlan3->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    //create execution node 4
    //Add sub plan
    NES_DEBUG("GlobalQueryPlanTest: Adding a query plan to the global query plan");
    auto query4 = Query::from("default_logical").sink(printSinkDescriptor);
    auto plan4 = query4.getQueryPlan();
    SharedQueryId sharedQueryId4 = PlanIdGenerator::getNextSharedQueryId();
    DecomposedQueryId decomposedQueryPlanId4 = PlanIdGenerator::getNextDecomposedQueryPlanId();
    auto decomposedQueryPlan4 =
        DecomposedQueryPlan::create(decomposedQueryPlanId4, sharedQueryId4, node4Id, plan4->getRootOperators());
    decomposedQueryPlan4->setState(QueryState::MARKED_FOR_DEPLOYMENT);

    auto lockedTopologyNode1 = topology->lockTopologyNode(node1Id);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode1);
    auto lockedTopologyNode2 = topology->lockTopologyNode(node2Id);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode2);
    auto lockedTopologyNode3 = topology->lockTopologyNode(node3Id);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode3);
    auto lockedTopologyNode4 = topology->lockTopologyNode(node4Id);
    globalExecutionPlan->registerExecutionNode(lockedTopologyNode4);

    //create execution node
    globalExecutionPlan->addDecomposedQueryPlan(node1Id, decomposedQueryPlan1);
    globalExecutionPlan->addDecomposedQueryPlan(node2Id, decomposedQueryPlan2);
    globalExecutionPlan->addDecomposedQueryPlan(node3Id, decomposedQueryPlan3);
    globalExecutionPlan->addDecomposedQueryPlan(node4Id, decomposedQueryPlan4);

    const std::string& actualPlan = globalExecutionPlan->getAsString();
    NES_INFO("Actual query plan \n{}", actualPlan);

    std::string expectedPlan = "ExecutionNode(id:" + node1Id.toString()
        + ")\n"
          "| QuerySubPlan(SharedQueryId:"
        + sharedQueryId1.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId1.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  "
        + plan1->getRootOperators()[0]->toString()
        + "\n"
          "|  |--"
        + plan1->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|--ExecutionNode(id:"
        + node2Id.toString()
        + ")\n"
          "|  | QuerySubPlan(SharedQueryId:"
        + sharedQueryId2.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId2.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  "
        + plan2->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |--"
        + plan2->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |--ExecutionNode(id:"
        + node3Id.toString()
        + ")\n"
          "|  |  | QuerySubPlan(SharedQueryId:"
        + sharedQueryId3.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId3.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  |  "
        + plan3->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |  |--"
        + plan3->getRootOperators()[0]->getChildren()[0]->toString()
        + "\n"
          "|  |  |--ExecutionNode(id:"
        + node4Id.toString()
        + ")\n"
          "|  |  |  | QuerySubPlan(SharedQueryId:"
        + sharedQueryId4.toString() + ", DecomposedQueryId:" + decomposedQueryPlanId4.toString() + ", DecomposedQueryVersion:" + std::to_string(version)
        + ", queryState:" + std::string(magic_enum::enum_name(QueryState::MARKED_FOR_DEPLOYMENT))
        + ")\n"
          "|  |  |  |  "
        + plan4->getRootOperators()[0]->toString()
        + "\n"
          "|  |  |  |  |--"
        + plan4->getRootOperators()[0]->getChildren()[0]->toString() + "\n";

    ASSERT_EQ(expectedPlan, actualPlan);
}
