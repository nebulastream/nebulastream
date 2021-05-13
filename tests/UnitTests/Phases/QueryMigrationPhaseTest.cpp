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
//
// Created by balint on 19.04.21.
//
#include <Catalogs/QueryCatalog.hpp>
#include <Services/MaintenanceService.hpp>
#include <WorkQueues/NESRequestQueue.hpp>

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Nodes/Expressions/ConstantValueExpressionNode.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryMigrationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

namespace NES {

class QueryMigrationPhaseTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }
};

/**
 * @brief In this test we test if all Network Sink Operators that point to a specific node location are found.
 */
TEST_F(QueryMigrationPhaseTest, testFindNetworkSinks) {
    TopologyPtr topology = Topology::create();

    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 4);
    topology->setAsRoot(rootNode);

    TopologyNodePtr node1 = TopologyNode::create(2, "localhost", 123, 124, 4);
    topology->addNewPhysicalNodeAsChild(rootNode, node1);

    TopologyNodePtr node2 = TopologyNode::create(3, "localhost", 123, 124, 4);
    topology->addNewPhysicalNodeAsChild(rootNode, node2);

    TopologyNodePtr node3 = TopologyNode::create(4, "localhost", 123, 124, 4);
    topology->addNewPhysicalNodeAsChild(node1, node3);
    topology->addNewPhysicalNodeAsChild(node2, node3);
    GlobalExecutionPlanPtr globalExecutionPlanPtr = GlobalExecutionPlan::create();
    WorkerRPCClientPtr workerRpcClientPtr = std::make_shared<WorkerRPCClient>();

    std::vector<OperatorNodePtr> networkSinks;
    auto destinationNode = topology->findNodeWithId(2);
    Network::NesPartition nesPartition(0, 0, 0, 0);


    Network::NodeLocation nodeLocation(destinationNode->getId(), destinationNode->getIpAddress(),destinationNode->getDataPort());
    Network::NodeLocation dummy (0,"test",0);
    networkSinks.push_back(LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3),1));
    networkSinks.push_back(LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3),2));
    networkSinks.push_back(LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3),3));
    networkSinks.push_back(LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3),4));
    networkSinks.push_back(LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(dummy, nesPartition, std::chrono::seconds(5), 3),5));

    QueryPlanPtr queryPlan = QueryPlan::create(1,1,networkSinks);

    ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topology->findNodeWithId(2));
    executionNode->addNewQuerySubPlan(queryPlan->getQueryId(),queryPlan);

    QueryMigrationPhasePtr mPhase = QueryMigrationPhase::create(globalExecutionPlanPtr,topology,workerRpcClientPtr);
    auto map = mPhase->querySubPlansAndNetworkSinksToReconfigure(queryPlan->getQueryId(),executionNode,nodeLocation);
    ASSERT_TRUE(map.size() == 1);
    std::vector<OperatorId> sinks = map[queryPlan->getQuerySubPlanId()];
    ASSERT_TRUE(sinks.size() == 4);
    for(int i = 0; i<sinks.size(); i++){
        ASSERT_TRUE(sinks.at(i) == i+1);
    }
}
TEST_F(QueryMigrationPhaseTest, testFindNetworkSinksSeveralQueryPlans) {
    TopologyPtr topology = Topology::create();

    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 4);
    topology->setAsRoot(rootNode);

    TopologyNodePtr node1 = TopologyNode::create(2, "localhost", 123, 124, 4);
    topology->addNewPhysicalNodeAsChild(rootNode, node1);

    TopologyNodePtr node2 = TopologyNode::create(3, "localhost", 123, 124, 4);
    topology->addNewPhysicalNodeAsChild(rootNode, node2);

    TopologyNodePtr node3 = TopologyNode::create(4, "localhost", 123, 124, 4);
    topology->addNewPhysicalNodeAsChild(node1, node3);
    topology->addNewPhysicalNodeAsChild(node2, node3);

    GlobalExecutionPlanPtr globalExecutionPlanPtr = GlobalExecutionPlan::create();
    WorkerRPCClientPtr workerRpcClientPtr = std::make_shared<WorkerRPCClient>();

    std::vector<OperatorNodePtr> networkSinks1;
    std::vector<OperatorNodePtr> networkSinks2;
    auto destinationNode = topology->findNodeWithId(2);
    Network::NesPartition nesPartition(0, 0, 0, 0);

    Network::NodeLocation nodeLocation(destinationNode->getId(), destinationNode->getIpAddress(), destinationNode->getDataPort());
    Network::NodeLocation dummy(0, "test", 0);
    auto sink1 = LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3), 1);
    auto sink2 = LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3), 2);
    auto sink3 = LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3), 3);
    auto sink4 = LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(5), 3), 4);
    auto sink5 = LogicalOperatorFactory::createSinkOperator(
        Network::NetworkSinkDescriptor::create(dummy, nesPartition, std::chrono::seconds(5), 3), 5);

    networkSinks1.push_back(sink1);
    networkSinks1.push_back(sink2);
    networkSinks1.push_back(sink3);
    networkSinks1.push_back(sink4);
    networkSinks1.push_back(sink5);

    networkSinks2.push_back(sink3);
    networkSinks2.push_back(sink5);

    QueryPlanPtr queryPlan1 = QueryPlan::create(1, 1, networkSinks1);
    QueryPlanPtr queryPlan2 = QueryPlan::create(1, 2, networkSinks2);
    QueryPlanPtr dummyPlan = QueryPlan::create(1, 3, {sink5});

    ExecutionNodePtr executionNode = ExecutionNode::createExecutionNode(topology->findNodeWithId(2));
    executionNode->addNewQuerySubPlan(queryPlan1->getQueryId(), queryPlan1);
    executionNode->addNewQuerySubPlan(queryPlan2->getQueryId(), queryPlan2);
    executionNode->addNewQuerySubPlan(dummyPlan->getQueryId(), dummyPlan);

    QueryMigrationPhasePtr mPhase = QueryMigrationPhase::create(globalExecutionPlanPtr, topology, workerRpcClientPtr);
    auto map = mPhase->querySubPlansAndNetworkSinksToReconfigure(queryPlan1->getQueryId(), executionNode, nodeLocation);
    ASSERT_TRUE(map.size() == 2);
    std::vector<OperatorId> sinks1 = map[queryPlan1->getQuerySubPlanId()];
    std::vector<OperatorId> sinks2 = map[queryPlan2->getQuerySubPlanId()];

    ASSERT_TRUE(sinks1.size() == 4);
    for (int i = 0; i < sinks1.size(); i++) {
        EXPECT_TRUE(sinks1.at(i) == i + 1);
    }
    ASSERT_TRUE(sinks2.size() == 1);
    EXPECT_TRUE(sinks2.at(0) == 3);
}

TEST_F(QueryMigrationPhaseTest, testFindParentExecutionNodes) {


    TopologyPtr topology = Topology::create();

    TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 4);
    TopologyNodePtr node1 = TopologyNode::create(2, "localhost", 123, 124, 4);
    TopologyNodePtr node2 = TopologyNode::create(3, "localhost", 123, 124, 4);
    TopologyNodePtr node3 = TopologyNode::create(4, "localhost", 123, 124, 4);
    TopologyNodePtr node4 = TopologyNode::create(5, "localhost", 123, 124, 4);
    TopologyNodePtr node5 = TopologyNode::create(6, "localhost", 123, 124, 4);
    TopologyNodePtr node6 = TopologyNode::create(7, "localhost", 123, 124, 4);
    TopologyNodePtr node7 = TopologyNode::create(8, "localhost", 123, 124, 4);

    topology->setAsRoot(rootNode);
    topology->addNewPhysicalNodeAsChild(rootNode, node1);
    topology->addNewPhysicalNodeAsChild(rootNode, node2);
    topology->addNewPhysicalNodeAsChild(rootNode, node3);
    topology->addNewPhysicalNodeAsChild(rootNode, node4);
    topology->addNewPhysicalNodeAsChild(rootNode, node5);
    topology->addNewPhysicalNodeAsChild(rootNode, node6);

    topology->addNewPhysicalNodeAsChild(node1, node7);
    topology->addNewPhysicalNodeAsChild(node2, node7);
    topology->addNewPhysicalNodeAsChild(node3, node7);
    topology->addNewPhysicalNodeAsChild(node4, node7);
    topology->addNewPhysicalNodeAsChild(node5, node7);
    topology->addNewPhysicalNodeAsChild(node6, node7);

    ExecutionNodePtr executionNode1 = ExecutionNode::createExecutionNode(topology->findNodeWithId(2));
    ExecutionNodePtr executionNode2 = ExecutionNode::createExecutionNode(topology->findNodeWithId(4));
    ExecutionNodePtr executionNode3 = ExecutionNode::createExecutionNode(topology->findNodeWithId(6));

    QueryPlanPtr queryPlan1 = QueryPlan::create();
    QueryPlanPtr queryPlan2 = QueryPlan::create();
    QueryPlanPtr queryPlan3 = QueryPlan::create();

    queryPlan1->setQueryId(1);
    queryPlan1->setQuerySubPlanId(1);
    queryPlan2->setQueryId(1);
    queryPlan2->setQuerySubPlanId(2);
    queryPlan3->setQueryId(1);
    queryPlan3->setQuerySubPlanId(3);

    executionNode1->addNewQuerySubPlan(1,queryPlan1);
    executionNode2->addNewQuerySubPlan(1,queryPlan2);
    executionNode3->addNewQuerySubPlan(1,queryPlan3);

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    WorkerRPCClientPtr workerRpcClientPtr = std::make_shared<WorkerRPCClient>();

    QueryMigrationPhasePtr migrationPhase = QueryMigrationPhase::create(globalExecutionPlan,topology,workerRpcClientPtr);
    //Test correct behavior for 0 parent/child ExecutionNodes
    std::vector<TopologyNodePtr> parentExecutionNodes = migrationPhase->findParentExecutionNodesAsTopologyNodes(1,8);
    std::vector<TopologyNodePtr> childExecutionNodes = migrationPhase->findChildExecutionNodesAsTopologyNodes(1,1);
    EXPECT_TRUE(parentExecutionNodes.empty());
    EXPECT_TRUE(childExecutionNodes.empty());

    globalExecutionPlan->addExecutionNode(executionNode1);
    //Test correct behavior for 1 parent/child ExecutionNodes
    parentExecutionNodes = migrationPhase->findParentExecutionNodesAsTopologyNodes(1,8);
    ASSERT_TRUE(parentExecutionNodes.size() == 1);
    EXPECT_TRUE(parentExecutionNodes.at(0)->getId() == 2);
    childExecutionNodes = migrationPhase->findChildExecutionNodesAsTopologyNodes(1,1);
    ASSERT_TRUE(childExecutionNodes.size() == 1);
    EXPECT_TRUE(childExecutionNodes.at(0)->getId() == 2);


    globalExecutionPlan->addExecutionNode(executionNode2);
    globalExecutionPlan->addExecutionNode(executionNode3);

    //Test correct behavior for many parent/child ExecutionNodes
    parentExecutionNodes = migrationPhase->findParentExecutionNodesAsTopologyNodes(1,8);
    childExecutionNodes = migrationPhase->findChildExecutionNodesAsTopologyNodes(1,1);
    ASSERT_TRUE(parentExecutionNodes.size() == 3);
    ASSERT_TRUE(childExecutionNodes.size() == 3);

    for (auto id : {2,4,6}) {
        auto foundParent = std::find_if(parentExecutionNodes.begin(),parentExecutionNodes.end(),[id](TopologyNodePtr& node){
            return node->getId() == id;
        });
        auto foundChild = std::find_if(childExecutionNodes.begin(),childExecutionNodes.end(),[id](TopologyNodePtr& node){
          return node->getId() == id;
        });
        EXPECT_TRUE(foundParent != parentExecutionNodes.end());
        EXPECT_TRUE(foundChild != childExecutionNodes.end());
    }
}
}// nes namepsace