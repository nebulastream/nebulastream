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
        StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalog() {

        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, 4);
        topology->setAsRoot(rootNode);

        TopologyNodePtr node1 = TopologyNode::create(2, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, node1);

        TopologyNodePtr node2 = TopologyNode::create(3, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, node2);

        TopologyNodePtr node3 = TopologyNode::create(4, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, node3);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName, schema);

        SourceConfigPtr sourceConfig = SourceConfig::create();
        sourceConfig->setSourceFrequency(0);
        sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
        sourceConfig->setPhysicalStreamName("test2");
        sourceConfig->setLogicalStreamName("car");

        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, node3);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);

    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

/**
 * @brief In this test we test if all Network Sink Operators that point to a specific node location are found.
 */
TEST_F(QueryMigrationPhaseTest, testFindNetworkSinks) {
    setupTopologyAndStreamCatalog();
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
    setupTopologyAndStreamCatalog();
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
        ASSERT_TRUE(sinks1.at(i) == i + 1);
    }
    ASSERT_TRUE(sinks2.size() == 1);
    ASSERT_TRUE(sinks2.at(0) == 3);
}

TEST_F(QueryMigrationPhaseTest, testFindParentExecutionNodes) {
}
TEST_F(QueryMigrationPhaseTest, testFindChildExecutionNodes) {
}
}// nes namepsace