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

#include <API/Query.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/StreamCatalogEntry.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryReconfigurationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace web;

class QueryReconfigurationTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup QueryPlacementTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::setupLogging("QueryPlacementTest.log", NES::LOG_DEBUG);
        std::cout << "Setup QueryPlacementTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup QueryPlacementTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down QueryPlacementTest test class." << std::endl; }

    void setupTopologyAndStreamCatalog(std::vector<uint16_t> resources = {4, 4, 4, 4}) {

        topology = Topology::create();

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, resources[0]);
        topology->setAsRoot(rootNode);

        TopologyNodePtr worker1 = TopologyNode::create(2, "localhost", 123, 124, resources[1]);
        topology->addNewPhysicalNodeAsChild(rootNode, worker1);

        TopologyNodePtr sourceNode1 = TopologyNode::create(3, "localhost", 123, 124, resources[2]);
        topology->addNewPhysicalNodeAsChild(worker1, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(4, "localhost", 123, 124, resources[3]);
        topology->addNewPhysicalNodeAsChild(worker1, sourceNode2);

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

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode1);
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf, sourceNode2);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

/* Test query placement of query with multiple sinks with TopDown strategy  */
TEST_F(QueryReconfigurationTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog({10, 10, 1, 1});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("BottomUp",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto workerRpcClient = std::make_shared<WorkerRPCClient>();
    auto reconfigurationPhase = QueryReconfigurationPhase::create(globalExecutionPlan, workerRpcClient);

    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(1);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    std::shared_ptr<QueryPlan> planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    placementStrategy->updateGlobalExecutionPlan(planToDeploy);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(2);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    planToDeploy->setQueryId(1);

    placementStrategy->partiallyUpdateGlobalExecutionPlan(planToDeploy);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(planToDeploy->getQueryId());

    reconfigurationPhase->execute(updatedSharedQMToDeploy[0]);
}

TEST_F(QueryReconfigurationTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndStreamCatalog({10, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("TopDown",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(1);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    std::shared_ptr<QueryPlan> planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    planToDeploy->setQueryId(queryPlan1->getQueryId());

    placementStrategy->updateGlobalExecutionPlan(planToDeploy);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(2);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    // new query
    auto queryPlan3 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .map(Attribute("newNewId") = 4)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan3->setQueryId(3);

    queryPlan3 = queryReWritePhase->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    queryPlan3 = topologySpecificReWrite->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    z3InferencePhase->execute(queryPlan3);

    globalQueryPlan->addQueryPlan(queryPlan3);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    planToDeploy->setQueryId(queryPlan1->getQueryId());

    auto mergedPlacementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("TopDown",
                                                                                    globalExecutionPlan,
                                                                                    topology,
                                                                                    typeInferencePhase,
                                                                                    streamCatalog);

    mergedPlacementStrategy->partiallyUpdateGlobalExecutionPlan(planToDeploy);
}