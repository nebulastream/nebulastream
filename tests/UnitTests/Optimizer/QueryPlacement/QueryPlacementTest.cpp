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
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Phases/QueryRewritePhase.hpp>
#include <Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gtest/gtest.h>

using namespace NES;
using namespace web;

class QueryPlacementTest : public testing::Test {
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

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, 4);
        topology->addNewPhysicalNodeAsChild(rootNode, sourceNode2);

        std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                             "->addField(\"value\", BasicType::UINT64);";
        const std::string streamName = "car";

        streamCatalog = std::make_shared<StreamCatalog>();
        streamCatalog->addLogicalStream(streamName, schema);

        PhysicalStreamConfigPtr conf =
            PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "1",
                                         /**Source Frequence**/ 0, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                         /**Number of Buffers To Produce**/ 1, /**Physical Stream Name**/ "test2",
                                         /**Logical Stream Name**/ "car");

        StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, sourceNode1);
        StreamCatalogEntryPtr streamCatalogEntry2 = std::make_shared<StreamCatalogEntry>(conf, sourceNode2);

        streamCatalog->addPhysicalStream("car", streamCatalogEntry1);
        streamCatalog->addPhysicalStream("car", streamCatalogEntry2);
    }

    StreamCatalogPtr streamCatalog;
    TopologyPtr topology;
};

/* Test query placement with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy =
        PlacementStrategyFactory::getStrategy("BottomUp", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement with top down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy =
        PlacementStrategyFactory::getStrategy("TopDown", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            std::vector<SourceLogicalOperatorNodePtr> sourceOperators = querySubPlan->getSourceOperators();
            ASSERT_EQ(sourceOperators.size(), 2);
            for (auto sourceOperator : sourceOperators) {
                ASSERT_TRUE(sourceOperator->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (auto children : actualRootOperator->getChildren()) {
                ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy =
        PlacementStrategyFactory::getStrategy("BottomUp", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2);
            for (auto querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(), expectedRootOperators.end(),
                                          [&](OperatorNodePtr expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                ASSERT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
                for (auto children : actualRootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2);
            for (auto rootOperator : actualRootOperators) {
                ASSERT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (auto children : rootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy =
        PlacementStrategyFactory::getStrategy("BottomUp", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2);
            for (auto querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(), expectedRootOperators.end(),
                                          [&](OperatorNodePtr expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                ASSERT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
                for (auto children : actualRootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2);
            for (auto rootOperator : actualRootOperators) {
                ASSERT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (auto children : rootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with TopDown strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy =
        PlacementStrategyFactory::getStrategy("TopDown", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2);
            for (auto actualRootOperator : actualRootOperators) {
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(), expectedRootOperators.end(),
                                          [&](OperatorNodePtr expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                ASSERT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
                for (auto children : actualRootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
                }
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1);
            for (auto rootOperator : actualRootOperators) {
                ASSERT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (auto children : rootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with Top Down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    TypeInferencePhasePtr typeInferencePhase = TypeInferencePhase::create(streamCatalog);
    auto placementStrategy =
        PlacementStrategyFactory::getStrategy("TopDown", globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car", 1));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    QueryRewritePhasePtr queryReWritePhase = QueryRewritePhase::create(streamCatalog);
    queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3);
    for (auto executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2);
            for (auto querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(), expectedRootOperators.end(),
                                          [&](OperatorNodePtr expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                ASSERT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2);
                for (auto children : actualRootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            ASSERT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2);
            for (auto rootOperator : actualRootOperators) {
                ASSERT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (auto children : rootOperator->getChildren()) {
                    ASSERT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}
