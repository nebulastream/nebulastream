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
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalStreamSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace NES;
using namespace web;

class QueryPlacementTest : public testing::Test {
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

/* Test query placement with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("BottomUp",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0u];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement with top down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("TopDown",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());

    QueryPlanPtr queryPlan = query.getQueryPlan();
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    queryPlan->setQueryId(queryId);

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            std::vector<SourceLogicalOperatorNodePtr> sourceOperators = querySubPlan->getSourceOperators();
            ASSERT_EQ(sourceOperators.size(), 2u);
            for (const auto& sourceOperator : sourceOperators) {
                EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperatorNode>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("BottomUp",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

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

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2u);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2u || executionNode->getId() == 3u);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2u);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithBottomUpStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("BottomUp",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

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

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1u) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2u);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2U || executionNode->getId() == 3U);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2U);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with TopDown strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("TopDown",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

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

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlans[0]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (auto actualRootOperator : actualRootOperators) {
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<FilterLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2ULL || executionNode->getId() == 3ULL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with Top Down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithTopDownStrategy) {

    setupTopologyAndStreamCatalog();

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);
    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy("TopDown",
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              streamCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

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

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    placementStrategy->updateGlobalExecutionPlan(queryPlan);
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 2UL);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1UL);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2UL || executionNode->getId() == 3UL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            }
        }
    }
}

TEST_F(QueryPlacementTest, testManualPlacement) {
    // Setup the topology
    // We are using a linear topology of three nodes:
    // srcNode -> midNode -> sinkNode
    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 4);
    auto midNode = TopologyNode::create(1, "localhost", 4001, 5001, 4);
    auto srcNode = TopologyNode::create(2, "localhost", 4002, 5002, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewPhysicalNodeAsChild(sinkNode, midNode);
    topology->addNewPhysicalNodeAsChild(midNode, srcNode);

    ASSERT_TRUE(sinkNode->containAsChild(midNode));
    ASSERT_TRUE(midNode->containAsChild(srcNode));

    // Prepare the source and schema
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
    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, srcNode);

    streamCatalog->addPhysicalStream("car", streamCatalogEntry1);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    sinkOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    auto manualPlacementStrategy =
        Optimizer::ManualPlacementStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    // basic case: one operator per node
    std::vector<std::vector<bool>> mapping = {{true, false, false}, {false, true, false}, {false, false, true}};

    manualPlacementStrategy->setBinaryMapping(mapping);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    // Execute the placement
    manualPlacementStrategy->updateGlobalExecutionPlan(testQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString=" << globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(testQueryPlan->getQueryId());

    EXPECT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 0U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
        } else if (executionNode->getId() == 1U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
        } else if (executionNode->getId() == 1U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
        }
    }
}

TEST_F(QueryPlacementTest, testManualPlacementMultipleOperatorInANode) {
    // Setup the topology
    // We are using a linear topology of three nodes:
    // srcNode -> midNode -> sinkNode
    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 4);
    auto midNode = TopologyNode::create(1, "localhost", 4001, 5001, 4);
    auto srcNode = TopologyNode::create(2, "localhost", 4002, 5002, 4);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewPhysicalNodeAsChild(sinkNode, midNode);
    topology->addNewPhysicalNodeAsChild(midNode, srcNode);

    ASSERT_TRUE(sinkNode->containAsChild(midNode));
    ASSERT_TRUE(midNode->containAsChild(srcNode));

    // Prepare the source and schema
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
    StreamCatalogEntryPtr streamCatalogEntry1 = std::make_shared<StreamCatalogEntry>(conf, srcNode);

    streamCatalog->addPhysicalStream("car", streamCatalogEntry1);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalStreamSourceDescriptor::create("car"));

    sinkOperator->addChild(filterOperator);
    filterOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(streamCatalog);

    auto manualPlacementStrategy =
        Optimizer::ManualPlacementStrategy::create(globalExecutionPlan, topology, typeInferencePhase, streamCatalog);

    // case: allowing more than one operators per node
    std::vector<std::vector<bool>> mapping = {{true, true, false}, {false, false, false}, {false, false, true}};

    manualPlacementStrategy->setBinaryMapping(mapping);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite = Optimizer::TopologySpecificQueryRewritePhase::create(streamCatalog);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    // Execute the placement
    manualPlacementStrategy->updateGlobalExecutionPlan(testQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString=" << globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(testQueryPlan->getQueryId());

    EXPECT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 0U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperatorNode>());
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<FilterLogicalOperatorNode>());
        } else if (executionNode->getId() == 1U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());//system generated source
        } else if (executionNode->getId() == 1U) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(testQueryPlan->getQueryId());
            EXPECT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            EXPECT_EQ(actualRootOperators.size(), 1U);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());// stream source
        }
    }
}
