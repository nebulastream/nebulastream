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

#include "Operators/LogicalOperators/Operators/LogicalFilterOperator.hpp"
#include "Operators/LogicalOperators/Operators/LogicalInferModelOperator.hpp"
#include "Operators/LogicalOperators/Operators/LogicalMapOperator.hpp"
#include "Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp"
#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/Sinks/LogicalSinkOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/LogicalWatermarkAssignerOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacement/BasePlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/ManualPlacementStrategy.hpp>
#include <Optimizer/QueryPlacement/PlacementStrategyFactory.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class QueryPlacementTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    /* Will be called before any test in this class are executed. */

    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryPlacementTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup QueryPlacementTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_DEBUG("Setup QueryPlacementTest test case.");
        udfCatalog = Catalogs::UDF::UDFCatalog::create();
    }

    void setupTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

        topology = Topology::create();
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        TopologyNodePtr rootNode = TopologyNode::create(1, "localhost", 123, 124, resources[0], properties);
        rootNode->addNodeProperty("tf_installed", true);
        topology->setAsRoot(rootNode);

        TopologyNodePtr sourceNode1 = TopologyNode::create(2, "localhost", 123, 124, resources[1], properties);
        sourceNode1->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, sourceNode1);

        TopologyNodePtr sourceNode2 = TopologyNode::create(3, "localhost", 123, 124, resources[2], properties);
        sourceNode2->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, sourceNode2);

        auto schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        const std::string sourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

        CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(csvSourceType);

        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode1);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode2);

        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<LogicalMapOperator>()) {
                auto op = (*qPlanItr)->as<LogicalMapOperator>();
                NES_DEBUG("input schema in bytes: {}", op->getInputSchema()->getSchemaSizeInBytes());
                NES_DEBUG("output schema in bytes: {}", op->getOutputSchema()->getSchemaSizeInBytes());
                double schemaSizeComparison =
                    1.0 * op->getOutputSchema()->getSchemaSizeInBytes() / op->getInputSchema()->getSchemaSizeInBytes();

                op->addProperty("DMF", schemaSizeComparison);
            }
        }
    }
};

/* Test query placement with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<LogicalSinkOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
            }
        }
    }
}

/* Test query placement with elegant strategy  */ /*
TEST_F(QueryPlacementTest, testElegantPlacingQueryWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::ELEGANT_ENERGY);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sampleCodeGenerationPhase = Optimizer::SampleCodeGenerationPhase::create();
    queryPlan = sampleCodeGenerationPhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
}*/

/* Test query placement with top down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                EXPECT_TRUE(sourceOperator->instanceOf<LogicalSourceOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<LogicalSinkOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                    EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
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
                EXPECT_TRUE(rootOperator->instanceOf<LogicalSinkOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with bottom up strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);

    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                    EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
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
                EXPECT_TRUE(rootOperator->instanceOf<LogicalSinkOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with TopDown strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({6, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));
    auto filterOperator = LogicalOperatorFactory::createFilterOperator(Attribute("id") < 45);
    filterOperator->addChild(sourceOperator);
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    sinkOperator1->addChild(filterOperator);
    sinkOperator2->addChild(filterOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                    EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
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
                EXPECT_TRUE(rootOperator->instanceOf<LogicalSinkOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with Bottom up strategy  */
TEST_F(QueryPlacementTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({5, 4, 4});

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificReWrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(1);
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    std::shared_ptr<QueryPlan> planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(updatedSharedQMToDeploy[0]);
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::Deployed);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(2);
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    queryPlacementPhase->execute(updatedSharedQMToDeploy[0]);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(planToDeploy->getQueryId());
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(planToDeploy->getQueryId());
            ASSERT_EQ(querySubPlans.size(), 2UL);
            for (const auto& querySubPlan : querySubPlans) {
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1UL);
                auto actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = planToDeploy->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorNodePtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2ULL || executionNode->getId() == 3ULL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(planToDeploy->getQueryId());
            // map merged into querySubPlan with filter
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<LogicalSinkOperator>());
                EXPECT_TRUE(rootOperator->as<LogicalSinkOperator>()
                                ->getSinkDescriptor()
                                ->instanceOf<Network::NetworkSinkDescriptor>());
            }
            for (const auto& sourceOperator : querySubPlan->getSourceOperators()) {
                EXPECT_TRUE(sourceOperator->getParents().size() == 1);
                auto sourceParent = sourceOperator->getParents()[0];
                EXPECT_TRUE(sourceParent->instanceOf<LogicalFilterOperator>());
                auto filterParents = sourceParent->getParents();
                EXPECT_TRUE(filterParents.size() == 2);
                uint8_t distinctParents = 0;
                for (const auto& filterParent : filterParents) {
                    if (filterParent->instanceOf<LogicalMapOperator>()) {
                        EXPECT_TRUE(filterParent->getParents()[0]->instanceOf<LogicalSinkOperator>());
                        distinctParents += 1;
                    } else {
                        EXPECT_TRUE(filterParent->instanceOf<LogicalSinkOperator>());
                        distinctParents += 2;
                    }
                }
                ASSERT_EQ(distinctParents, 3);
            }
        }
    }
}

TEST_F(QueryPlacementTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({10, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificReWrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(updatedSharedQMToDeploy[0]);
    //Mark as deployed
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::Deployed);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(PlanIdGenerator::getNextQueryId());
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

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
    queryPlan3->setQueryId(PlanIdGenerator::getNextQueryId());

    queryPlan3 = queryReWritePhase->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    queryPlan3 = topologySpecificReWrite->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    z3InferencePhase->execute(queryPlan3);

    globalQueryPlan->addQueryPlan(queryPlan3);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    auto sharedQueryPlanId = updatedSharedQMToDeploy[0]->getId();

    queryPlacementPhase->execute(updatedSharedQMToDeploy[0]);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlanId);
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->getId() == 1) {
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            ASSERT_EQ(querySubPlans.size(), 1UL);
            ASSERT_EQ(querySubPlans[0]->getSinkOperators().size(), 3UL);
        } else {
            EXPECT_TRUE(executionNode->getId() == 2ULL || executionNode->getId() == 3ULL);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlanId);
            // map merged into querySubPlan with filter
            ASSERT_EQ(querySubPlans.size(), 1UL);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<LogicalSinkOperator>());
                EXPECT_TRUE(rootOperator->as<LogicalSinkOperator>()
                                ->getSinkDescriptor()
                                ->instanceOf<Network::NetworkSinkDescriptor>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with Top Down strategy  */
TEST_F(QueryPlacementTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));
    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator1 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    auto sinkOperator2 = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    sinkOperator1->addChild(sourceOperator);
    sinkOperator2->addChild(sourceOperator);
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);
    queryPlan->addRootOperator(sinkOperator1);
    queryPlan->addRootOperator(sinkOperator2);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                    EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
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
                EXPECT_TRUE(rootOperator->instanceOf<LogicalSinkOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
                }
            }
        }
    }
}

// Test manual placement
TEST_F(QueryPlacementTest, testManualPlacement) {
    setupTopologyAndSourceCatalog({4, 4, 4});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::Manual);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    NES::Optimizer::PlacementMatrix binaryMapping = {{true, false, false, false, false},
                                                     {false, true, true, false, false},
                                                     {false, false, false, true, true}};

    NES::Optimizer::BasePlacementStrategy::pinOperators(sharedQueryPlan->getQueryPlan(), topology, binaryMapping);
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<LogicalSinkOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
            }
        }
    }
}

// Test manual placement with limited resources. The manual placement should place the operator depending on the mapping
// without considering availability of the topology nodes
TEST_F(QueryPlacementTest, testManualPlacementLimitedResources) {
    setupTopologyAndSourceCatalog({1, 1, 1});// each node only has a capacity of 1
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::Manual);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    NES::Optimizer::PlacementMatrix binaryMapping = {{true, false, false, false, false},
                                                     {false, true, true, false, false},
                                                     {false, false, false, true, true}};

    NES::Optimizer::ManualPlacementStrategy::pinOperators(sharedQueryPlan->getQueryPlan(), topology, binaryMapping);

    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<LogicalSinkOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
            }
        }
    }
}

// Test manual placement to place expanded operators in the same topology node
TEST_F(QueryPlacementTest, testManualPlacementExpandedOperatorInASingleNode) {
    setupTopologyAndSourceCatalog({1, 1, 1});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::Manual);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getId();

    NES::Optimizer::PlacementMatrix binaryMapping = {{true, true, false, true, false},
                                                     {false, false, true, false, false},
                                                     {false, false, false, false, true}};

    NES::Optimizer::ManualPlacementStrategy::pinOperators(sharedQueryPlan->getQueryPlan(), topology, binaryMapping);

    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
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
                EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
            std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
            ASSERT_EQ(querySubPlans.size(), 1u);
            auto querySubPlan = querySubPlans[0];
            std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorNodePtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<LogicalSinkOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<LogicalSourceOperator>());
            }
        }
    }
}

/**
 * Test on a linear topology with one logical source
 * Topology: sinkNode--midNode---srcNode
 * Query: SinkOp---MapOp---SourceOp
 */
//TODO: enable this test after fixing #2486
TEST_F(QueryPlacementTest, DISABLED_testIFCOPPlacement) {
    // Setup the topology
    // We are using a linear topology of three nodes:
    // srcNode -> midNode -> sinkNode
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 4, properties);
    auto midNode = TopologyNode::create(1, "localhost", 4001, 5001, 4, properties);
    auto srcNode = TopologyNode::create(2, "localhost", 4002, 5002, 4, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode);
    topology->addNewTopologyNodeAsChild(midNode, srcNode);

    ASSERT_TRUE(sinkNode->containAsChild(midNode));
    ASSERT_TRUE(midNode->containAsChild(srcNode));

    // Prepare the source and schema
    auto schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("value2") = Attribute("value") * 2);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::IFCOP);
    testQueryPlan->addRootOperator(sinkOperator);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy(Optimizer::PlacementStrategy::IFCOP,
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              coordinatorConfiguration);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto queryId = sharedQueryPlan->getId();
    // Execute the placement
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    // check if map is placed two times
    uint32_t mapPlacementCount = 0;

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(testQueryPlan->getQueryId())) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<LogicalSinkOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<LogicalMapOperator>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                        if (childrenOfMapOp->as<LogicalSourceOperator>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<LogicalSourceOperator>());
                    // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                    if (child->as<LogicalSourceOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                    }
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_EQ(mapPlacementCount, 1U);
}

/**
 * Test on a branched topology with one logical source
 * Topology: sinkNode--mid1--srcNode1
 *                   \
 *                    --mid2--srcNode2
 * Query: SinkOp---MapOp---SourceOp
 */
//TODO: enable this test after fixing #2486
TEST_F(QueryPlacementTest, DISABLED_testIFCOPPlacementOnBranchedTopology) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 4, properties);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 4, properties);
    auto midNode2 = TopologyNode::create(2, "localhost", 4002, 5002, 4, properties);
    auto srcNode1 = TopologyNode::create(3, "localhost", 4003, 5003, 4, properties);
    auto srcNode2 = TopologyNode::create(4, "localhost", 4004, 5004, 4, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(sinkNode, midNode2);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);
    topology->addNewTopologyNodeAsChild(midNode2, srcNode2);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(sinkNode->containAsChild(midNode2));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));
    ASSERT_TRUE(midNode2->containAsChild(srcNode2));

    NES_DEBUG("QueryPlacementTest:: topology: {}", topology->toString());

    // Prepare the source and schema
    auto schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry2 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode2);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);

    // Prepare the query
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(PrintSinkDescriptor::create());
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("value2") = Attribute("value") * 2);
    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(sourceOperator);

    QueryPlanPtr testQueryPlan = QueryPlan::create();
    testQueryPlan->addRootOperator(sinkOperator);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto placementStrategy = Optimizer::PlacementStrategyFactory::getStrategy(Optimizer::PlacementStrategy::IFCOP,
                                                                              globalExecutionPlan,
                                                                              topology,
                                                                              typeInferencePhase,
                                                                              coordinatorConfiguration);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    placementStrategy->updateGlobalExecutionPlan(testQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString={}", globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(testQueryPlan->getQueryId());

    EXPECT_EQ(executionNodes.size(), 5UL);
    // check if map is placed two times
    uint32_t mapPlacementCount = 0;

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(testQueryPlan->getQueryId())) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<LogicalSinkOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<LogicalMapOperator>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                        if (childrenOfMapOp->as<LogicalSourceOperator>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid =
                                executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                        } else if (childrenOfMapOp->as<LogicalSourceOperator>()->getId()
                                   == testQueryPlan->getSourceOperators()[1]->getId()) {
                            isSource2PlacementValid =
                                executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<LogicalSourceOperator>());
                    // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                    if (child->as<LogicalSourceOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid =
                            executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                    } else if (child->as<LogicalSourceOperator>()->getId()
                               == testQueryPlan->getSourceOperators()[1]->getId()) {
                        isSource2PlacementValid =
                            executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                    }
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_TRUE(isSource2PlacementValid);
    EXPECT_EQ(mapPlacementCount, 2U);
}

/**
 * Test placement of self join query on a topology with one logical source
 * Topology: sinkNode--mid1--srcNode1(A)
 *
 * Query: SinkOp---join---SourceOp(A)
 *                    \
 *                     -----SourceOp(A)
 *
 *
 *
 */
TEST_F(QueryPlacementTest, testTopDownPlacementOfSelfJoinQuery) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 14, properties);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 4, properties);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 4, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));

    NES_DEBUG("QueryPlacementTest:: topology: {}", topology->toString());

    // Prepare the source and schema
    auto schema = Schema::create()
                      ->addField("id", BasicType::UINT32)
                      ->addField("value", BasicType::UINT64)
                      ->addField("timestamp", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car")
                      .as("c1")
                      .joinWith(Query::from("car").as("c2"))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->enableQueryReconfiguration = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto queryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString={}", globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    EXPECT_EQ(executionNodes.size(), 3UL);

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(queryId)) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<LogicalSinkOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            auto sourceOperators = querySubPlan->getSourceOperators();

            for (const auto& sourceOperator : sourceOperators) {
                if (sourceOperator->as<LogicalSourceOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                } else if (sourceOperator->as<LogicalSourceOperator>()->getId()
                           == testQueryPlan->getSourceOperators()[1]->getId()) {
                    isSource2PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_TRUE(isSource2PlacementValid);
}

/**
 * Test placement of self join query on a topology with one logical source
 * Topology: sinkNode--mid1--srcNode1(A)
 *
 * Query: SinkOp---join---SourceOp(A)
 *                    \
 *                     -----SourceOp(A)
 *
 *
 *
 */
TEST_F(QueryPlacementTest, testBottomUpPlacementOfSelfJoinQuery) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 14, properties);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 4, properties);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 4, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));

    NES_DEBUG("QueryPlacementTest:: topology: {}", topology->toString());

    // Prepare the source and schema
    auto schema = Schema::create()
                      ->addField("id", BasicType::UINT32)
                      ->addField("value", BasicType::UINT64)
                      ->addField("timestamp", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car")
                      .as("c1")
                      .joinWith(Query::from("car").as("c2"))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {
            OperatorNodePtr root = querySubPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<LogicalSinkOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            auto sourceOperators = querySubPlan->getSourceOperators();

            for (const auto& sourceOperator : sourceOperators) {
                if (sourceOperator->as<LogicalSourceOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                } else if (sourceOperator->as<LogicalSourceOperator>()->getId()
                           == testQueryPlan->getSourceOperators()[1]->getId()) {
                    isSource2PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                }
            }
        }
    }

    EXPECT_TRUE(isSinkPlacementValid);
    EXPECT_TRUE(isSource1PlacementValid);
    EXPECT_TRUE(isSource2PlacementValid);
}

/**
 * Test if TopDownPlacement respects resources constrains
 * Topology: sinkNode(1)--mid1(1)--srcNode1(A)(2)
 *
 * Query: SinkOp--filter()--source(A)
 *
 *
 *
 */
TEST_F(QueryPlacementTest, testTopDownPlacementWthThightResourcesConstrains) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 1, properties);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 1, properties);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 2, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));

    NES_DEBUG("QueryPlacementTest:: topology: {}", topology->toString());

    // Prepare the source and schema
    auto schema = Schema::create()
                      ->addField("id", BasicType::UINT32)
                      ->addField("value", BasicType::UINT64)
                      ->addField("timestamp", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSourceCar = PhysicalSource::create(csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNode1);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {
            auto ops = querySubPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->getId() == 0) {
                ASSERT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<LogicalSourceOperator>());
            } else if (executionNode->getId() == 1) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<LogicalSinkOperator>());
                auto filter = sink->getChildren()[0];
                ASSERT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                ASSERT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
            } else if (executionNode->getId() == 2) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<LogicalSinkOperator>());
                auto source = sink->getChildren()[0];
                ASSERT_TRUE(source->instanceOf<LogicalSourceOperator>());
                ASSERT_EQ(source->as<LogicalSourceOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]
                              ->getChildren()[0]
                              ->getChildren()[0]
                              ->as<LogicalSourceOperator>()
                              ->getId());
            }
            NES_INFO("Sub Plan: {}", querySubPlan->toString());
        }
    }
}
/**
 * Test if BottomUp placement respects resources constrains
 * Topology: sinkNode(1)--mid1(1)--srcNode1(A)(1)
 *
 * Query: SinkOp--filter()--source(A)
 *
 *
 *
 */
TEST_F(QueryPlacementTest, testBottomUpPlacementWthThightResourcesConstrains) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 1, properties);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 1, properties);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 1, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));

    NES_DEBUG("QueryPlacementTest:: topology: {}", topology->toString());

    // Prepare the source and schema
    auto schema = Schema::create()
                      ->addField("id", BasicType::UINT32)
                      ->addField("value", BasicType::UINT64)
                      ->addField("timestamp", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);

    auto physicalSourceCar = PhysicalSource::create(csvSourceType);
    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSourceCar, logicalSource, srcNode1);

    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {
            auto ops = querySubPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->getId() == 0) {
                ASSERT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<LogicalSourceOperator>());
            } else if (executionNode->getId() == 1) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<LogicalSinkOperator>());
                auto filter = sink->getChildren()[0];
                ASSERT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                ASSERT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
            } else if (executionNode->getId() == 2) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<LogicalSinkOperator>());
                auto source = sink->getChildren()[0];
                ASSERT_TRUE(source->instanceOf<LogicalSourceOperator>());
                ASSERT_EQ(source->as<LogicalSourceOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]
                              ->getChildren()[0]
                              ->getChildren()[0]
                              ->as<LogicalSourceOperator>()
                              ->getId());
            }
            NES_INFO("Sub Plan: {}", querySubPlan->toString());
        }
    }
}

/**
 * Test if BottomUp placement respects resources constrains with BinaryOperators
 * Topology: sinkNode(1)--mid1(5)--srcNode1(A)(1)
 *                             \ --srcNode2(B)(1)
 *
 * Query: SinkOp--join()--source(A)
 *                     \--source(B)
 *
 *
 */
TEST_F(QueryPlacementTest, testBottomUpPlacementWthThightResourcesConstrainsInAJoin) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    auto sinkNode = TopologyNode::create(0, "localhost", 4000, 5000, 1, properties);
    auto midNode1 = TopologyNode::create(1, "localhost", 4001, 5001, 5, properties);
    auto srcNode1 = TopologyNode::create(2, "localhost", 4003, 5003, 1, properties);
    auto srcNode2 = TopologyNode::create(3, "localhost", 4003, 5004, 1, properties);

    TopologyPtr topology = Topology::create();
    topology->setAsRoot(sinkNode);

    topology->addNewTopologyNodeAsChild(sinkNode, midNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode1);
    topology->addNewTopologyNodeAsChild(midNode1, srcNode2);

    ASSERT_TRUE(sinkNode->containAsChild(midNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode1));
    ASSERT_TRUE(midNode1->containAsChild(srcNode2));

    NES_INFO("Topology:\n{}", topology->toString());

    NES_DEBUG("QueryPlacementTest:: topology: {}", topology->toString());

    // Prepare the source and schema
    auto schema = Schema::create()
                      ->addField("id", BasicType::UINT32)
                      ->addField("value", BasicType::UINT64)
                      ->addField("timestamp", BasicType::UINT64);

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    {
        const std::string sourceName = "car1";
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode1);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }
    {
        const std::string sourceName = "car2";
        sourceCatalog->addLogicalSource(sourceName, schema);
        auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
        CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
        csvSourceType->setGatheringInterval(0);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSource = PhysicalSource::create(csvSourceType);
        Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry1 =
            std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, srcNode2);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);
    }

    Query query = Query::from("car1")
                      .joinWith(Query::from("car2"))
                      .where(Attribute("id"))
                      .equalsTo(Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

    auto sink = testQueryPlan->getSinkOperators()[0]->as<LogicalSinkOperator>();
    auto join = sink->getChildren()[0]->as<LogicalJoinOperator>();
    auto watermark1 = join->getChildren()[0]->as<LogicalWatermarkAssignerOperator>();
    auto source1 = watermark1->getChildren()[0]->as<LogicalSourceOperator>();
    auto watermark2 = join->getChildren()[1]->as<LogicalWatermarkAssignerOperator>();
    auto source2 = watermark2->getChildren()[0]->as<LogicalSourceOperator>();

    EXPECT_EQ(executionNodes.size(), 4UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {
            auto ops = querySubPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->getId() == 0) {
                ASSERT_EQ(ops[0]->getId(), sink->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<LogicalSourceOperator>());
            } else if (executionNode->getId() == 1) {
                auto placedSink = ops[0];
                ASSERT_TRUE(sink->instanceOf<LogicalSinkOperator>());
                auto placedJoin = placedSink->getChildren()[0];
                ASSERT_TRUE(placedJoin->instanceOf<LogicalJoinOperator>());
                ASSERT_EQ(placedJoin->as<LogicalJoinOperator>()->getId(), join->getId());
                ASSERT_EQ(placedJoin->getChildren().size(), 2);
                auto placedWatermark1 = placedJoin->getChildren()[0];
                ASSERT_TRUE(placedWatermark1->instanceOf<LogicalWatermarkAssignerOperator>());
                ASSERT_EQ(placedWatermark1->as<LogicalWatermarkAssignerOperator>()->getId(), watermark1->getId());

                auto placedWatermark2 = placedJoin->getChildren()[1];
                ASSERT_TRUE(placedWatermark2->instanceOf<LogicalWatermarkAssignerOperator>());
                ASSERT_EQ(placedWatermark2->as<LogicalWatermarkAssignerOperator>()->getId(), watermark2->getId());
            } else if (executionNode->getId() == 2) {
                auto placedSink = ops[0];
                ASSERT_TRUE(placedSink->instanceOf<LogicalSinkOperator>());
                auto placedSource = placedSink->getChildren()[0];
                ASSERT_TRUE(placedSource->instanceOf<LogicalSourceOperator>());
                ASSERT_EQ(placedSource->as<LogicalSourceOperator>()->getId(), source1->getId());
            } else if (executionNode->getId() == 3) {
                auto placedSink = ops[0];
                ASSERT_TRUE(placedSink->instanceOf<LogicalSinkOperator>());
                auto placedSource = placedSink->getChildren()[0];
                ASSERT_TRUE(placedSource->instanceOf<LogicalSourceOperator>());
                ASSERT_EQ(placedSource->as<LogicalSourceOperator>()->getId(), source2->getId());
            }
            NES_INFO("Sub Plan: {}", querySubPlan->toString());
        }
    }
}