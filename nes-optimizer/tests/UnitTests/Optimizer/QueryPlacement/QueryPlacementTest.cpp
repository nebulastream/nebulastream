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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Source/SourceCatalogEntry.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Enums/PlacementAmendmentMode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/FilterLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/MapLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Windows/Joins/JoinLogicalOperatorNode.hpp>
#include <Optimizer/Exceptions/QueryPlacementException.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
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
        properties["tf_installed"] = true;

        int rootNodeId = 1;
        topology->registerTopologyNode(rootNodeId, "localhost", 123, 124, resources[0], properties);
        topology->setRootTopologyNodeId(rootNodeId);

        int sourceNode1Id = 2;
        topology->registerTopologyNode(sourceNode1Id, "localhost", 123, 124, resources[1], properties);
        topology->addTopologyNodeAsChild(rootNodeId, sourceNode1Id);

        int sourceNode2Id = 3;
        topology->registerTopologyNode(sourceNode2Id, "localhost", 123, 124, resources[2], properties);
        topology->addTopologyNodeAsChild(rootNodeId, sourceNode2Id);

        auto carSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        const std::string carSourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(carSourceName, carSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(carSourceName);

        CSVSourceTypePtr csvSourceTypeForCar = CSVSourceType::create(carSourceName, "carPhysicalSourceName");
        csvSourceTypeForCar->setGatheringInterval(0);
        csvSourceTypeForCar->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSourceForCar = PhysicalSource::create(csvSourceTypeForCar);

        auto sourceCatalogEntry1 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceForCar, logicalSource, sourceNode1Id);
        auto sourceCatalogEntry2 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceForCar, logicalSource, sourceNode2Id);

        sourceCatalog->addPhysicalSource(carSourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(carSourceName, sourceCatalogEntry2);

        globalExecutionPlan = GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        QueryPlanIterator queryPlanIterator = QueryPlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != QueryPlanIterator::end(); ++qPlanItr) {
            // set data modification factor for map operator
            if ((*qPlanItr)->instanceOf<MapLogicalOperatorNode>()) {
                auto op = (*qPlanItr)->as<MapLogicalOperatorNode>();
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
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::DEPLOYED);

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
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                EXPECT_TRUE(rootOperator->as<SinkLogicalOperatorNode>()
                                ->getSinkDescriptor()
                                ->instanceOf<Network::NetworkSinkDescriptor>());
            }
            for (const auto& sourceOperator : querySubPlan->getSourceOperators()) {
                EXPECT_TRUE(sourceOperator->getParents().size() == 1);
                auto sourceParent = sourceOperator->getParents()[0];
                EXPECT_TRUE(sourceParent->instanceOf<FilterLogicalOperatorNode>());
                auto filterParents = sourceParent->getParents();
                EXPECT_TRUE(filterParents.size() == 2);
                uint8_t distinctParents = 0;
                for (const auto& filterParent : filterParents) {
                    if (filterParent->instanceOf<MapLogicalOperatorNode>()) {
                        EXPECT_TRUE(filterParent->getParents()[0]->instanceOf<SinkLogicalOperatorNode>());
                        distinctParents += 1;
                    } else {
                        EXPECT_TRUE(filterParent->instanceOf<SinkLogicalOperatorNode>());
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
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::DEPLOYED);

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
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperatorNode>());
                EXPECT_TRUE(rootOperator->as<SinkLogicalOperatorNode>()
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

    TopologyPtr topology = Topology::create();

    int rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 4, properties);
    topology->setRootTopologyNodeId(rootNodeId);

    int middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 4, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);

    int sourceNodeId = 2;
    topology->registerTopologyNode(sourceNodeId, "localhost", 4002, 5002, 4, properties);
    topology->addTopologyNodeAsChild(middleNodeId, sourceNodeId);

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
    auto sourceCatalogEntry1 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId);
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
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<MapLogicalOperatorNode>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                        if (childrenOfMapOp->as<SourceLogicalOperatorNode>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<SourceLogicalOperatorNode>());
                    // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                    if (child->as<SourceLogicalOperatorNode>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
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

    TopologyPtr topology = Topology::create();

    int rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 4, properties);
    topology->setRootTopologyNodeId(rootNodeId);

    int middleNodeId1 = 1;
    topology->registerTopologyNode(middleNodeId1, "localhost", 4001, 5001, 4, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId1);

    int middleNodeId2 = 2;
    topology->registerTopologyNode(middleNodeId2, "localhost", 4001, 5001, 4, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId2);

    int sourceNodeId1 = 3;
    topology->registerTopologyNode(sourceNodeId1, "localhost", 4002, 5002, 4, properties);
    topology->addTopologyNodeAsChild(middleNodeId1, sourceNodeId1);

    int sourceNodeId2 = 4;
    topology->registerTopologyNode(sourceNodeId2, "localhost", 4002, 5002, 4, properties);
    topology->addTopologyNodeAsChild(middleNodeId2, sourceNodeId2);

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
    auto sourceCatalogEntry1 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId1);
    auto sourceCatalogEntry2 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId2);
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

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    const auto& sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);

    // Execute the placement phase
    auto queryPlacementPhase =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    queryPlacementPhase->execute(sharedQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString={}", globalExecutionPlan->getAsString());

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlan->getId());

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
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<MapLogicalOperatorNode>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                        if (childrenOfMapOp->as<SourceLogicalOperatorNode>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid =
                                executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                        } else if (childrenOfMapOp->as<SourceLogicalOperatorNode>()->getId()
                                   == testQueryPlan->getSourceOperators()[1]->getId()) {
                            isSource2PlacementValid =
                                executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<SourceLogicalOperatorNode>());
                    // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                    if (child->as<SourceLogicalOperatorNode>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid =
                            executionNode->getTopologyNode()->getId() == 3 || executionNode->getTopologyNode()->getId() == 4;
                    } else if (child->as<SourceLogicalOperatorNode>()->getId()
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

    TopologyPtr topology = Topology::create();

    WorkerId rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 14, properties);
    topology->setRootTopologyNodeId(rootNodeId);
    WorkerId middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 4, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);
    WorkerId srcNodeId = 2;
    topology->registerTopologyNode(srcNodeId, "localhost", 4003, 5003, 4, properties);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId);
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
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            auto sourceOperators = querySubPlan->getSourceOperators();

            for (const auto& sourceOperator : sourceOperators) {
                if (sourceOperator->as<SourceLogicalOperatorNode>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                } else if (sourceOperator->as<SourceLogicalOperatorNode>()->getId()
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

    TopologyPtr topology = Topology::create();

    WorkerId rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 14, properties);
    topology->setRootTopologyNodeId(rootNodeId);
    WorkerId middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 4, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);
    WorkerId srcNodeId = 2;
    topology->registerTopologyNode(srcNodeId, "localhost", 4003, 5003, 4, properties);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId);
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
            if (root->as<SinkLogicalOperatorNode>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->getTopologyNode()->getId() == 0;
            }

            auto sourceOperators = querySubPlan->getSourceOperators();

            for (const auto& sourceOperator : sourceOperators) {
                if (sourceOperator->as<SourceLogicalOperatorNode>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->getTopologyNode()->getId() == 2;
                } else if (sourceOperator->as<SourceLogicalOperatorNode>()->getId()
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
 *
 * Test if TopDownPlacement respects resources constrains
 * Topology: sinkNode(1)--mid1(1)--srcNode1(A)(2)
 *
 * Query: SinkOp--filter()--source(A)
 *
 */
TEST_F(QueryPlacementTest, testTopDownPlacementWthTightResourcesConstrains) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    WorkerId rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 1, properties);
    topology->setRootTopologyNodeId(rootNodeId);
    WorkerId middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 1, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);
    WorkerId srcNodeId = 2;
    topology->registerTopologyNode(srcNodeId, "localhost", 4003, 5003, 2, properties);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);
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
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            } else if (executionNode->getId() == 1) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperatorNode>());
                auto filter = sink->getChildren()[0];
                ASSERT_TRUE(filter->instanceOf<FilterLogicalOperatorNode>());
                ASSERT_EQ(filter->as<FilterLogicalOperatorNode>()->getId(),
                          testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<FilterLogicalOperatorNode>()->getId());
            } else if (executionNode->getId() == 2) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperatorNode>());
                auto source = sink->getChildren()[0];
                ASSERT_TRUE(source->instanceOf<SourceLogicalOperatorNode>());
                ASSERT_EQ(source->as<SourceLogicalOperatorNode>()->getId(),
                          testQueryPlan->getRootOperators()[0]
                              ->getChildren()[0]
                              ->getChildren()[0]
                              ->as<SourceLogicalOperatorNode>()
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
TEST_F(QueryPlacementTest, testBottomUpPlacementWthTightResourcesConstrains) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    WorkerId rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 1, properties);
    topology->setRootTopologyNodeId(rootNodeId);
    WorkerId middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 1, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);
    WorkerId srcNodeId = 2;
    topology->registerTopologyNode(srcNodeId, "localhost", 4003, 5003, 1, properties);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 =
        Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);

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
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            } else if (executionNode->getId() == 1) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperatorNode>());
                auto filter = sink->getChildren()[0];
                ASSERT_TRUE(filter->instanceOf<FilterLogicalOperatorNode>());
                ASSERT_EQ(filter->as<FilterLogicalOperatorNode>()->getId(),
                          testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<FilterLogicalOperatorNode>()->getId());
            } else if (executionNode->getId() == 2) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperatorNode>());
                auto source = sink->getChildren()[0];
                ASSERT_TRUE(source->instanceOf<SourceLogicalOperatorNode>());
                ASSERT_EQ(source->as<SourceLogicalOperatorNode>()->getId(),
                          testQueryPlan->getRootOperators()[0]
                              ->getChildren()[0]
                              ->getChildren()[0]
                              ->as<SourceLogicalOperatorNode>()
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
TEST_F(QueryPlacementTest, testBottomUpPlacementWthTightResourcesConstrainsInAJoin) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    WorkerId rootNodeId = 0;
    topology->registerTopologyNode(rootNodeId, "localhost", 4000, 5000, 1, properties);
    topology->setRootTopologyNodeId(rootNodeId);
    WorkerId middleNodeId = 1;
    topology->registerTopologyNode(middleNodeId, "localhost", 4001, 5001, 5, properties);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);
    WorkerId srcNodeId1 = 2;
    topology->registerTopologyNode(srcNodeId1, "localhost", 4003, 5003, 1, properties);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId1);
    WorkerId srcNodeId2 = 3;
    topology->registerTopologyNode(srcNodeId2, "localhost", 4003, 5003, 1, properties);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId2);

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
        auto sourceCatalogEntry1 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId1);
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
        auto sourceCatalogEntry2 =
           Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId2);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
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

    auto sink = testQueryPlan->getSinkOperators()[0]->as<SinkLogicalOperatorNode>();
    auto join = sink->getChildren()[0]->as<JoinLogicalOperatorNode>();
    auto watermark1 = join->getChildren()[0]->as<WatermarkAssignerLogicalOperatorNode>();
    auto source1 = watermark1->getChildren()[0]->as<SourceLogicalOperatorNode>();
    auto watermark2 = join->getChildren()[1]->as<WatermarkAssignerLogicalOperatorNode>();
    auto source2 = watermark2->getChildren()[0]->as<SourceLogicalOperatorNode>();

    EXPECT_EQ(executionNodes.size(), 4UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        for (const auto& querySubPlan : executionNode->getQuerySubPlans(sharedQueryId)) {
            auto ops = querySubPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->getId() == 0) {
                ASSERT_EQ(ops[0]->getId(), sink->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperatorNode>());
            } else if (executionNode->getId() == 1) {
                auto placedSink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperatorNode>());
                auto placedJoin = placedSink->getChildren()[0];
                ASSERT_TRUE(placedJoin->instanceOf<JoinLogicalOperatorNode>());
                ASSERT_EQ(placedJoin->as<JoinLogicalOperatorNode>()->getId(), join->getId());
                ASSERT_EQ(placedJoin->getChildren().size(), 2);
                auto placedWatermark1 = placedJoin->getChildren()[0];
                ASSERT_TRUE(placedWatermark1->instanceOf<WatermarkAssignerLogicalOperatorNode>());
                ASSERT_EQ(placedWatermark1->as<WatermarkAssignerLogicalOperatorNode>()->getId(), watermark1->getId());

                auto placedWatermark2 = placedJoin->getChildren()[1];
                ASSERT_TRUE(placedWatermark2->instanceOf<WatermarkAssignerLogicalOperatorNode>());
                ASSERT_EQ(placedWatermark2->as<WatermarkAssignerLogicalOperatorNode>()->getId(), watermark2->getId());
            } else if (executionNode->getId() == 2) {
                auto placedSink = ops[0];
                ASSERT_TRUE(placedSink->instanceOf<SinkLogicalOperatorNode>());
                auto placedSource = placedSink->getChildren()[0];
                ASSERT_TRUE(placedSource->instanceOf<SourceLogicalOperatorNode>());
                ASSERT_EQ(placedSource->as<SourceLogicalOperatorNode>()->getId(), source1->getId());
            } else if (executionNode->getId() == 3) {
                auto placedSink = ops[0];
                ASSERT_TRUE(placedSink->instanceOf<SinkLogicalOperatorNode>());
                auto placedSource = placedSink->getChildren()[0];
                ASSERT_TRUE(placedSource->instanceOf<SourceLogicalOperatorNode>());
                ASSERT_EQ(placedSource->as<SourceLogicalOperatorNode>()->getId(), source2->getId());
            }
            NES_INFO("Sub Plan: {}", querySubPlan->toString());
        }
    }
}

/*
 * Test query placement using BottomUp strategy for two queries:
 *
 * Q1: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 * Q2: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 *
 * On the following topology:
 *
 * Topology: rootNode(6)---srcNode1(4)
 *                     \
 *                      ---srcNode2 (4)
 *
 *  We perform both placements concurrently using the pessimistic approach.
 *  The Expectations are that both placements should be successful and should result in a consistent global execution plan.
 *
 */
TEST_F(QueryPlacementTest, testConcurrentOperatorPlacementUsingPessimisticBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());

    // Setup Queries
    std::vector<QueryPlanPtr> queryPlans;
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    auto numOfQueries = 2;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
        QueryPlanPtr queryPlan = query.getQueryPlan();

        queryPlan = queryReWritePhase->execute(queryPlan);
        queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
        typeInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<bool>> placementResults;
    for (auto i = 0; i < numOfQueries; i++) {
        std::future<bool> placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                                      topology,
                                                                                      typeInferencePhase,
                                                                                      coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]);
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(placementResults[i].get());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        std::vector<ExecutionNodePtr> executionNodes =
            globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlans[i]->getId());

        //Assertion
        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->getId() == 1u) {
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlans[i]->getId());
                ASSERT_EQ(querySubPlans.size(), 1u);
                auto querySubPlan = querySubPlans[0u];
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            } else {
                EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlans[i]->getId());
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
}

/*
 * Test query placement using TopDown strategy for two queries:
 *
 * Q1: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 * Q2: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 *
 * On the following topology:
 *
 * Topology: rootNode(6)---srcNode1(4)
 *                     \
 *                      ---srcNode2 (4)
 *
 *  We perform both placements concurrently using the pessimistic approach.
 *  The Expectations are that both placements should be successful and should result in a consistent global execution plan.
 *
 */
TEST_F(QueryPlacementTest, testConcurrentOperatorPlacementUsingPessimisticTopDownStrategy) {

    setupTopologyAndSourceCatalog({6, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());

    // Setup Queries
    std::vector<QueryPlanPtr> queryPlans;
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    auto numOfQueries = 2;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
        QueryPlanPtr queryPlan = query.getQueryPlan();

        queryPlan = queryReWritePhase->execute(queryPlan);
        queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);
        typeInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<bool>> placementResults;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        auto placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                                      topology,
                                                                                      typeInferencePhase,
                                                                                      coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]);
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(placementResults[i].get());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        SharedQueryId sharedQueryId = sharedQueryPlans[i]->getId();
        std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->getId() == 1u) {
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
                ASSERT_EQ(querySubPlans.size(), 1u);
                auto querySubPlan = querySubPlans[0];
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                auto upstreamOperators = actualRootOperators[0]->getChildren();
                ASSERT_EQ(upstreamOperators.size(), 2u);
                for (const auto& upstreamOperator : upstreamOperators) {
                    EXPECT_TRUE(upstreamOperator->instanceOf<FilterLogicalOperatorNode>());
                }
            } else {
                EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
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
}

/*
 * Test query placement using TopDown strategy for two queries:
 *
 * Q1: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 * Q2: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 *
 * On the following topology:
 *
 * Topology: rootNode(6)---srcNode1(4)
 *                     \
 *                      ---srcNode2 (4)
 *
 *  We perform both placements concurrently using the optimistic approach.
 *  The Expectations are that both placements should be successful and should result in a consistent global execution plan.
 *
 */
TEST_F(QueryPlacementTest, testConcurrentOperatorPlacementUsingOptimisticTopDownStrategy) {

    setupTopologyAndSourceCatalog({6, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());

    // Setup Queries
    std::vector<QueryPlanPtr> queryPlans;
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    auto numOfQueries = 2;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
        QueryPlanPtr queryPlan = query.getQueryPlan();

        queryPlan = queryReWritePhase->execute(queryPlan);
        queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);
        typeInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<bool>> placementResults;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        auto placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                                      topology,
                                                                                      typeInferencePhase,
                                                                                      coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]);
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(placementResults[i].get());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        SharedQueryId sharedQueryId = sharedQueryPlans[i]->getId();
        std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryId);

        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->getId() == 1u) {
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
                ASSERT_EQ(querySubPlans.size(), 1u);
                auto querySubPlan = querySubPlans[0];
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                auto upstreamOperators = actualRootOperators[0]->getChildren();
                ASSERT_EQ(upstreamOperators.size(), 2u);
                for (const auto& upstreamOperator : upstreamOperators) {
                    EXPECT_TRUE(upstreamOperator->instanceOf<FilterLogicalOperatorNode>());
                }
            } else {
                EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryId);
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
}

/*
 * Test query placement using BottomUp strategy for two queries:
 *
 * Q1: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 * Q2: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 *
 * On the following topology:
 *
 * Topology: rootNode(6)---srcNode1(4)
 *                     \
 *                      ---srcNode2 (4)
 *
 *  We perform both placements concurrently using the optimistic approach.
 *  The Expectations are that both placements should be successful and should result in a consistent global execution plan.
 *
 */
TEST_F(QueryPlacementTest, testConcurrentOperatorPlacementUsingOptimisticBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());

    // Setup Queries
    std::vector<QueryPlanPtr> queryPlans;
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    auto numOfQueries = 2;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
        QueryPlanPtr queryPlan = query.getQueryPlan();

        queryPlan = queryReWritePhase->execute(queryPlan);
        queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
        typeInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<bool>> placementResults;
    for (auto i = 0; i < numOfQueries; i++) {
        std::future<bool> placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementPhase::create(globalExecutionPlan,
                                                                                      topology,
                                                                                      typeInferencePhase,
                                                                                      coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]);
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(placementResults[i].get());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        std::vector<ExecutionNodePtr> executionNodes =
            globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlans[i]->getId());

        //Assertion
        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->getId() == 1u) {
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlans[i]->getId());
                ASSERT_EQ(querySubPlans.size(), 1u);
                auto querySubPlan = querySubPlans[0u];
                std::vector<OperatorNodePtr> actualRootOperators = querySubPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorNodePtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperatorNode>());
                }
            } else {
                EXPECT_TRUE(executionNode->getId() == 2 || executionNode->getId() == 3);
                std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(sharedQueryPlans[i]->getId());
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
}

/*
 * Test query placement using BottomUp strategy for two queries:
 *
 * Q1: Query::from("car").filter(Attribute("id") < 45).map(Attribute("value") = 45).sink(PrintSinkDescriptor::create())
 * Q2: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 *
 * On the following topology:
 *
 * Topology: rootNode(2)---srcNode1(2)
 *                     \
 *                      ---srcNode2 (2)
 *
 *  We perform both placements concurrently using the optimistic approach.
 *  The Expectations are that Q1 will fails but Q2 will succeed.
 */
TEST_F(QueryPlacementTest, testIfCanPlaceQueryAfterPlacementFailureConcurrentOperatorPlacementUsingOptimisticBottomUpStrategy) {

    setupTopologyAndSourceCatalog({2, 2, 2});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());

    // Setup Queries
    std::vector<QueryPlanPtr> queryPlans;
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    auto numOfQueries = 2;

    //Setup the query that can not be placed
    Query query1 =
        Query::from("car").filter(Attribute("id") < 45).map(Attribute("value") = 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    typeInferencePhase->execute(queryPlan1);

    topologySpecificQueryRewrite->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);
    auto sharedQueryPlan1 = SharedQueryPlan::create(queryPlan1);
    auto sharedQueryPlanId1 = sharedQueryPlan1->getId();

    //Record the plans
    queryPlans.emplace_back(queryPlan1);
    sharedQueryPlans.emplace_back(sharedQueryPlan1);

    //Setup the query that can be placed
    Query query2 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    typeInferencePhase->execute(queryPlan2);

    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);
    auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
    auto sharedQueryPlanId2 = sharedQueryPlan1->getId();

    //Record the plans
    queryPlans.emplace_back(queryPlan2);
    sharedQueryPlans.emplace_back(sharedQueryPlan2);

    // Initiate placement requests
    auto queryPlacementPhaseInstance1 =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    EXPECT_THROW(queryPlacementPhaseInstance1->execute(sharedQueryPlans[0]), Exceptions::QueryPlacementException);

    auto queryPlacementPhaseInstance2 =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    EXPECT_TRUE(queryPlacementPhaseInstance2->execute(sharedQueryPlans[1]));

    // Check the execution plan for failed shared query plans
    //Assertion
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlans[0]->getId());
    ASSERT_EQ(executionNodes.size(), 0u);

    // Check the execution plan for success shared query plans
    //Assertion
    executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlans[1]->getId());
    ASSERT_EQ(executionNodes.size(), 3u);
}

/*
 * Test query placement using BottomUp strategy for two queries:
 *
 * Q1: Query::from("car").filter(Attribute("id") < 45).map(Attribute("value") = 45).sink(PrintSinkDescriptor::create())
 * Q2: Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create())
 *
 * On the following topology:
 *
 * Topology: rootNode(2)---srcNode1(2)
 *                     \
 *                      ---srcNode2 (2)
 *
 *  We perform both placements concurrently using the pessimistic approach.
 *  The Expectations are that Q1 will fails but Q2 will succeed.
 */
TEST_F(QueryPlacementTest, testIfCanPlaceQueryAfterPlacementFailureConcurrentOperatorPlacementUsingPessimisticBottomUpStrategy) {

    setupTopologyAndSourceCatalog({1, 2, 2});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::PESSIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());

    // Setup Queries
    std::vector<QueryPlanPtr> queryPlans;
    std::vector<SharedQueryPlanPtr> sharedQueryPlans;
    auto numOfQueries = 2;

    //Setup the query that can not be placed
    Query query1 =
        Query::from("car").filter(Attribute("id") < 45).map(Attribute("value") = 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan1 = query1.getQueryPlan();

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    typeInferencePhase->execute(queryPlan1);

    topologySpecificQueryRewrite->execute(queryPlan1);
    typeInferencePhase->execute(queryPlan1);
    auto sharedQueryPlan1 = SharedQueryPlan::create(queryPlan1);
    auto sharedQueryPlanId1 = sharedQueryPlan1->getId();

    //Record the plans
    queryPlans.emplace_back(queryPlan1);
    sharedQueryPlans.emplace_back(sharedQueryPlan1);

    //Setup the query that can be placed
    Query query2 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan2 = query2.getQueryPlan();

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    typeInferencePhase->execute(queryPlan2);

    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);
    auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
    auto sharedQueryPlanId2 = sharedQueryPlan1->getId();

    //Record the plans
    queryPlans.emplace_back(queryPlan2);
    sharedQueryPlans.emplace_back(sharedQueryPlan2);

    // Initiate placement requests
    auto queryPlacementPhaseInstance1 =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    EXPECT_THROW(queryPlacementPhaseInstance1->execute(sharedQueryPlans[0]), Exceptions::QueryPlacementException);

    auto queryPlacementPhaseInstance2 =
        Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, coordinatorConfiguration);
    EXPECT_TRUE(queryPlacementPhaseInstance2->execute(sharedQueryPlans[1]));

    // Check the execution plan for failed shared query plans
    //Assertion
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlans[0]->getId());
    ASSERT_EQ(executionNodes.size(), 0u);

    // Check the execution plan for success shared query plans
    //Assertion
    executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(sharedQueryPlans[1]->getId());
    ASSERT_EQ(executionNodes.size(), 3u);
}