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
#include <Operators/LogicalOperators/LogicalFilterOperator.hpp>
#include <Operators/LogicalOperators/LogicalInferModelOperator.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Watermarks/WatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperators/Windows/Joins/LogicalJoinOperator.hpp>
#include <Optimizer/Exceptions/QueryPlacementAdditionException.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/StatisticIdInferencePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedCompleteQueryMergerRule.hpp>
#include <Optimizer/QueryMerger/Z3SignatureBasedPartialQueryMergerRule.hpp>
#include <Optimizer/QueryPlacementAddition/BasePlacementAdditionStrategy.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Utils/PlanIterator.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <StatisticCollection/StatisticRegistry/StatisticRegistry.hpp>
#include <Util/DeploymentContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/QueryState.hpp>
#include <gtest/gtest.h>
#include <z3++.h>

using namespace NES;
using namespace z3;
using namespace Configurations;

class QueryPlacementAmendmentTest : public Testing::BaseUnitTest {
  public:
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::StatisticIdInferencePhasePtr statisticIdInferencePhase;
    std::shared_ptr<Catalogs::UDF::UDFCatalog> udfCatalog;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;
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

        auto statisticRegistry = Statistic::StatisticRegistry::create();
        auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(statisticRegistry,
                                                                              Statistic::DefaultStatisticProbeGenerator::create(),
                                                                              Statistic::DefaultStatisticCache::create(),
                                                                              topology);
    }

    void setupTopologyAndSourceCatalog(std::vector<uint16_t> resources) {

        topology = Topology::create();
        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
        properties["tf_installed"] = true;

        auto rootNodeId = WorkerId(1);
        topology->registerWorker(rootNodeId, "localhost", 123, 124, resources[0], properties, 0, 0);
        topology->addAsRootWorkerId(rootNodeId);

        auto sourceNode1Id = WorkerId(2);
        topology->registerWorker(sourceNode1Id, "localhost", 123, 124, resources[1], properties, 0, 0);

        auto sourceNode2Id = WorkerId(3);
        topology->registerWorker(sourceNode2Id, "localhost", 123, 124, resources[2], properties, 0, 0);

        auto carSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        const std::string carSourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(carSourceName, carSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(carSourceName);

        CSVSourceTypePtr csvSourceTypeForCar1 = CSVSourceType::create(carSourceName, "carPhysicalSourceName1");
        csvSourceTypeForCar1->setGatheringInterval(0);
        csvSourceTypeForCar1->setNumberOfTuplesToProducePerBuffer(0);
        CSVSourceTypePtr csvSourceTypeForCar2 = CSVSourceType::create(carSourceName, "carPhysicalSourceName2");
        csvSourceTypeForCar2->setGatheringInterval(0);
        csvSourceTypeForCar2->setNumberOfTuplesToProducePerBuffer(0);
        auto physicalSourceForCar1 = PhysicalSource::create(csvSourceTypeForCar1);
        auto physicalSourceForCar2 = PhysicalSource::create(csvSourceTypeForCar2);

        auto sourceCatalogEntry1 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceForCar1, logicalSource, sourceNode1Id);
        auto sourceCatalogEntry2 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceForCar2, logicalSource, sourceNode2Id);

        sourceCatalog->addPhysicalSource(carSourceName, sourceCatalogEntry1);
        sourceCatalog->addPhysicalSource(carSourceName, sourceCatalogEntry2);

        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
        statisticIdInferencePhase = Optimizer::StatisticIdInferencePhase::create();
    }

    static void assignDataModificationFactor(QueryPlanPtr queryPlan) {
        PlanIterator queryPlanIterator = PlanIterator(std::move(queryPlan));

        for (auto qPlanItr = queryPlanIterator.begin(); qPlanItr != PlanIterator::end(); ++qPlanItr) {
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
TEST_F(QueryPlacementAmendmentTest, testPlacingQueryWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    typeInferencePhase->execute(queryPlan);
    statisticIdInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1u);
            auto decomposedQueryPlan = decomposedQueryPlans[0u];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1u);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
            }
        }
    }
}

/* Test query placement with top down strategy  */
TEST_F(QueryPlacementAmendmentTest, testPlacingQueryWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
    statisticIdInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1u);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getId(), queryPlan->getRootOperators()[0]->getId());
            std::vector<SourceLogicalOperatorPtr> sourceOperators = decomposedQueryPlan->getSourceOperators();
            ASSERT_EQ(sourceOperators.size(), 2u);
            for (const auto& sourceOperator : sourceOperators) {
                EXPECT_TRUE(sourceOperator->instanceOf<SourceLogicalOperator>());
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1u);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1u);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            for (const auto& children : actualRootOperator->getChildren()) {
                EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks with bottom up strategy  */
TEST_F(QueryPlacementAmendmentTest, testPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

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
    statisticIdInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 2u);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorPtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1u);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2u);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with bottom up strategy  */
TEST_F(QueryPlacementAmendmentTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithBottomUpStrategy) {

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
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3u);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 2u);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorPtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1U);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2U);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with TopDown strategy  */
TEST_F(QueryPlacementAmendmentTest, testPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({6, 4, 4});

    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
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
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1UL);
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlans[0]->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (auto actualRootOperator : actualRootOperators) {
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorPtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1UL);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            }
        }
    }
}

/* Test query placement of query with multiple sinks with Bottom up strategy  */
TEST_F(QueryPlacementAmendmentTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithBottomUpStrategy) {

    setupTopologyAndSourceCatalog({5, 4, 4});

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);
    z3::ContextPtr context = std::make_shared<z3::context>();
    auto z3InferencePhase =
        Optimizer::SignatureInferencePhase::create(context,
                                                   NES::Optimizer::QueryMergerRule::Z3SignatureBasedCompleteQueryMergerRule);
    auto signatureBasedEqualQueryMergerRule = Optimizer::Z3SignatureBasedPartialQueryMergerRule::create(context);
    auto globalQueryPlan = GlobalQueryPlan::create();

    auto queryPlan1 = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create()).getQueryPlan();
    queryPlan1->setQueryId(QueryId(1));
    queryPlan1->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    queryPlan1 = queryReWritePhase->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    queryPlan1 = statisticIdInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    auto planToDeploy = updatedSharedQMToDeploy[0]->getQueryPlan();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(updatedSharedQMToDeploy[0]);
    updatedSharedQMToDeploy[0]->setStatus(SharedQueryPlanStatus::DEPLOYED);

    // new Query
    auto queryPlan2 = Query::from("car")
                          .filter(Attribute("id") < 45)
                          .map(Attribute("newId") = 2)
                          .sink(PrintSinkDescriptor::create())
                          .getQueryPlan();
    queryPlan2->setQueryId(QueryId(2));
    queryPlan2->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    queryPlan2 = queryReWritePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    queryPlan2 = topologySpecificReWrite->execute(queryPlan2);
    queryPlan2 = statisticIdInferencePhase->execute(queryPlan2);
    queryPlan2 = typeInferencePhase->execute(queryPlan2);
    z3InferencePhase->execute(queryPlan2);

    globalQueryPlan->addQueryPlan(queryPlan2);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();

    queryPlacementAmendmentPhase->execute(updatedSharedQMToDeploy[0]);

    SharedQueryId sharedQueryId = UNSURE_CONVERSION_TODO_4761(planToDeploy->getQueryId(), SharedQueryId);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 2UL);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1UL);
                auto actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = planToDeploy->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorPtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            // map merged into decomposedQueryPlan with filter
            ASSERT_EQ(decomposedQueryPlans.size(), 1UL);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                EXPECT_TRUE(
                    rootOperator->as<SinkLogicalOperator>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>());
            }
            for (const auto& sourceOperator : decomposedQueryPlan->getSourceOperators()) {
                EXPECT_TRUE(sourceOperator->getParents().size() == 1);
                auto sourceParent = sourceOperator->getParents()[0];
                EXPECT_TRUE(sourceParent->instanceOf<LogicalFilterOperator>());
                auto filterParents = sourceParent->getParents();
                EXPECT_TRUE(filterParents.size() == 2);
                uint8_t distinctParents = 0;
                for (const auto& filterParent : filterParents) {
                    if (filterParent->instanceOf<LogicalMapOperator>()) {
                        EXPECT_TRUE(filterParent->getParents()[0]->instanceOf<SinkLogicalOperator>());
                        distinctParents += 1;
                    } else {
                        EXPECT_TRUE(filterParent->instanceOf<SinkLogicalOperator>());
                        distinctParents += 2;
                    }
                }
                ASSERT_EQ(distinctParents, 3);
            }
        }
    }
}

TEST_F(QueryPlacementAmendmentTest, testPartialPlacingQueryWithMultipleSinkOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({10, 4, 4});

    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificReWrite = Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                                                        sourceCatalog,
                                                                                        Configurations::OptimizerConfiguration(),
                                                                                        statisticProbeHandler);
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
    queryPlan1 = statisticIdInferencePhase->execute(queryPlan1);
    queryPlan1 = topologySpecificReWrite->execute(queryPlan1);
    queryPlan1 = typeInferencePhase->execute(queryPlan1);
    z3InferencePhase->execute(queryPlan1);

    globalQueryPlan->addQueryPlan(queryPlan1);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    auto updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(updatedSharedQMToDeploy[0]);
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
    queryPlan2 = statisticIdInferencePhase->execute(queryPlan2);
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
    queryPlan3 = statisticIdInferencePhase->execute(queryPlan3);
    queryPlan3 = topologySpecificReWrite->execute(queryPlan3);
    queryPlan3 = typeInferencePhase->execute(queryPlan3);
    z3InferencePhase->execute(queryPlan3);

    globalQueryPlan->addQueryPlan(queryPlan3);
    signatureBasedEqualQueryMergerRule->apply(globalQueryPlan);

    updatedSharedQMToDeploy = globalQueryPlan->getSharedQueryPlansToDeploy();
    auto sharedQueryId = updatedSharedQMToDeploy[0]->getId();

    queryPlacementAmendmentPhase->execute(updatedSharedQMToDeploy[0]);

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1UL);
            ASSERT_EQ(decomposedQueryPlans[0]->getSinkOperators().size(), 3UL);
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            // map merged into decomposedQueryPlan with filter
            ASSERT_EQ(decomposedQueryPlans.size(), 1UL);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                EXPECT_TRUE(
                    rootOperator->as<SinkLogicalOperator>()->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>());
            }
        }
    }
}

/* Test query placement of query with multiple sinks and multiple source operators with Top Down strategy  */
TEST_F(QueryPlacementAmendmentTest, testPlacingQueryWithMultipleSinkAndOnlySourceOperatorsWithTopDownStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});

    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
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
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
    statisticIdInferencePhase->execute(queryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    //Assertion
    ASSERT_EQ(executionNodes.size(), 3UL);
    for (const auto& executionNode : executionNodes) {
        if (executionNode->operator*()->getId() == WorkerId(1)) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 2UL);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1UL);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                auto expectedRootOperators = queryPlan->getRootOperators();
                auto found = std::find_if(expectedRootOperators.begin(),
                                          expectedRootOperators.end(),
                                          [&](const OperatorPtr& expectedRootOperator) {
                                              return expectedRootOperator->getId() == actualRootOperator->getId();
                                          });
                EXPECT_TRUE(found != expectedRootOperators.end());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2UL);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            }
        } else {
            EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2) || executionNode->operator*()->getId() == WorkerId(3));
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(decomposedQueryPlans.size(), 1UL);
            auto decomposedQueryPlan = decomposedQueryPlans[0];
            std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2UL);
            for (const auto& rootOperator : actualRootOperators) {
                EXPECT_TRUE(rootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : rootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
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
TEST_F(QueryPlacementAmendmentTest, DISABLED_testIFCOPPlacement) {
    // Setup the topology
    // We are using a linear topology of three nodes:
    // srcNode -> midNode -> sinkNode
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = INVALID_WORKER_NODE_ID;
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 4, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);

    auto middleNodeId = WorkerId(1);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 4, properties, 0, 0);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);

    auto sourceNodeId = WorkerId(2);
    topology->registerWorker(sourceNodeId, "localhost", 4002, 5002, 4, properties, 0, 0);
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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId);
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
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    // Execute the placement
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    // check if map is placed two times
    uint32_t mapPlacementCount = 0;

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
            executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            OperatorPtr root = decomposedQueryPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->operator*()->getId() == INVALID_WORKER_NODE_ID;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<LogicalMapOperator>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                        if (childrenOfMapOp->as<SourceLogicalOperator>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid = executionNode->operator*()->getId() == WorkerId(2);
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<SourceLogicalOperator>());
                    // if the current operator is a source, it should be placed in topology node with id=2 (source nodes)
                    if (child->as<SourceLogicalOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid = executionNode->operator*()->getId() == WorkerId(2);
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
TEST_F(QueryPlacementAmendmentTest, DISABLED_testIFCOPPlacementOnBranchedTopology) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = INVALID_WORKER_NODE_ID;
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 4, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);

    auto middleNodeId1 = WorkerId(1);
    topology->registerWorker(middleNodeId1, "localhost", 4001, 5001, 4, properties, 0, 0);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId1);

    auto middleNodeId2 = WorkerId(2);
    topology->registerWorker(middleNodeId2, "localhost", 4001, 5001, 4, properties, 0, 0);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId2);

    auto sourceNodeId1 = WorkerId(3);
    topology->registerWorker(sourceNodeId1, "localhost", 4002, 5002, 4, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId1, sourceNodeId1);

    auto sourceNodeId2 = WorkerId(4);
    topology->registerWorker(sourceNodeId2, "localhost", 4002, 5002, 4, properties, 0, 0);
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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId1);
    auto sourceCatalogEntry2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId2);
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
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    const auto& sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);

    // Execute the placement phase
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString={}", globalExecutionPlan->getAsString());

    SharedQueryId sharedQueryId = sharedQueryPlan->getId();
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 5UL);
    // check if map is placed two times
    uint32_t mapPlacementCount = 0;

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
            executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            OperatorPtr root = decomposedQueryPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->operator*()->getId() == INVALID_WORKER_NODE_ID;
            }

            for (const auto& child : root->getChildren()) {
                if (child->instanceOf<LogicalMapOperator>()) {
                    mapPlacementCount++;
                    for (const auto& childrenOfMapOp : child->getChildren()) {
                        // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                        if (childrenOfMapOp->as<SourceLogicalOperator>()->getId()
                            == testQueryPlan->getSourceOperators()[0]->getId()) {
                            isSource1PlacementValid = executionNode->operator*()->getId() == WorkerId(3)
                                || executionNode->operator*()->getId() == WorkerId(4);
                        } else if (childrenOfMapOp->as<SourceLogicalOperator>()->getId()
                                   == testQueryPlan->getSourceOperators()[1]->getId()) {
                            isSource2PlacementValid = executionNode->operator*()->getId() == WorkerId(3)
                                || executionNode->operator*()->getId() == WorkerId(4);
                        }
                    }
                } else {
                    EXPECT_TRUE(child->instanceOf<SourceLogicalOperator>());
                    // if the current operator is a source, it should be placed in topology node with id 3 or 4 (source nodes)
                    if (child->as<SourceLogicalOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                        isSource1PlacementValid = executionNode->operator*()->getId() == WorkerId(3)
                            || executionNode->operator*()->getId() == WorkerId(4);
                    } else if (child->as<SourceLogicalOperator>()->getId() == testQueryPlan->getSourceOperators()[1]->getId()) {
                        isSource2PlacementValid = executionNode->operator*()->getId() == WorkerId(3)
                            || executionNode->operator*()->getId() == WorkerId(4);
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
 */
TEST_F(QueryPlacementAmendmentTest, testTopDownPlacementOfSelfJoinQuery) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 14, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 4, properties, 0, 0);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 4, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
                      .where(Attribute("id") == Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    NES_DEBUG("RandomSearchTest: globalExecutionPlanAsString={}", globalExecutionPlan->getAsString());

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        for (const auto& decomposedQueryPlan : executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId)) {
            OperatorPtr root = decomposedQueryPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->operator*()->getId() == WorkerId(1);
            }

            auto sourceOperators = decomposedQueryPlan->getSourceOperators();

            for (const auto& sourceOperator : sourceOperators) {
                if (sourceOperator->as<SourceLogicalOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->operator*()->getId() == WorkerId(3);
                } else if (sourceOperator->as<SourceLogicalOperator>()->getId()
                           == testQueryPlan->getSourceOperators()[1]->getId()) {
                    isSource2PlacementValid = executionNode->operator*()->getId() == WorkerId(3);
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
TEST_F(QueryPlacementAmendmentTest, testBottomUpPlacementOfSelfJoinQuery) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 14, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 4, properties, 0, 0);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 4, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
                      .where(Attribute("id") == Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);

    bool isSinkPlacementValid = false;
    bool isSource1PlacementValid = false;
    bool isSource2PlacementValid = false;
    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
            executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            OperatorPtr root = decomposedQueryPlan->getRootOperators()[0];

            // if the current operator is the sink of the query, it must be placed in the sink node (topology node with id 0)
            if (root->as<SinkLogicalOperator>()->getId() == testQueryPlan->getSinkOperators()[0]->getId()) {
                isSinkPlacementValid = executionNode->operator*()->getId() == WorkerId(1);
            }

            auto sourceOperators = decomposedQueryPlan->getSourceOperators();

            for (const auto& sourceOperator : sourceOperators) {
                if (sourceOperator->as<SourceLogicalOperator>()->getId() == testQueryPlan->getSourceOperators()[0]->getId()) {
                    isSource1PlacementValid = executionNode->operator*()->getId() == WorkerId(3);
                } else if (sourceOperator->as<SourceLogicalOperator>()->getId()
                           == testQueryPlan->getSourceOperators()[1]->getId()) {
                    isSource2PlacementValid = executionNode->operator*()->getId() == WorkerId(3);
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
 */
TEST_F(QueryPlacementAmendmentTest, testTopDownPlacementWthTightResourcesConstrains) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 1, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 1, properties, 0, 0);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 2, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
            executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            auto ops = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                ASSERT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                auto filter = sink->getChildren()[0];
                ASSERT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                ASSERT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
            } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                auto source = sink->getChildren()[0];
                ASSERT_TRUE(source->instanceOf<SourceLogicalOperator>());
                ASSERT_EQ(source->as<SourceLogicalOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]
                              ->getChildren()[0]
                              ->getChildren()[0]
                              ->as<SourceLogicalOperator>()
                              ->getId());
            }
            NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
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
TEST_F(QueryPlacementAmendmentTest, testBottomUpPlacementWthTightResourcesConstrains) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 1, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 1, properties, 0, 0);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 1, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);

    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    EXPECT_EQ(executionNodes.size(), 3UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
            executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            auto ops = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                ASSERT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                auto filter = sink->getChildren()[0];
                ASSERT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                ASSERT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
            } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                auto sink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                auto source = sink->getChildren()[0];
                ASSERT_TRUE(source->instanceOf<SourceLogicalOperator>());
                ASSERT_EQ(source->as<SourceLogicalOperator>()->getId(),
                          testQueryPlan->getRootOperators()[0]
                              ->getChildren()[0]
                              ->getChildren()[0]
                              ->as<SourceLogicalOperator>()
                              ->getId());
            }
            NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
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
TEST_F(QueryPlacementAmendmentTest, testBottomUpPlacementWthTightResourcesConstrainsInAJoin) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 1, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 5, properties, 0, 0);
    auto srcNodeId1 = WorkerId(3);
    topology->registerWorker(srcNodeId1, "localhost", 4003, 5003, 1, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId1);
    auto srcNodeId2 = WorkerId(4);
    topology->registerWorker(srcNodeId2, "localhost", 4003, 5003, 1, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId2);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId1);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId2);

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
        auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId1);
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
        auto sourceCatalogEntry2 = Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, srcNodeId2);
        sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry2);
    }

    Query query = Query::from("car1")
                      .joinWith(Query::from("car2"))
                      .where(Attribute("id") == Attribute("id"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                      .sink(NullOutputSinkDescriptor::create());

    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    auto deploymentUnit = queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

    auto sink = testQueryPlan->getSinkOperators()[0]->as<SinkLogicalOperator>();
    auto join = sink->getChildren()[0]->as<LogicalJoinOperator>();
    auto watermark1 = join->getChildren()[0]->as<WatermarkAssignerLogicalOperator>();
    auto source1 = watermark1->getChildren()[0]->as<SourceLogicalOperator>();
    auto watermark2 = join->getChildren()[1]->as<WatermarkAssignerLogicalOperator>();
    auto source2 = watermark2->getChildren()[0]->as<SourceLogicalOperator>();

    EXPECT_EQ(executionNodes.size(), 4UL);
    NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
    for (const auto& executionNode : executionNodes) {
        std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
            executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            auto ops = decomposedQueryPlan->getRootOperators();
            ASSERT_EQ(ops.size(), 1);
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                ASSERT_EQ(ops[0]->getId(), sink->getId());
                ASSERT_EQ(ops[0]->getChildren().size(), 1);
                EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                auto placedSink = ops[0];
                ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                auto placedJoin = placedSink->getChildren()[0];
                ASSERT_TRUE(placedJoin->instanceOf<LogicalJoinOperator>());
                ASSERT_EQ(placedJoin->as<LogicalJoinOperator>()->getId(), join->getId());
                ASSERT_EQ(placedJoin->getChildren().size(), 2);
                auto placedWatermark1 = placedJoin->getChildren()[0];
                ASSERT_TRUE(placedWatermark1->instanceOf<WatermarkAssignerLogicalOperator>());
                auto placedWatermark2 = placedJoin->getChildren()[1];
                ASSERT_TRUE(placedWatermark2->instanceOf<WatermarkAssignerLogicalOperator>());
            } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                auto placedSink = ops[0];
                ASSERT_TRUE(placedSink->instanceOf<SinkLogicalOperator>());
                for (const auto& placedSource : placedSink->getChildren()) {
                    ASSERT_TRUE(placedSource->instanceOf<SourceLogicalOperator>());
                };
            } else if (executionNode->operator*()->getId() == WorkerId(4)) {
                auto placedSink = ops[0];
                ASSERT_TRUE(placedSink->instanceOf<SinkLogicalOperator>());
                auto placedSource = placedSink->getChildren()[0];
                ASSERT_TRUE(placedSource->instanceOf<SourceLogicalOperator>());
                ASSERT_EQ(placedSource->as<SourceLogicalOperator>()->getId(), source2->getId());
            }
            NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
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
TEST_F(QueryPlacementAmendmentTest, testConcurrentOperatorPlacementUsingPessimisticBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);

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
        statisticIdInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<std::set<Optimizer::DeploymentContextPtr>>> placementResults;
    for (auto i = 0; i < numOfQueries; i++) {
        std::future<std::set<Optimizer::DeploymentContextPtr>> placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                               topology,
                                                                                               typeInferencePhase,
                                                                                               coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]).getAllDeploymentContexts();
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(!placementResults[i].get().empty());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlans[i]->getId());

        //Assertion
        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlans[i]->getId());
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0u];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            } else {
                EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2)
                            || executionNode->operator*()->getId() == WorkerId(3));
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlans[i]->getId());
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
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
TEST_F(QueryPlacementAmendmentTest, testConcurrentOperatorPlacementUsingPessimisticTopDownStrategy) {

    setupTopologyAndSourceCatalog({6, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);

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
        statisticIdInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<std::set<Optimizer::DeploymentContextPtr>>> placementResults;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        auto placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                               topology,
                                                                                               typeInferencePhase,
                                                                                               coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]).getAllDeploymentContexts();
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(!placementResults[i].get().empty());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        SharedQueryId sharedQueryId = sharedQueryPlans[i]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                auto upstreamOperators = actualRootOperators[0]->getChildren();
                ASSERT_EQ(upstreamOperators.size(), 2u);
                for (const auto& upstreamOperator : upstreamOperators) {
                    EXPECT_TRUE(upstreamOperator->instanceOf<LogicalFilterOperator>());
                }
            } else {
                EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2)
                            || executionNode->operator*()->getId() == WorkerId(3));
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
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
TEST_F(QueryPlacementAmendmentTest, testConcurrentOperatorPlacementUsingOptimisticTopDownStrategy) {

    setupTopologyAndSourceCatalog({6, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);

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
        statisticIdInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<std::set<Optimizer::DeploymentContextPtr>>> placementResults;
    for (uint16_t i = 0; i < numOfQueries; i++) {
        auto placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                               topology,
                                                                                               typeInferencePhase,
                                                                                               coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]).getAllDeploymentContexts();
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(!placementResults[i].get().empty());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        SharedQueryId sharedQueryId = sharedQueryPlans[i]->getId();
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                auto upstreamOperators = actualRootOperators[0]->getChildren();
                ASSERT_EQ(upstreamOperators.size(), 2u);
                for (const auto& upstreamOperator : upstreamOperators) {
                    EXPECT_TRUE(upstreamOperator->instanceOf<LogicalFilterOperator>());
                }
            } else {
                EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2)
                            || executionNode->operator*()->getId() == WorkerId(3));
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
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
TEST_F(QueryPlacementAmendmentTest, testConcurrentOperatorPlacementUsingOptimisticBottomUpStrategy) {

    setupTopologyAndSourceCatalog({4, 4, 4});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);

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
        statisticIdInferencePhase->execute(queryPlan);

        topologySpecificQueryRewrite->execute(queryPlan);
        typeInferencePhase->execute(queryPlan);
        auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
        auto sharedQueryPlanId = sharedQueryPlan->getId();

        //Record the plans
        queryPlans.emplace_back(queryPlan);
        sharedQueryPlans.emplace_back(sharedQueryPlan);
    }

    // Initiate placement requests
    std::vector<std::future<std::set<Optimizer::DeploymentContextPtr>>> placementResults;
    for (auto i = 0; i < numOfQueries; i++) {
        auto placementResult = std::async(std::launch::async, [&, index = i]() {
            auto queryPlacementPhaseInstance = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                               topology,
                                                                                               typeInferencePhase,
                                                                                               coordinatorConfiguration);
            return queryPlacementPhaseInstance->execute(sharedQueryPlans[index]).getAllDeploymentContexts();
        });
        placementResults.emplace_back(std::move(placementResult));
    }

    // Make sure both placement succeeded
    for (uint16_t i = 0; i < numOfQueries; i++) {
        EXPECT_TRUE(!placementResults[i].get().empty());
    }

    // Check the execution plan for both shared query plans
    for (uint16_t i = 0; i < numOfQueries; i++) {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlans[i]->getId());

        //Assertion
        ASSERT_EQ(executionNodes.size(), 3u);
        for (const auto& executionNode : executionNodes) {
            if (executionNode->operator*()->getId() == WorkerId(1)) {
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlans[i]->getId());
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0u];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                ASSERT_EQ(actualRootOperator->getId(), queryPlans[i]->getRootOperators()[0]->getId());
                ASSERT_EQ(actualRootOperator->getChildren().size(), 2u);
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<SourceLogicalOperator>());
                }
            } else {
                EXPECT_TRUE(executionNode->operator*()->getId() == WorkerId(2)
                            || executionNode->operator*()->getId() == WorkerId(3));
                auto decomposedQueryPlans = executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryPlans[i]->getId());
                ASSERT_EQ(decomposedQueryPlans.size(), 1u);
                auto decomposedQueryPlan = decomposedQueryPlans[0];
                std::vector<OperatorPtr> actualRootOperators = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(actualRootOperators.size(), 1u);
                OperatorPtr actualRootOperator = actualRootOperators[0];
                EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
                for (const auto& children : actualRootOperator->getChildren()) {
                    EXPECT_TRUE(children->instanceOf<LogicalFilterOperator>());
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
TEST_F(QueryPlacementAmendmentTest, testIfCanPlaceQueryAfterPlacementFailureConcurrentOperatorPlacementUsingOCCBottomUpStrategy) {

    setupTopologyAndSourceCatalog({2, 2, 2});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::OPTIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);

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
    statisticIdInferencePhase->execute(queryPlan1);

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
    statisticIdInferencePhase->execute(queryPlan2);

    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);
    auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
    auto sharedQueryPlanId2 = sharedQueryPlan1->getId();

    //Record the plans
    queryPlans.emplace_back(queryPlan2);
    sharedQueryPlans.emplace_back(sharedQueryPlan2);

    // Initiate placement requests
    auto queryPlacementPhaseInstance1 = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementPhaseInstance1->execute(sharedQueryPlans[0]);
    EXPECT_EQ(sharedQueryPlans[0]->getStatus(), SharedQueryPlanStatus::PARTIALLY_PROCESSED);

    auto queryPlacementPhaseInstance2 = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementPhaseInstance2->execute(sharedQueryPlans[1]);
    EXPECT_EQ(sharedQueryPlans[1]->getStatus(), SharedQueryPlanStatus::PROCESSED);

    // Check the execution plan for failed shared query plans
    //Assertion
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlans[0]->getId());
    ASSERT_EQ(executionNodes.size(), 0u);

    // Check the execution plan for success shared query plans
    //Assertion
    executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlans[1]->getId());
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
TEST_F(QueryPlacementAmendmentTest, testIfCanPlaceQueryAfterPlacementFailureConcurrentOperatorPlacementUsing2PLBottomUpStrategy) {

    setupTopologyAndSourceCatalog({1, 2, 2});
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.placementAmendmentMode = Optimizer::PlacementAmendmentMode::PESSIMISTIC;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);

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
    statisticIdInferencePhase->execute(queryPlan1);

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
    statisticIdInferencePhase->execute(queryPlan2);

    topologySpecificQueryRewrite->execute(queryPlan2);
    typeInferencePhase->execute(queryPlan2);
    auto sharedQueryPlan2 = SharedQueryPlan::create(queryPlan2);
    auto sharedQueryPlanId2 = sharedQueryPlan1->getId();

    //Record the plans
    queryPlans.emplace_back(queryPlan2);
    sharedQueryPlans.emplace_back(sharedQueryPlan2);

    // Initiate placement requests
    auto queryPlacementPhaseInstance1 = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementPhaseInstance1->execute(sharedQueryPlans[0]);
    EXPECT_EQ(sharedQueryPlans[0]->getStatus(), SharedQueryPlanStatus::PARTIALLY_PROCESSED);

    auto queryPlacementPhaseInstance2 = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementPhaseInstance1->execute(sharedQueryPlans[1]);
    EXPECT_EQ(sharedQueryPlans[1]->getStatus(), SharedQueryPlanStatus::PROCESSED);

    // Check the execution plan for failed shared query plans
    //Assertion
    auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlans[0]->getId());
    ASSERT_EQ(executionNodes.size(), 0u);

    // Check the execution plan for success shared query plans
    //Assertion
    executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryPlans[1]->getId());
    ASSERT_EQ(executionNodes.size(), 3u);
}

/**
 * Test re-placement of a query plan using TopDownPlacement
 * Topology: sinkNode(1)--mid1(1)--srcNode1(A)(2)
 *
 * Query: SinkOp--filter()--source(A)
 */
TEST_F(QueryPlacementAmendmentTest, testTopDownForRePlacement) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 1, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 1, properties, 0, 0);
    topology->addTopologyNodeAsChild(rootNodeId, middleNodeId);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 2, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::TopDown);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    Network::NesPartition sourcePartitionNode0beforeReplacement(INVALID_SHARED_QUERY_ID,
                                                                INVALID_OPERATOR_ID,
                                                                PartitionId(0),
                                                                SubpartitionId(0));
    Network::NesPartition sinkPartitionNode1BeforeReplacement(INVALID_SHARED_QUERY_ID,
                                                              INVALID_OPERATOR_ID,
                                                              PartitionId(0),
                                                              SubpartitionId(0));
    Network::NesPartition sourcePartitionNode1beforeReplacement(INVALID_SHARED_QUERY_ID,
                                                                INVALID_OPERATOR_ID,
                                                                PartitionId(0),
                                                                SubpartitionId(0));
    Network::NesPartition sinkPartitionNode2BeforeReplacement(INVALID_SHARED_QUERY_ID,
                                                              INVALID_OPERATOR_ID,
                                                              PartitionId(0),
                                                              SubpartitionId(0));
    DecomposedQueryId subPlanIdToRemoveInNextIteration = INVALID_DECOMPOSED_QUERY_PLAN_ID;

    {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

        EXPECT_EQ(executionNodes.size(), 3UL);
        NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
        for (const auto& executionNode : executionNodes) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                auto ops = decomposedQueryPlan->getRootOperators();
                ASSERT_EQ(ops.size(), 1);
                if (executionNode->operator*()->getId() == WorkerId(1)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    ASSERT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                    ASSERT_EQ(ops[0]->getChildren().size(), 1);
                    EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
                    sourcePartitionNode0beforeReplacement = ops[0]
                                                                ->getChildren()[0]
                                                                ->as<SourceLogicalOperator>()
                                                                ->getSourceDescriptor()
                                                                ->as<Network::NetworkSourceDescriptor>()
                                                                ->getNesPartition();
                } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                    auto sink = ops[0];
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    sinkPartitionNode1BeforeReplacement = sink->as<SinkLogicalOperator>()
                                                              ->getSinkDescriptor()
                                                              ->as<Network::NetworkSinkDescriptor>()
                                                              ->getNesPartition();
                    auto filter = sink->getChildren()[0];
                    ASSERT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                    ASSERT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                              testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
                    ASSERT_EQ(filter->getChildren().size(), 1);

                    sourcePartitionNode1beforeReplacement = filter->getChildren()[0]
                                                                ->as<SourceLogicalOperator>()
                                                                ->getSourceDescriptor()
                                                                ->as<Network::NetworkSourceDescriptor>()
                                                                ->getNesPartition();
                    subPlanIdToRemoveInNextIteration = decomposedQueryPlan->getDecomposedQueryId();
                } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                    auto sink = ops[0];
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    sinkPartitionNode2BeforeReplacement = sink->as<SinkLogicalOperator>()
                                                              ->getSinkDescriptor()
                                                              ->as<Network::NetworkSinkDescriptor>()
                                                              ->getNesPartition();
                    auto source = sink->getChildren()[0];
                    ASSERT_TRUE(source->instanceOf<SourceLogicalOperator>());
                    ASSERT_EQ(source->as<SourceLogicalOperator>()->getId(),
                              testQueryPlan->getRootOperators()[0]
                                  ->getChildren()[0]
                                  ->getChildren()[0]
                                  ->as<SourceLogicalOperator>()
                                  ->getId());
                }
                NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
            }
        }
        ASSERT_EQ(sinkPartitionNode1BeforeReplacement, sourcePartitionNode0beforeReplacement);
        ASSERT_EQ(sinkPartitionNode2BeforeReplacement, sourcePartitionNode1beforeReplacement);
    }
    const QueryPlanPtr& queryPlan = sharedQueryPlan->getQueryPlan();

    auto sourceOperators = queryPlan->getSourceOperators();
    std::set<OperatorId> sourceOperatorIds;
    std::for_each(sourceOperators.begin(), sourceOperators.end(), [&](const OperatorPtr& item) {
        sourceOperatorIds.emplace(item->getId());
    });

    auto sinkOperators = queryPlan->getSinkOperators();
    std::set<OperatorId> sinkOperatorIds;
    std::for_each(sinkOperators.begin(), sinkOperators.end(), [&](const OperatorPtr& item) {
        sinkOperatorIds.emplace(item->getId());
    });

    sharedQueryPlan->performReOperatorPlacement(sourceOperatorIds, sinkOperatorIds);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    Network::NesPartition sourcePartitionNode0afterReplacement(INVALID_SHARED_QUERY_ID,
                                                               INVALID_OPERATOR_ID,
                                                               PartitionId(0),
                                                               SubpartitionId(0));
    Network::NesPartition sinkPartitionNode1afterReplacement(INVALID_SHARED_QUERY_ID,
                                                             INVALID_OPERATOR_ID,
                                                             PartitionId(0),
                                                             SubpartitionId(0));
    Network::NesPartition sourcePartitionNode1afterReplacement(INVALID_SHARED_QUERY_ID,
                                                               INVALID_OPERATOR_ID,
                                                               PartitionId(0),
                                                               SubpartitionId(0));
    Network::NesPartition sinkPartitionNode2afterReplacement(INVALID_SHARED_QUERY_ID,
                                                             INVALID_OPERATOR_ID,
                                                             PartitionId(0),
                                                             SubpartitionId(0));

    {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);

        EXPECT_EQ(executionNodes.size(), 3UL);
        NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
        for (const auto& executionNode : executionNodes) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                if (decomposedQueryPlan->getState() == QueryState::MARKED_FOR_MIGRATION) {
                    continue;
                }
                auto ops = decomposedQueryPlan->getRootOperators();
                if (executionNode->operator*()->getId() == WorkerId(1)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_EQ(ops.size(), 1);
                    ASSERT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                    ASSERT_EQ(ops[0]->getChildren().size(), 1);
                    EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
                    sourcePartitionNode0afterReplacement = ops[0]
                                                               ->getChildren()[0]
                                                               ->as<SourceLogicalOperator>()
                                                               ->getSourceDescriptor()
                                                               ->as<Network::NetworkSourceDescriptor>()
                                                               ->getNesPartition();
                } else if (executionNode->operator*()->getId() == WorkerId(2)
                           && decomposedQueryPlan->getDecomposedQueryId() == subPlanIdToRemoveInNextIteration) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                } else if (executionNode->operator*()->getId() == WorkerId(2)
                           && decomposedQueryPlan->getDecomposedQueryId() != subPlanIdToRemoveInNextIteration) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_EQ(ops.size(), 1);
                    auto sink = ops[0];
                    sinkPartitionNode1afterReplacement = sink->as<SinkLogicalOperator>()
                                                             ->getSinkDescriptor()
                                                             ->as<Network::NetworkSinkDescriptor>()
                                                             ->getNesPartition();
                    ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    auto filter = sink->getChildren()[0];
                    ASSERT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                    ASSERT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                              testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
                    sourcePartitionNode1afterReplacement = filter->getChildren()[0]
                                                               ->as<SourceLogicalOperator>()
                                                               ->getSourceDescriptor()
                                                               ->as<Network::NetworkSourceDescriptor>()
                                                               ->getNesPartition();
                } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_EQ(ops.size(), 1);
                    auto sink = ops[0];
                    sinkPartitionNode2afterReplacement = sink->as<SinkLogicalOperator>()
                                                             ->getSinkDescriptor()
                                                             ->as<Network::NetworkSinkDescriptor>()
                                                             ->getNesPartition();
                    ASSERT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    auto source = sink->getChildren()[0];
                    ASSERT_TRUE(source->instanceOf<SourceLogicalOperator>());
                    ASSERT_EQ(source->as<SourceLogicalOperator>()->getId(),
                              testQueryPlan->getRootOperators()[0]
                                  ->getChildren()[0]
                                  ->getChildren()[0]
                                  ->as<SourceLogicalOperator>()
                                  ->getId());
                }
                NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
            }
        }
        ASSERT_EQ(sinkPartitionNode1afterReplacement, sourcePartitionNode0afterReplacement);
        ASSERT_EQ(sinkPartitionNode2afterReplacement, sourcePartitionNode1afterReplacement);

        //check that the descriptors have actually been updated
        ASSERT_NE(sinkPartitionNode1afterReplacement, sinkPartitionNode1BeforeReplacement);
        ASSERT_NE(sinkPartitionNode2afterReplacement, sinkPartitionNode2BeforeReplacement);
    }
}

/**
 * Test re-placement of a query plan using BottomUpPlacement
 * Topology: sinkNode(1)--mid1(1)--srcNode1(A)(2)
 *
 * Query: SinkOp--filter()--source(A)
 */
TEST_F(QueryPlacementAmendmentTest, testBottomUpForRePlacement) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 1, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 1, properties, 0, 0);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 2, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    coordinatorConfiguration->optimizer.enableIncrementalPlacement = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    assignDataModificationFactor(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    Network::NesPartition sourcePartitionNode0beforeReplacement(INVALID_SHARED_QUERY_ID,
                                                                INVALID_OPERATOR_ID,
                                                                PartitionId(0),
                                                                SubpartitionId(0));
    Network::NesPartition sinkPartitionNode1BeforeReplacement(INVALID_SHARED_QUERY_ID,
                                                              INVALID_OPERATOR_ID,
                                                              PartitionId(0),
                                                              SubpartitionId(0));
    Network::NesPartition sourcePartitionNode1beforeReplacement(INVALID_SHARED_QUERY_ID,
                                                                INVALID_OPERATOR_ID,
                                                                PartitionId(0),
                                                                SubpartitionId(0));
    Network::NesPartition sinkPartitionNode2BeforeReplacement(INVALID_SHARED_QUERY_ID,
                                                              INVALID_OPERATOR_ID,
                                                              PartitionId(0),
                                                              SubpartitionId(0));
    DecomposedQueryId planIdToRemoveInNextIteration = INVALID_DECOMPOSED_QUERY_PLAN_ID;

    {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
        EXPECT_EQ(executionNodes.size(), 3UL);
        NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
        for (const auto& executionNode : executionNodes) {
            for (const auto& decomposedQueryPlan : executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId)) {
                auto ops = decomposedQueryPlan->getRootOperators();
                EXPECT_EQ(ops.size(), 1);
                if (executionNode->operator*()->getId() == WorkerId(1)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                    EXPECT_EQ(ops[0]->getChildren().size(), 1);
                    EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
                    sourcePartitionNode0beforeReplacement = ops[0]
                                                                ->getChildren()[0]
                                                                ->as<SourceLogicalOperator>()
                                                                ->getSourceDescriptor()
                                                                ->as<Network::NetworkSourceDescriptor>()
                                                                ->getNesPartition();
                } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                    auto sink = ops[0];
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    sinkPartitionNode1BeforeReplacement = sink->as<SinkLogicalOperator>()
                                                              ->getSinkDescriptor()
                                                              ->as<Network::NetworkSinkDescriptor>()
                                                              ->getNesPartition();
                    auto source = sink->getChildren()[0];
                    EXPECT_TRUE(source->instanceOf<SourceLogicalOperator>());
                    sourcePartitionNode1beforeReplacement = source->as<SourceLogicalOperator>()
                                                                ->getSourceDescriptor()
                                                                ->as<Network::NetworkSourceDescriptor>()
                                                                ->getNesPartition();
                    planIdToRemoveInNextIteration = decomposedQueryPlan->getDecomposedQueryId();
                } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                    auto sink = ops[0];
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    sinkPartitionNode2BeforeReplacement = sink->as<SinkLogicalOperator>()
                                                              ->getSinkDescriptor()
                                                              ->as<Network::NetworkSinkDescriptor>()
                                                              ->getNesPartition();
                    auto source = sink->getChildren()[0];
                    EXPECT_TRUE(source->instanceOf<LogicalFilterOperator>());
                    EXPECT_EQ(source->as<LogicalFilterOperator>()->getId(),
                              testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
                }
                NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
            }
        }

        ASSERT_EQ(sinkPartitionNode1BeforeReplacement, sourcePartitionNode0beforeReplacement);
        ASSERT_EQ(sinkPartitionNode2BeforeReplacement, sourcePartitionNode1beforeReplacement);
    }

    const QueryPlanPtr& queryPlan = sharedQueryPlan->getQueryPlan();

    auto filterOperators = queryPlan->getOperatorByType<LogicalFilterOperator>();
    std::set<OperatorId> sourceOperatorIds;
    std::for_each(filterOperators.begin(), filterOperators.end(), [&](const OperatorPtr& item) {
        sourceOperatorIds.emplace(item->getId());
    });

    auto sinkOperators = queryPlan->getSinkOperators();
    std::set<OperatorId> sinkOperatorIds;
    std::for_each(sinkOperators.begin(), sinkOperators.end(), [&](const OperatorPtr& item) {
        sinkOperatorIds.emplace(item->getId());
    });

    sharedQueryPlan->performReOperatorPlacement(sourceOperatorIds, sinkOperatorIds);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);

    Network::NesPartition sourcePartitionNode0afterReplacement(INVALID_SHARED_QUERY_ID,
                                                               INVALID_OPERATOR_ID,
                                                               PartitionId(0),
                                                               SubpartitionId(0));
    Network::NesPartition sinkPartitionNode1afterReplacement(INVALID_SHARED_QUERY_ID,
                                                             INVALID_OPERATOR_ID,
                                                             PartitionId(0),
                                                             SubpartitionId(0));
    Network::NesPartition sourcePartitionNode1afterReplacement(INVALID_SHARED_QUERY_ID,
                                                               INVALID_OPERATOR_ID,
                                                               PartitionId(0),
                                                               SubpartitionId(0));
    Network::NesPartition sinkPartitionNode2afterReplacement(INVALID_SHARED_QUERY_ID,
                                                             INVALID_OPERATOR_ID,
                                                             PartitionId(0),
                                                             SubpartitionId(0));

    {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
        EXPECT_EQ(executionNodes.size(), 3UL);
        NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
        for (const auto& executionNode : executionNodes) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                bool foundMigration = false;
                auto ops = decomposedQueryPlan->getRootOperators();
                if (executionNode->operator*()->getId() == WorkerId(1)) {
                    ASSERT_EQ(decomposedQueryPlans.size(), 2);
                    if (decomposedQueryPlan->getState() == QueryState::MARKED_FOR_MIGRATION) {
                        ASSERT_FALSE(foundMigration);
                        foundMigration = true;
                        continue;
                    }
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    EXPECT_EQ(ops.size(), 1);
                    EXPECT_EQ(ops[0]->getId(), testQueryPlan->getRootOperators()[0]->getId());
                    EXPECT_EQ(ops[0]->getChildren().size(), 1);
                    EXPECT_TRUE(ops[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
                    sourcePartitionNode0afterReplacement = ops[0]
                                                               ->getChildren()[0]
                                                               ->as<SourceLogicalOperator>()
                                                               ->getSourceDescriptor()
                                                               ->as<Network::NetworkSourceDescriptor>()
                                                               ->getNesPartition();
                } else if (executionNode->operator*()->getId() == WorkerId(2)
                           && decomposedQueryPlan->getDecomposedQueryId() == planIdToRemoveInNextIteration) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_MIGRATION);
                } else if (executionNode->operator*()->getId() == WorkerId(2)
                           && decomposedQueryPlan->getDecomposedQueryId() != planIdToRemoveInNextIteration) {
                    EXPECT_EQ(ops.size(), 1);
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_DEPLOYMENT);
                    auto sink = ops[0];
                    sinkPartitionNode1afterReplacement = sink->as<SinkLogicalOperator>()
                                                             ->getSinkDescriptor()
                                                             ->as<Network::NetworkSinkDescriptor>()
                                                             ->getNesPartition();
                    EXPECT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    EXPECT_TRUE(sink->getChildren()[0]->instanceOf<SourceLogicalOperator>());
                    sourcePartitionNode1afterReplacement = sink->getChildren()[0]
                                                               ->as<SourceLogicalOperator>()
                                                               ->getSourceDescriptor()
                                                               ->as<Network::NetworkSourceDescriptor>()
                                                               ->getNesPartition();
                } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_REDEPLOYMENT);
                    EXPECT_EQ(ops.size(), 1);
                    auto sink = ops[0];
                    EXPECT_TRUE(sink->instanceOf<SinkLogicalOperator>());
                    sinkPartitionNode2afterReplacement = sink->as<SinkLogicalOperator>()
                                                             ->getSinkDescriptor()
                                                             ->as<Network::NetworkSinkDescriptor>()
                                                             ->getNesPartition();
                    auto filter = sink->getChildren()[0];
                    EXPECT_TRUE(filter->instanceOf<LogicalFilterOperator>());
                    EXPECT_EQ(filter->as<LogicalFilterOperator>()->getId(),
                              testQueryPlan->getRootOperators()[0]->getChildren()[0]->as<LogicalFilterOperator>()->getId());
                }
                NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
            }
        }
        ASSERT_EQ(sinkPartitionNode1afterReplacement, sourcePartitionNode0afterReplacement);
        ASSERT_EQ(sinkPartitionNode2afterReplacement, sourcePartitionNode1afterReplacement);

        //check that the descriptors have actually been updated
        ASSERT_NE(sinkPartitionNode1afterReplacement, sinkPartitionNode1BeforeReplacement);
        ASSERT_NE(sinkPartitionNode2afterReplacement, sinkPartitionNode2BeforeReplacement);
    }
}

/**
 * Test re-placement of a query plan using BottomUpPlacement
 * Topology: sinkNode(1)--mid1(1)--srcNode1(A)(2)
 *
 * Query: SinkOp--filter()--source(A)
 */
TEST_F(QueryPlacementAmendmentTest, testBottomUpForProcessingSharedQueryPlanToBeRemoved) {
    // Setup the topology
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

    TopologyPtr topology = Topology::create();

    auto rootNodeId = WorkerId(1);
    topology->registerWorker(rootNodeId, "localhost", 4000, 5000, 1, properties, 0, 0);
    topology->addAsRootWorkerId(rootNodeId);
    auto middleNodeId = WorkerId(2);
    topology->registerWorker(middleNodeId, "localhost", 4001, 5001, 1, properties, 0, 0);
    auto srcNodeId = WorkerId(3);
    topology->registerWorker(srcNodeId, "localhost", 4003, 5003, 2, properties, 0, 0);
    topology->addTopologyNodeAsChild(middleNodeId, srcNodeId);
    topology->removeTopologyNodeAsChild(rootNodeId, srcNodeId);

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
    auto sourceCatalogEntry1 = Catalogs::Source::SourceCatalogEntry::create(physicalSourceCar, logicalSource, srcNodeId);
    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry1);

    Query query = Query::from("car").filter(Attribute("value") > 1).sink(NullOutputSinkDescriptor::create());
    auto testQueryPlan = query.getQueryPlan();
    testQueryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    testQueryPlan->setQueryId(QueryId(1));

    // Prepare the placement
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    auto typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    // Execute optimization phases prior to placement
    testQueryPlan = typeInferencePhase->execute(testQueryPlan);
    auto coordinatorConfiguration = Configurations::CoordinatorConfiguration::createDefault();
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfiguration);
    testQueryPlan = queryReWritePhase->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);
    statisticIdInferencePhase->execute(testQueryPlan);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology,
                                                             sourceCatalog,
                                                             Configurations::OptimizerConfiguration(),
                                                             statisticProbeHandler);
    topologySpecificQueryRewrite->execute(testQueryPlan);
    typeInferencePhase->execute(testQueryPlan);

    // Execute the placement
    auto sharedQueryPlan = SharedQueryPlan::create(testQueryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementAmendmentPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                                        topology,
                                                                                        typeInferencePhase,
                                                                                        coordinatorConfiguration);
    queryPlacementAmendmentPhase->execute(sharedQueryPlan);
    {
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
        EXPECT_EQ(executionNodes.size(), 3UL);
    }

    {
        sharedQueryPlan->markQueryForRemoval(QueryId(1));
        queryPlacementAmendmentPhase->execute(sharedQueryPlan);
        auto executionNodes = globalExecutionPlan->getLockedExecutionNodesHostingSharedQueryId(sharedQueryId);
        EXPECT_EQ(executionNodes.size(), 3UL);
        NES_INFO("Test Query Plan:\n {}", testQueryPlan->toString());
        for (const auto& executionNode : executionNodes) {
            std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans =
                executionNode->operator*()->getAllDecomposedQueryPlans(sharedQueryId);
            for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
                auto ops = decomposedQueryPlan->getRootOperators();
                if (executionNode->operator*()->getId() == WorkerId(1)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_MIGRATION);
                } else if (executionNode->operator*()->getId() == WorkerId(2)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_MIGRATION);
                } else if (executionNode->operator*()->getId() == WorkerId(3)) {
                    EXPECT_EQ(decomposedQueryPlan->getState(), QueryState::MARKED_FOR_MIGRATION);
                }
                NES_INFO("Sub Plan: {}", decomposedQueryPlan->toString());
            }
        }
    }
}
