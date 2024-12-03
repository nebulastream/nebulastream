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

#include <API/Expressions/Expressions.hpp>
#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Operators/LogicalOperators/LogicalMapOperator.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources//SourceLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/LogicalSourceDescriptor.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <Util/Core.hpp>
#include <Util/QuerySignatureContext.hpp>
#include <Util/TestUtils.hpp>
#include <z3++.h>

using namespace NES;

using namespace Configurations;
const uint64_t numberOfBuffersToProduceInTuples = 1000000;
const uint64_t ingestionRate = 20;

class ActiveStandbyTest : public Testing::BaseIntegrationTest {

  public:
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig1;
    WorkerConfigurationPtr workerConfig2;
    WorkerConfigurationPtr workerConfig3;
    std::shared_ptr<QueryParsingService> queryParsingService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    TopologyPtr topology;
    SchemaPtr inputSchema;
    LambdaSourceTypePtr lambdaSource;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Statistic::StatisticProbeHandlerPtr statisticProbeHandler;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
                NES::Logger::setupLogging("ActiveStandby.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup ActiveStandbyTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        udfCatalog = Catalogs::UDF::UDFCatalog::create();

        auto statisticRegistry = Statistic::StatisticRegistry::create();
        auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(statisticRegistry,
                                                                              Statistic::DefaultStatisticProbeGenerator::create(),
                                                                              Statistic::DefaultStatisticCache::create(),
                                                                              topology);

        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->numberOfBuffersPerEpoch = 2;

        workerConfig1 = WorkerConfiguration::create();
        workerConfig1->numWorkerThreads = 1;
        workerConfig1->numberOfBuffersPerEpoch = 2;

        workerConfig2 = WorkerConfiguration::create();
        workerConfig2->numWorkerThreads = 1;
        workerConfig2->numberOfBuffersPerEpoch = 2;

        workerConfig3 = WorkerConfiguration::create();
        workerConfig3->numWorkerThreads = 1;
        workerConfig3->numberOfBuffersPerEpoch = 2;

        inputSchema = Schema::create()
                  ->addField("id", DataTypeFactory::createUInt64())
                  ->addField("value", DataTypeFactory::createUInt64())
                  ->addField("timestamp", DataTypeFactory::createUInt64());

        auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t id;
                uint64_t value;
                uint64_t timestamp;
            };

            auto* records = buffer.getBuffer<Record>();
            auto ts = time(nullptr);
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].id = u;
                //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                records[u].value = u % 10;
                records[u].timestamp = ts;
            }
        };

        lambdaSource = LambdaSourceType::create("window",
                                        "x1",
                                        std::move(func1),
                                        numberOfBuffersToProduceInTuples,
                                        ingestionRate,
                                        GatheringMode::INGESTION_RATE_MODE);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup ActiveStandbyTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down ActiveStandbyTest test class." << std::endl; }

    void setupTopologyAndSourceCatalogSimpleShortDiamond() {
        topology = Topology::create();

        std::map<std::string, std::any> properties;
        properties[NES::Worker::Properties::MAINTENANCE] = false;
        properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;

        auto rootNodeId = WorkerId(4);
        topology->registerWorker(rootNodeId, "localhost", 123, 124, 5, properties, 0, 0);

        auto middleNode1Id = WorkerId(3);
        auto middleNode2Id = WorkerId(2);
        topology->registerWorker(middleNode1Id, "localhost", 123, 124, 10, properties, 0, 0, middleNode2Id);
        topology->addTopologyNodeAsChild(rootNodeId, middleNode1Id);

        topology->registerWorker(middleNode2Id, "localhost", 123, 124, 10, properties, 0, 0, middleNode1Id);
        topology->addTopologyNodeAsChild(rootNodeId, middleNode2Id);

        auto sourceNodeId = WorkerId(1);
        topology->registerWorker(sourceNodeId, "localhost", 123, 124, 10, properties, 0, 0);
        topology->removeTopologyNodeAsChild(rootNodeId, sourceNodeId);
        topology->addTopologyNodeAsChild(middleNode1Id, sourceNodeId);
        topology->addTopologyNodeAsChild(middleNode2Id, sourceNodeId);

        auto carSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
        const std::string carSourceName = "car";

        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
        sourceCatalog->addLogicalSource(carSourceName, carSchema);
        auto logicalSource = sourceCatalog->getLogicalSource(carSourceName);

        CSVSourceTypePtr csvSourceTypeForCar1 = CSVSourceType::create(carSourceName, "carPhysicalSourceName1");
        csvSourceTypeForCar1->setGatheringInterval(10);
        csvSourceTypeForCar1->setNumberOfTuplesToProducePerBuffer(10);

        auto physicalSourceForCar1 = PhysicalSource::create(csvSourceTypeForCar1);

        auto sourceCatalogEntry1 =
            Catalogs::Source::SourceCatalogEntry::create(physicalSourceForCar1, logicalSource, sourceNodeId);

        sourceCatalog->addPhysicalSource(carSourceName, sourceCatalogEntry1);

        globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
        typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);
    }
};

TEST_F(ActiveStandbyTest, testActiveStandbyDeployment) {
    setupTopologyAndSourceCatalogSimpleShortDiamond();

    auto sourceOperator = LogicalOperatorFactory::createSourceOperator(LogicalSourceDescriptor::create("car"));

    SinkDescriptorPtr printSinkDescriptor = PrintSinkDescriptor::create();
    auto sinkOperator = LogicalOperatorFactory::createSinkOperator(printSinkDescriptor);
    auto mapOperator = LogicalOperatorFactory::createMapOperator(Attribute("id") = 10);

    sinkOperator->addChild(mapOperator);
    mapOperator->addChild(sourceOperator);

    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    queryPlan->setFaultTolerance(FaultToleranceType::AS);
    queryPlan->addRootOperator(sinkOperator);

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

    ASSERT_EQ(executionNodes.size(), 4U);
    for (const auto& executionNode : executionNodes) {
        auto executionNodeUnlocked = executionNode->operator*();
        if (executionNodeUnlocked->getId().getRawValue() == 1) {
            auto querySubPlans = executionNodeUnlocked->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 2U);
            EXPECT_TRUE(actualRootOperators[0]->instanceOf<SinkLogicalOperator>());
            ASSERT_EQ(actualRootOperators[0]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[0]->getChildren()[0]->instanceOf<SourceLogicalOperator>());

            EXPECT_TRUE(actualRootOperators[1]->instanceOf<SinkLogicalOperator>());
            ASSERT_EQ(actualRootOperators[1]->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperators[1]->getChildren()[0]->instanceOf<SourceLogicalOperator>());
        } else if (executionNodeUnlocked->getId().getRawValue() == 3) {
            // map should be placed on cloud node
            auto querySubPlans = executionNodeUnlocked->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 1U);
        } else if (executionNodeUnlocked->getId().getRawValue() == 2) {
            auto querySubPlans = executionNodeUnlocked->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            EXPECT_TRUE(actualRootOperator->instanceOf<SinkLogicalOperator>());
            ASSERT_EQ(actualRootOperator->getChildren().size(), 1U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<LogicalMapOperator>());
            ASSERT_EQ(actualRootOperator->getChildren()[0]->getChildren().size(), 1U);
        } else {
            auto querySubPlans = executionNodeUnlocked->getAllDecomposedQueryPlans(sharedQueryId);
            ASSERT_EQ(querySubPlans.size(), 1U);
            auto querySubPlan = querySubPlans[0U];
            std::vector<OperatorPtr> actualRootOperators = querySubPlan->getRootOperators();
            ASSERT_EQ(actualRootOperators.size(), 1U);
            OperatorPtr actualRootOperator = actualRootOperators[0];
            ASSERT_EQ(actualRootOperator->getChildren().size(), 2U);
            EXPECT_TRUE(actualRootOperator->getChildren()[0]->instanceOf<SourceLogicalOperator>());
            EXPECT_TRUE(actualRootOperator->getChildren()[1]->instanceOf<SourceLogicalOperator>());
        }
    }
}
/*
 * @brief test full lifecycle of active standby
 */
TEST_F(ActiveStandbyTest, testActiveStandbyRun) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    //Setup Source
    NES_INFO("UpstreamBackupTest: Start worker 3");
    workerConfig3->physicalSourceTypes.add(lambdaSource);

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");

    wrk3->removeParent(crd->getNesWorker()->getWorkerId());
    wrk3->addParent(wrk2->getWorkerId());
    wrk3->addParent(wrk1->getWorkerId());

    auto queryCatalog = crd->getQueryCatalog();
    auto requestHandlerService = crd->getRequestHandlerService();


    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    auto query = Query::from("window").filter(Attribute("id") > 10).sink(NullOutputSinkDescriptor::create());

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                             Optimizer::PlacementStrategy::BottomUp,
                                                                             FaultToleranceType::AS);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("UpstreamBackupTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}
