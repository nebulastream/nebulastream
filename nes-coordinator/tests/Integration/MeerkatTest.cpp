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
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Catalogs/UDF/UDFCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Optimizer/Phases/QueryPlacementAmendmentPhase.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/StatisticCache/DefaultStatisticCache.hpp>
#include <StatisticCollection/StatisticProbeHandling/DefaultStatisticProbeGenerator.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <Util/Core.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/TestUtils.hpp>
#include <z3++.h>

using namespace std;

namespace NES {
using namespace Configurations;
using namespace Optimizer;

// ((1934 + 1) * 15 + 1) * 5
const uint64_t numberOfNodesPerLevel3 = 12;
const uint64_t numberOfNodesPerLevel2 = 4;
const uint64_t numberOfNodesPerLevel1 = 2;
const uint64_t numberOfNodes = 96;

// const uint64_t numberOfNodesPerLevel3 = 95;
// const uint64_t numberOfNodesPerLevel2 = 21;
// const uint64_t numberOfNodesPerLevel1 = 8;
// const uint64_t numberOfNodes = 16035;

//const uint64_t numberOfNodesPerLevel3 = 176;
//const uint64_t numberOfNodesPerLevel2 = 21;
//const uint64_t numberOfNodesPerLevel1 = 8;
//const uint64_t numberOfNodes = 29743;

// const uint64_t numberOfNodesPerLevel3 = 140;
// const uint64_t numberOfNodesPerLevel2 = 25;
// const uint64_t numberOfNodesPerLevel1 = 12;
// const uint64_t numberOfNodes = 42311;

class MeerkatTest : public Testing::BaseIntegrationTest {
public:
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig1;
    WorkerConfigurationPtr workerConfig2;
    WorkerConfigurationPtr workerConfig3;
    WorkerConfigurationPtr workerConfig4;
    WorkerConfigurationPtr workerConfig5;
    WorkerConfigurationPtr workerConfig6;
    WorkerConfigurationPtr workerConfig7;
    WorkerConfigurationPtr workerConfig8;
    WorkerConfigurationPtr workerConfig9;
    WorkerConfigurationPtr workerConfig10;
    WorkerConfigurationPtr workerConfig11;
    WorkerConfigurationPtr workerConfig12;
    WorkerConfigurationPtr workerConfig13;
    WorkerConfigurationPtr workerConfig14;
    WorkerConfigurationPtr workerConfig15;
    WorkerConfigurationPtr workerConfig16;
    WorkerConfigurationPtr workerConfig17;
    WorkerConfigurationPtr workerConfig18;
    std::shared_ptr<QueryParsingService> queryParsingService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    SchemaPtr inputSchema;
    LambdaSourceTypePtr lambdaSource;
    LambdaSourceTypePtr lambdaSource1;
    TopologyPtr topology;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AdvancedFaultToleranceTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AdvancedFaultToleranceTests test class.");
    }

    void SetUp() override {
        BaseIntegrationTest::SetUp();

        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->numberOfBuffersPerEpoch = 2;
        coordinatorConfig->worker.loadBalancing = 1000;

        workerConfig1 = WorkerConfiguration::create();
        workerConfig1->numberOfBuffersPerEpoch = 2;
        workerConfig1->numWorkerThreads = 1;
        workerConfig1->loadBalancing = 1000;

        workerConfig2 = WorkerConfiguration::create();
        workerConfig2->numberOfBuffersPerEpoch = 2;
        workerConfig2->numWorkerThreads = 1;
        workerConfig2->numberOfBuffersInGlobalBufferManager = 1024;
        workerConfig2->loadBalancing = 1000;

        workerConfig3 = WorkerConfiguration::create();
        workerConfig3->numberOfBuffersPerEpoch = 2;
        workerConfig3->numWorkerThreads = 1;
        workerConfig3->loadBalancing = 1000;

        workerConfig4 = WorkerConfiguration::create();
        workerConfig4->numberOfBuffersPerEpoch = 2;
        workerConfig4->numWorkerThreads = 1;
        workerConfig4->loadBalancing = 1000;

        workerConfig5 = WorkerConfiguration::create();
        workerConfig5->numberOfBuffersPerEpoch = 2;
        workerConfig5->numWorkerThreads = 1;
        workerConfig5->loadBalancing = 1000;

        workerConfig6 = WorkerConfiguration::create();
        workerConfig6->numberOfBuffersPerEpoch = 2;
        workerConfig6->numWorkerThreads = 1;
        workerConfig6->loadBalancing = 1000;

        workerConfig7 = WorkerConfiguration::create();
        workerConfig7->numberOfBuffersPerEpoch = 2;
        workerConfig7->numWorkerThreads = 1;
        workerConfig7->loadBalancing = 1000;

        workerConfig8 = WorkerConfiguration::create();
        workerConfig8->numberOfBuffersPerEpoch = 2;
        workerConfig8->numWorkerThreads = 1;
        workerConfig8->loadBalancing = 1000;

        workerConfig9 = WorkerConfiguration::create();
        workerConfig9->numberOfBuffersPerEpoch = 2;
        workerConfig9->numWorkerThreads = 1;
        workerConfig9->loadBalancing = 1000;

        workerConfig10 = WorkerConfiguration::create();
        workerConfig10->numberOfBuffersPerEpoch = 2;
        workerConfig10->numWorkerThreads = 1;
        workerConfig10->loadBalancing = 1000;

        workerConfig11 = WorkerConfiguration::create();
        workerConfig11->numberOfBuffersPerEpoch = 2;
        workerConfig11->numWorkerThreads = 1;
        workerConfig11->loadBalancing = 1000;

        workerConfig12 = WorkerConfiguration::create();
        workerConfig12->numberOfBuffersPerEpoch = 2;
        workerConfig12->numWorkerThreads = 1;
        workerConfig12->loadBalancing = 1000;

        workerConfig13 = WorkerConfiguration::create();
        workerConfig13->numberOfBuffersPerEpoch = 2;
        workerConfig13->numWorkerThreads = 1;
        workerConfig13->loadBalancing = 1000;

        workerConfig14 = WorkerConfiguration::create();
        workerConfig14->numberOfBuffersPerEpoch = 2;
        workerConfig14->numWorkerThreads = 1;
        workerConfig14->loadBalancing = 1000;

        workerConfig15 = WorkerConfiguration::create();
        workerConfig15->numberOfBuffersPerEpoch = 2;
        workerConfig15->numWorkerThreads = 1;
        workerConfig15->loadBalancing = 1000;

        workerConfig16 = WorkerConfiguration::create();
        workerConfig16->numberOfBuffersPerEpoch = 2;
        workerConfig16->numWorkerThreads = 1;
        workerConfig16->loadBalancing = 1000;

        workerConfig17 = WorkerConfiguration::create();
        workerConfig17->numberOfBuffersPerEpoch = 2;
        workerConfig17->numWorkerThreads = 1;
        workerConfig17->loadBalancing = 1000;

        workerConfig18 = WorkerConfiguration::create();
        workerConfig18->numberOfBuffersPerEpoch = 2;
        workerConfig18->numWorkerThreads = 1;
        workerConfig18->loadBalancing = 1000;

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
                records[u].value = u % 10;
                records[u].timestamp = ts;
            }
        };

        lambdaSource = LambdaSourceType::create("window", "x1", std::move(func1), 1000000, 20, GatheringMode::INGESTION_RATE_MODE);
        lambdaSource1 = LambdaSourceType::create("window", "x2", std::move(func1), 1000000, 20, GatheringMode::INGESTION_RATE_MODE);

        udfCatalog = Catalogs::UDF::UDFCatalog::create();
        topology = Topology::create();
    }

    std::vector<WorkerConfigurationPtr> generateWorkerConfigs(size_t count) {
        std::vector<WorkerConfigurationPtr> cfgs;
        for (size_t i = 0; i < count; i++) {
            auto wcfg = std::make_shared<WorkerConfiguration>();
            wcfg->numberOfBuffersPerEpoch = 2;
            wcfg->numWorkerThreads = 1;
            wcfg->loadBalancing = 1000;
            cfgs.push_back(wcfg);
        }
        return cfgs;
    }

};

TEST_F(MeerkatTest, testHandshakeProcedure) {
    NES_INFO("testHandshakeProcedure: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(false);
    EXPECT_NE(port, 0UL);

    NES_INFO("testHandshakeProcedure: Start worker1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    EXPECT_TRUE(wrk1->start(false, true));

    NES_INFO("testHandshakeProcedure: Start worker2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    EXPECT_TRUE(wrk2->start(false, true));

    wrk2->removeParent(crd->getNesWorker()->getWorkerId());
    wrk2->addParent(wrk1->getWorkerId());

    auto neighboursOfWrk1 = wrk1->getNodeEngine()->getNeighbours();
    EXPECT_EQ(neighboursOfWrk1.size(), 2UL);
    EXPECT_EQ(neighboursOfWrk1[wrk2->getWorkerId()], 1024UL);

    NES_INFO("testHandshakeProcedure: stop all");
    EXPECT_TRUE(wrk1->stop(true));
    EXPECT_TRUE(wrk2->stop(true));
    EXPECT_TRUE(crd->stopCoordinator(true));
}

TEST_F(MeerkatTest, testResourceInfoExchange) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    EXPECT_NE(crd->startCoordinator(false), 0UL);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig3));
    EXPECT_TRUE(wrk1->start(false, true));

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig4));
    EXPECT_TRUE(wrk2->start(false, true));

    wrk2->removeParent(crd->getNesWorker()->getWorkerId());
    wrk2->addParent(wrk1->getWorkerId());

    auto resources = wrk1->requestResourceInfoFromNeighbor(wrk2->getWorkerId());
    EXPECT_EQ(resources, 1024UL);

    EXPECT_TRUE(wrk1->stop(true));
    EXPECT_TRUE(wrk2->stop(true));
    EXPECT_TRUE(crd->stopCoordinator(true));
}

TEST_F(MeerkatTest, testMeerkatDiamondTopology) {

    coordinatorConfig->optimizer.enableIncrementalPlacement = true;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    EXPECT_NE(crd->startCoordinator(false), 0UL);


    size_t numIntermediates = 4;

    auto intermediateConfigs = generateWorkerConfigs(numIntermediates);

    std::vector<NesWorkerPtr> intermediates;
    intermediates.reserve(numIntermediates);
    for (size_t i = 0; i < numIntermediates; i++) {
        NesWorkerPtr w = std::make_shared<NesWorker>(std::move(intermediateConfigs[i]));
        EXPECT_TRUE(w->start(false, true));
        intermediates.push_back(w);
    }

    NesWorkerPtr wrkLeaf = std::make_shared<NesWorker>(std::move(workerConfig1));
    wrkLeaf->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource);
    EXPECT_TRUE(wrkLeaf->start(false, true));


    wrkLeaf->removeParent(crd->getNesWorker()->getWorkerId());


    size_t half = numIntermediates / 2;

    for (size_t i = 0; i < numIntermediates; i++) {
        intermediates[i]->removeParent(crd->getNesWorker()->getWorkerId());
    }


    for (size_t i = 0; i < half; i++) {
        if (i == 0) {
            intermediates[i]->addParent(crd->getNesWorker()->getWorkerId());
        } else {
            intermediates[i]->addParent(intermediates[i - 1]->getWorkerId());
        }
    }

    for (size_t i = half; i < numIntermediates; i++) {
        if (i == half) {
            intermediates[i]->addParent(crd->getNesWorker()->getWorkerId());
        } else {
            intermediates[i]->addParent(intermediates[i - 1]->getWorkerId());
        }
    }

    if (half > 0) {
        wrkLeaf->addParent(intermediates[half - 1]->getWorkerId());
    }
    if (numIntermediates > half) {
        wrkLeaf->addParent(intermediates[numIntermediates - 1]->getWorkerId());
    }

    auto query = Query::from("window").filter(Attribute("id") < 10).sink(NullOutputSinkDescriptor::create());
    // auto query = Query::from("window")
                        // .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).apply(Max(Attribute("timestamp"))).sink(PrintSinkDescriptor::create());
    QueryId qId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(
        query.getQueryPlan(),
        Optimizer::PlacementStrategy::BottomUp,
        FaultToleranceType::M
    );

    auto queryCatalog = crd->getQueryCatalog();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(qId, queryCatalog));
    auto sharedQueryPlanId = queryCatalog->getLinkedSharedQueryId(qId);

    if (intermediates.size() >= 3) {
        auto decomposedIds = intermediates[numIntermediates - 1]->getNodeEngine()->getDecomposedQueryIds(sharedQueryPlanId);
        if (!decomposedIds.empty()) {
            intermediates[numIntermediates - 1]->requestOffload(
                sharedQueryPlanId,
                decomposedIds[0].id,
                intermediates[numIntermediates - 2]->getWorkerId()
            );
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(qId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(qId, queryCatalog));

    for (auto & w : intermediates) {
        EXPECT_TRUE(w->stop(true));
    }
    EXPECT_TRUE(wrkLeaf->stop(true));
    EXPECT_TRUE(crd->stopCoordinator(true));
}

TEST_F(MeerkatTest, testMeerkatThreeWorkerTopology) {

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    EXPECT_NE(crd->startCoordinator(false), 0UL);

    NesWorkerPtr wrkMid = std::make_shared<NesWorker>(std::move(workerConfig2));
    EXPECT_TRUE(wrkMid->start(false, true));

    NesWorkerPtr wrkLeaf = std::make_shared<NesWorker>(std::move(workerConfig1));
    wrkLeaf->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource);
    EXPECT_TRUE(wrkLeaf->start(false, true));

    wrkLeaf->removeParent(crd->getNesWorker()->getWorkerId());
    wrkLeaf->addParent(wrkMid->getWorkerId());


    auto query = Query::from("window").filter(Attribute("id") < 10).sink(NullOutputSinkDescriptor::create());
    QueryId qId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                                   Optimizer::PlacementStrategy::BottomUp,
                                                                                   FaultToleranceType::M);

    auto queryCatalog = crd->getQueryCatalog();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(qId, queryCatalog));
    auto sharedQueryPlanId = queryCatalog->getLinkedSharedQueryId(qId);
    wrkMid->requestOffload(sharedQueryPlanId, wrkMid->getNodeEngine()->getDecomposedQueryIds(sharedQueryPlanId)[0].id, wrkLeaf->getWorkerId());
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(qId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(qId, queryCatalog));


    EXPECT_TRUE(wrkLeaf->stop(true));
    EXPECT_TRUE(wrkMid->stop(true));
    EXPECT_TRUE(crd->stopCoordinator(true));
}

TEST_F(MeerkatTest, testMeerkatThreeWorkerTopologyWithTwoSources) {

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    EXPECT_NE(crd->startCoordinator(false), 0UL);

    // for (auto i = 0; i < 8; i++) {

        auto workerConfig = WorkerConfiguration::create();
        workerConfig->numberOfBuffersPerEpoch = 2;
        workerConfig->numWorkerThreads = 1;
        workerConfig->loadBalancing = 1000;

        NesWorkerPtr wrkLeaf1 = std::make_shared<NesWorker>(std::move(workerConfig));
        wrkLeaf1->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource);
        EXPECT_TRUE(wrkLeaf1->start(false, true));
    // }
    auto query = Query::from("window").filter(Attribute("id") > 0).sink(NullOutputSinkDescriptor::create());
    QueryId qId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                                   Optimizer::PlacementStrategy::BottomUp,
                                                                                   FaultToleranceType::UB);

    auto queryCatalog = crd->getQueryCatalog();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(qId, queryCatalog));
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(qId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(qId, queryCatalog));


    EXPECT_TRUE(crd->stopCoordinator(true));
}

TEST_F(MeerkatTest, testDecisionTime) {
    auto topology = Topology::create();
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    uint64_t var = 1;
    auto rootNodeId = topology->registerWorker(WorkerId(var), "localhost", 123, 124, 4000000000, properties, 0, 0);
    var++;

    SchemaPtr schema = Schema::create()
                           ->addField("id", BasicType::UINT32)
                           ->addField("value", BasicType::UINT64);
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    sourceCatalog->addLogicalSource(sourceName, schema);
    LogicalSourcePtr logicalSource = sourceCatalog->getLogicalSource(sourceName);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create(sourceName, "test2");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    PhysicalSourcePtr physicalSource = PhysicalSource::create(csvSourceType);

    while (var < numberOfNodes) {
        // Create child node under the root.
        auto childNodeId = topology->registerWorker(WorkerId(var), "localhost", 123, 124, 4000000000, properties, 0, 0);
        var++;

        // Level 1: Create sub-child nodes under the child.
        for (uint64_t j = 0; j < numberOfNodesPerLevel1 && var < numberOfNodes; j++) {
            auto subChildNodeId = topology->registerWorker(WorkerId(var), "localhost", 123, 124, 4000000000, properties, 0, 0);
            topology->removeTopologyNodeAsChild(rootNodeId, subChildNodeId);
            topology->addTopologyNodeAsChild(childNodeId, subChildNodeId);
            var++;

            // Level 2: Create sub-sub-child nodes under the sub-child.
            for (uint64_t k = 0; k < numberOfNodesPerLevel2 && var < numberOfNodes; k++) {
                auto subSubChildNodeId = topology->registerWorker(WorkerId(var), "localhost", 123, 124, 4000000000, properties, 0, 0);
                topology->removeTopologyNodeAsChild(rootNodeId, subSubChildNodeId);
                topology->addTopologyNodeAsChild(subChildNodeId, subSubChildNodeId);
                var++;

                // Level 3: Create source nodes under the sub-sub-child.
                for (uint64_t l = 0; l < numberOfNodesPerLevel3 && var < numberOfNodes; l++) {
                    auto sourceNodeId = topology->registerWorker(WorkerId(var), "localhost", 123, 124, 4000000000, properties, 0, 0);
                    topology->removeTopologyNodeAsChild(rootNodeId, sourceNodeId);
                    topology->addTopologyNodeAsChild(subSubChildNodeId, sourceNodeId);
                    var++;

                    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                        Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, sourceNodeId);
                    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
                }
            }
        }
    }
    std::cout << "numberOfNodes created: " << var << std::endl;

    globalExecutionPlan = Optimizer::GlobalExecutionPlan::create();
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();
    queryPlan->setPlacementStrategy(Optimizer::PlacementStrategy::BottomUp);
    queryPlan->setFaultTolerance(FaultToleranceType::M);

    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->optimizer.performOnlySourceOperatorExpansion = true;
    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(coordinatorConfig);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto statisticRegistry = Statistic::StatisticRegistry::create();
    auto statisticProbeHandler = Statistic::StatisticProbeHandler::create(statisticRegistry,
                                                                          Statistic::DefaultStatisticProbeGenerator::create(),
                                                                          Statistic::DefaultStatisticCache::create(),
                                                                          topology);
    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, coordinatorConfig->optimizer, statisticProbeHandler);
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto sharedQueryId = sharedQueryPlan->getId();
    auto queryPlacementPhase = Optimizer::QueryPlacementAmendmentPhase::create(globalExecutionPlan,
                                                                               topology,
                                                                               typeInferencePhase,
                                                                               coordinatorConfig);
    queryPlacementPhase->execute(sharedQueryPlan);
}

TEST_F(MeerkatTest, testMeerkatThreeWorkersOffload) {

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalog()->addLogicalSource("window", inputSchema);
    EXPECT_NE(crd->startCoordinator(false), 0UL);

    auto workerConfig = WorkerConfiguration::create();
    workerConfig->numWorkerThreads = 1;

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    EXPECT_TRUE(wrk1->start(false, true));

    NesWorkerPtr wrkLeaf1 = std::make_shared<NesWorker>(std::move(workerConfig));
    wrkLeaf1->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource);
    EXPECT_TRUE(wrkLeaf1->start(false, true));

    wrkLeaf1->removeParent(crd->getNesWorker()->getWorkerId());
    wrkLeaf1->addParent(wrk1->getWorkerId());

    auto query = Query::from("window").filter(Attribute("id") > 0).sink(NullOutputSinkDescriptor::create());
    QueryId qId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                                   Optimizer::PlacementStrategy::BottomUp,
                                                                                   FaultToleranceType::NONE);

    auto queryCatalog = crd->getQueryCatalog();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(qId, queryCatalog));
    auto sharedQueryPlanId = queryCatalog->getLinkedSharedQueryId(qId);

    auto decomposedIds = wrkLeaf1->getNodeEngine()->getDecomposedQueryIds(sharedQueryPlanId);
    wrk1->requestOffload(sharedQueryPlanId, decomposedIds[0].id, wrkLeaf1->getWorkerId());
    std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(qId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(qId, queryCatalog));


    EXPECT_TRUE(crd->stopCoordinator(true));
    EXPECT_TRUE(wrkLeaf1->stop(true));
}
}
