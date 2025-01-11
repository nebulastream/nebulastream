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
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/CoordinatorHealthCheckService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/RequestHandlerService.hpp>
#include <StatisticCollection/StatisticProbeHandling/StatisticProbeHandler.hpp>
#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Util/Core.hpp>
#include <Util/FaultToleranceType.hpp>
#include <Util/TestUtils.hpp>
#include <z3++.h>

using namespace std;

namespace NES {
using namespace Configurations;

class MeerkatTest : public Testing::BaseIntegrationTest {
public:
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig1;
    WorkerConfigurationPtr workerConfig2;
    WorkerConfigurationPtr workerConfig3;
    WorkerConfigurationPtr workerConfig4;
    WorkerConfigurationPtr workerConfig5;
    std::shared_ptr<QueryParsingService> queryParsingService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    Catalogs::UDF::UDFCatalogPtr udfCatalog;
    SchemaPtr inputSchema;
    LambdaSourceTypePtr lambdaSource;
    LambdaSourceTypePtr lambdaSource1;
    TopologyPtr topology;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AdvancedFaultToleranceTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AdvancedFaultToleranceTests test class.");
    }

    void SetUp() override {
        BaseIntegrationTest::SetUp();

        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->numberOfBuffersPerEpoch = 2;
        coordinatorConfig->worker.loadBalancing = true;

        workerConfig1 = WorkerConfiguration::create();
        workerConfig1->numberOfBuffersPerEpoch = 2;
        workerConfig1->numWorkerThreads = 1;
        workerConfig1->loadBalancing = true;

        workerConfig2 = WorkerConfiguration::create();
        workerConfig2->numberOfBuffersPerEpoch = 2;
        workerConfig2->numWorkerThreads = 1;
        workerConfig2->numberOfBuffersInGlobalBufferManager = 1024;
        workerConfig2->loadBalancing = true;

        workerConfig3 = WorkerConfiguration::create();
        workerConfig3->numberOfBuffersPerEpoch = 2;
        workerConfig3->numWorkerThreads = 1;
        workerConfig3->loadBalancing = true;

        workerConfig4 = WorkerConfiguration::create();
        workerConfig4->numberOfBuffersPerEpoch = 2;
        workerConfig4->numWorkerThreads = 1;
        workerConfig4->loadBalancing = true;

        workerConfig5 = WorkerConfiguration::create();
        workerConfig5->numberOfBuffersPerEpoch = 2;
        workerConfig5->numWorkerThreads = 1;
        workerConfig5->loadBalancing = true;

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

        NesWorkerPtr wrkMid1 = std::make_shared<NesWorker>(std::move(workerConfig1));
        EXPECT_TRUE(wrkMid1->start(false, true));

        NesWorkerPtr wrkMid2 = std::make_shared<NesWorker>(std::move(workerConfig2));
        EXPECT_TRUE(wrkMid2->start(false, true));

        NesWorkerPtr wrkMid3 = std::make_shared<NesWorker>(std::move(workerConfig3));
        EXPECT_TRUE(wrkMid3->start(false, true));

        NesWorkerPtr wrkMid4 = std::make_shared<NesWorker>(std::move(workerConfig4));
        EXPECT_TRUE(wrkMid4->start(false, true));

        NesWorkerPtr wrkLeaf = std::make_shared<NesWorker>(std::move(workerConfig5));
        wrkLeaf->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource);
        EXPECT_TRUE(wrkLeaf->start(false, true));

        wrkMid3->removeParent(crd->getNesWorker()->getWorkerId());
        wrkMid3->addParent(wrkMid1->getWorkerId());

        wrkMid4->removeParent(crd->getNesWorker()->getWorkerId());
        wrkMid4->addParent(wrkMid2->getWorkerId());

        wrkLeaf->removeParent(crd->getNesWorker()->getWorkerId());
        wrkLeaf->addParent(wrkMid3->getWorkerId());
        wrkLeaf->addParent(wrkMid4->getWorkerId());

        auto query = Query::from("window").filter(Attribute("id") < 10).sink(NullOutputSinkDescriptor::create());
        QueryId qId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                                       Optimizer::PlacementStrategy::BottomUp,
                                                                                       FaultToleranceType::AS);

        auto queryCatalog = crd->getQueryCatalog();
        EXPECT_TRUE(TestUtils::waitForQueryToStart(qId, queryCatalog));
        auto sharedQueryPlanId = queryCatalog->getLinkedSharedQueryId(qId);
        // wrkMid3->requestOffload(sharedQueryPlanId, wrkMid3->getNodeEngine()->getDecomposedQueryIds(sharedQueryPlanId)[0], wrkMid1->getWorkerId());
        std::this_thread::sleep_for(std::chrono::milliseconds(100000));
        crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(qId);
        EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(qId, queryCatalog));


        EXPECT_TRUE(wrkMid4->stop(true));
        EXPECT_TRUE(wrkMid3->stop(true));
        EXPECT_TRUE(wrkMid1->stop(true));
        EXPECT_TRUE(wrkMid2->stop(true));
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
        wrkMid->requestOffload(sharedQueryPlanId, wrkMid->getNodeEngine()->getDecomposedQueryIds(sharedQueryPlanId)[0], wrkLeaf->getWorkerId());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
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

        NesWorkerPtr wrkLeaf1 = std::make_shared<NesWorker>(std::move(workerConfig2));
        wrkLeaf1->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource);
        EXPECT_TRUE(wrkLeaf1->start(false, true));

        NesWorkerPtr wrkLeaf2 = std::make_shared<NesWorker>(std::move(workerConfig1));
        wrkLeaf2->getWorkerConfiguration()->physicalSourceTypes.add(lambdaSource1);
        EXPECT_TRUE(wrkLeaf2->start(false, true));


        auto query = Query::from("window").filter(Attribute("id") < 10).sink(NullOutputSinkDescriptor::create());
        QueryId qId = crd->getRequestHandlerService()->validateAndQueueAddQueryRequest(query.getQueryPlan(),
                                                                                       Optimizer::PlacementStrategy::BottomUp,
                                                                                       FaultToleranceType::UB);

        auto queryCatalog = crd->getQueryCatalog();
        EXPECT_TRUE(TestUtils::waitForQueryToStart(qId, queryCatalog));
        std::this_thread::sleep_for(std::chrono::milliseconds(100000));
        crd->getRequestHandlerService()->validateAndQueueStopQueryRequest(qId);
        EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(qId, queryCatalog));


        EXPECT_TRUE(wrkLeaf1->stop(true));
        // EXPECT_TRUE(wrkLeaf2->stop(true));
        EXPECT_TRUE(crd->stopCoordinator(true));
    }
}

