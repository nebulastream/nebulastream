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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/Identifiers.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Optimizer/Phases/QueryRewritePhase.hpp>
#include <Optimizer/Phases/TopologySpecificQueryRewritePhase.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Optimizer/Phases/SignatureInferencePhase.hpp>
#include <Optimizer/Phases/TypeInferencePhase.hpp>
#include <Services/QueryService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using namespace std;
namespace NES {

using namespace Configurations;
const int timestamp = 1644426604;
const uint64_t numberOfTupleBuffers = 4;

// ((1934 + 1) * 15 + 1) * 5
//const uint64_t numberOfNodesPerLevel3 = 12;
//const uint64_t numberOfNodesPerLevel2 = 4;
//const uint64_t numberOfNodesPerLevel1 = 2;
//const uint64_t numberOfNodes = 95;

//const uint64_t numberOfNodesPerLevel3 = 95;
//const uint64_t numberOfNodesPerLevel2 = 21;
//const uint64_t numberOfNodesPerLevel1 = 8;
//const uint64_t numberOfNodes = 16035;

//const uint64_t numberOfNodesPerLevel3 = 176;
//const uint64_t numberOfNodesPerLevel2 = 21;
//const uint64_t numberOfNodesPerLevel1 = 8;
//const uint64_t numberOfNodes = 29743;

const uint64_t numberOfNodesPerLevel3 = 140;
const uint64_t numberOfNodesPerLevel2 = 25;
const uint64_t numberOfNodesPerLevel1 = 12;
const uint64_t numberOfNodes = 42311;


class UpstreamBackupTest : public Testing::NESBaseTest {
  public:
    std::string ipAddress = "127.0.0.1";
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig1;
    WorkerConfigurationPtr workerConfig2;
    WorkerConfigurationPtr workerConfig3;
    WorkerConfigurationPtr workerConfig4;
    WorkerConfigurationPtr workerConfig5;
    WorkerConfigurationPtr workerConfig6;
    WorkerConfigurationPtr workerConfig7;
    CSVSourceTypePtr csvSourceTypeInfinite;
    CSVSourceTypePtr csvSourceTypeFinite;
    SchemaPtr inputSchema;
    Runtime::BufferManagerPtr bufferManager;
    QueryParsingServicePtr queryParsingService;
    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::TypeInferencePhasePtr typeInferencePhase;
    std::shared_ptr<Catalogs::UDF::UdfCatalog> udfCatalog;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("UpstreamBackupTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UpstreamBackupTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        udfCatalog = Catalogs::UDF::UdfCatalog::create();

        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->numberOfBuffersPerEpoch = 100;
        coordinatorConfig->numberOfBuffersInGlobalBufferManager = 65536;
        coordinatorConfig->numberOfBuffersInSourceLocalBufferPool = 1024;
        coordinatorConfig->numWorkerThreads = 4;
        coordinatorConfig->bufferSizeInBytes = 131072;
        coordinatorConfig->numberOfSlots = 20;
        coordinatorConfig->memoryCapacity = 0;
        coordinatorConfig->networkCapacity = 0;
        coordinatorConfig->mtbfValue = 386228;
        coordinatorConfig->launchTime = 1652692028;

        workerConfig1 = WorkerConfiguration::create();
        workerConfig1->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig1->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig1->coordinatorPort = *rpcCoordinatorPort;
        workerConfig1->enableStatisticOutput = true;
        workerConfig1->numberOfBuffersToProduce = 5000000;
        workerConfig1->sourceGatheringInterval = 100;
        workerConfig1->numWorkerThreads = 4;
        workerConfig1->numberOfBuffersPerEpoch = 4;
        workerConfig1->bufferSizeInBytes = 131072;
        workerConfig1->numberOfSlots = 20;
        workerConfig1->memoryCapacity = 350;
        workerConfig1->networkCapacity = 100;
        workerConfig1->mtbfValue = 55000;
        workerConfig1->launchTime = 1652692028;
        workerConfig1->ingestionRate = 900;

        workerConfig2 = WorkerConfiguration::create();
        workerConfig2->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig2->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig2->coordinatorPort = *rpcCoordinatorPort;
        workerConfig2->enableStatisticOutput = true;
        workerConfig2->numberOfBuffersToProduce = 5000000;
        workerConfig2->sourceGatheringInterval = 100;
        workerConfig2->numWorkerThreads = 4;
        workerConfig2->numberOfBuffersPerEpoch = 4;
        workerConfig2->bufferSizeInBytes = 131072;
        workerConfig2->numberOfSlots = 20;
        workerConfig2->memoryCapacity = 20000;
        workerConfig2->networkCapacity = 20000;
        workerConfig2->mtbfValue = 50000;
        workerConfig2->launchTime = 1652692028;
        workerConfig2->ingestionRate = 900;

        workerConfig3 = WorkerConfiguration::create();
        workerConfig3->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig3->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig3->coordinatorPort = *rpcCoordinatorPort;
        workerConfig3->enableStatisticOutput = true;
        workerConfig3->numberOfBuffersToProduce = 5000000;
        workerConfig3->numWorkerThreads = 4;
        workerConfig3->numberOfBuffersPerEpoch = 100;
        workerConfig3->bufferSizeInBytes = 131072;
        workerConfig3->numberOfSlots = 2;
        workerConfig3->memoryCapacity = 350;
        workerConfig3->networkCapacity = 100;
        workerConfig3->mtbfValue = 15000;
        workerConfig3->launchTime = 1652692028;
        workerConfig3->ingestionRate = 900;

        workerConfig4 = WorkerConfiguration::create();
        workerConfig4->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig4->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig4->coordinatorPort = *rpcCoordinatorPort;
        workerConfig4->enableStatisticOutput = true;
        workerConfig4->numberOfBuffersToProduce = 5000000;
        workerConfig4->numWorkerThreads = 4;
        workerConfig4->numberOfBuffersPerEpoch = 100;
        workerConfig4->bufferSizeInBytes = 131072;
        workerConfig4->numberOfSlots = 2;
        workerConfig4->memoryCapacity = 350;
        workerConfig4->networkCapacity = 100;
        workerConfig4->mtbfValue = 15000;
        workerConfig4->launchTime = 1652692028;
        workerConfig4->ingestionRate = 900;

        workerConfig5 = WorkerConfiguration::create();
        workerConfig5->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig5->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig5->coordinatorPort = *rpcCoordinatorPort;
        workerConfig5->enableStatisticOutput = true;
        workerConfig5->numberOfBuffersToProduce = 5000000;
        workerConfig5->numWorkerThreads = 4;
        workerConfig5->numberOfBuffersPerEpoch = 100;
        workerConfig5->bufferSizeInBytes = 131072;
        workerConfig5->numberOfSlots = 2;
        workerConfig5->memoryCapacity = 350;
        workerConfig5->networkCapacity = 100;
        workerConfig5->mtbfValue = 15000;
        workerConfig5->launchTime = 1652692028;
        workerConfig5->ingestionRate = 900;

        workerConfig6 = WorkerConfiguration::create();
        workerConfig6->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig6->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig6->coordinatorPort = *rpcCoordinatorPort;
        workerConfig6->enableStatisticOutput = true;
        workerConfig6->numberOfBuffersToProduce = 5000000;
        workerConfig6->numWorkerThreads = 4;
        workerConfig6->numberOfBuffersPerEpoch = 100;
        workerConfig6->bufferSizeInBytes = 131072;
        workerConfig6->numberOfSlots = 2;
        workerConfig6->memoryCapacity = 350;
        workerConfig6->networkCapacity = 100;
        workerConfig6->mtbfValue = 15000;
        workerConfig6->launchTime = 1652692028;
        workerConfig6->ingestionRate = 900;

        workerConfig7 = WorkerConfiguration::create();
        workerConfig7->numberOfBuffersInSourceLocalBufferPool = 1024;
        workerConfig7->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig7->coordinatorPort = *rpcCoordinatorPort;
        workerConfig7->enableStatisticOutput = true;
        workerConfig7->numberOfBuffersToProduce = 5000000;
        workerConfig7->numWorkerThreads = 4;
        workerConfig7->numberOfBuffersPerEpoch = 100;
        workerConfig7->bufferSizeInBytes = 131072;
        workerConfig7->numberOfSlots = 2;
        workerConfig7->memoryCapacity = 350;
        workerConfig7->networkCapacity = 100;
        workerConfig7->mtbfValue = 15000;
        workerConfig7->launchTime = 1652692028;
        workerConfig7->sourceGatheringInterval = 900;

        csvSourceTypeInfinite = CSVSourceType::create();
        csvSourceTypeInfinite->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
        csvSourceTypeInfinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers);
        csvSourceTypeInfinite->setNumberOfBuffersToProduce(0);

        csvSourceTypeFinite = CSVSourceType::create();
        csvSourceTypeFinite->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
        csvSourceTypeFinite->setNumberOfTuplesToProducePerBuffer(numberOfTupleBuffers - 1);
        csvSourceTypeFinite->setNumberOfBuffersToProduce(numberOfTupleBuffers);

        inputSchema = Schema::create()
                          ->addField("a", DataTypeFactory::createUInt64())
                          ->addField("b", DataTypeFactory::createUInt64())
                          ->addField("c", DataTypeFactory::createUInt64())
                          ->addField("d", DataTypeFactory::createUInt64())
                          ->addField("e", DataTypeFactory::createUInt64())
                          ->addField("f", DataTypeFactory::createUInt64())
//                          ->addField("g", DataTypeFactory::createUInt64())
//                          ->addField("h", DataTypeFactory::createUInt64())
//                          ->addField("i", DataTypeFactory::createUInt64())
//                          ->addField("j", DataTypeFactory::createUInt64())
//                          ->addField("k", DataTypeFactory::createUInt64())
//                          ->addField("l", DataTypeFactory::createUInt64())
//                          ->addField("m", DataTypeFactory::createUInt64())
//                          ->addField("n", DataTypeFactory::createUInt64())
//                          ->addField("o", DataTypeFactory::createUInt64())
//                          ->addField("p", DataTypeFactory::createUInt64())
//                          ->addField("q", DataTypeFactory::createUInt64())
//                          ->addField("r", DataTypeFactory::createUInt64())
//                          ->addField("s", DataTypeFactory::createUInt64())
//                          ->addField("t", DataTypeFactory::createUInt64())
//                          ->addField("u", DataTypeFactory::createUInt64())
//                          ->addField("v", DataTypeFactory::createUInt64())
//                          ->addField("w", DataTypeFactory::createUInt64())
//                          ->addField("x", DataTypeFactory::createUInt64())
                          ->addField("timestamp1", DataTypeFactory::createUInt64())
                          ->addField("timestamp2", DataTypeFactory::createUInt64());
    }
};

/*
 * @brief test timestamp of watermark processor
 */
TEST_F(UpstreamBackupTest, testTimestampWatermarkProcessor) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
    workerConfig1->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"window\").sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    auto querySubPlanIds = crd->getNodeEngine()->getSubQueryIds(queryId);
    for (auto querySubPlanId : querySubPlanIds) {
        auto sinks = crd->getNodeEngine()->getExecutableQueryPlan(querySubPlanId)->getSinks();
        for (auto& sink : sinks) {
            auto buffer1 = bufferManager->getUnpooledBuffer(timestamp);
            buffer1->setWatermark(timestamp);
            buffer1->setSequenceNumber(1);
            sink->updateWatermark(buffer1.value());
            auto buffer2 = bufferManager->getUnpooledBuffer(timestamp);
            buffer2->setWatermark(timestamp);
            buffer2->setOriginId(1);
            buffer2->setSequenceNumber(1);
            sink->updateWatermark(buffer2.value());
            auto currentTimestamp = sink->getCurrentEpochBarrier();
            while (currentTimestamp == 0) {
                NES_INFO("UpstreamBackupTest: current timestamp: " << currentTimestamp);
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                currentTimestamp = sink->getCurrentEpochBarrier();
            }
            EXPECT_TRUE(currentTimestamp == timestamp);
        }
    }

    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}
/*
 * @brief test message passing between sink-coordinator-sources
 */
TEST_F(UpstreamBackupTest, testMessagePassingSinkCoordinatorSources) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
    workerConfig1->physicalSources.add(physicalSource1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"window\").sink(FileSinkDescriptor::create(\"" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    //get sink
    auto querySubPlanIds = crd->getNodeEngine()->getSubQueryIds(queryId);
    for (auto querySubPlanId : querySubPlanIds) {
        auto sinks = crd->getNodeEngine()->getExecutableQueryPlan(querySubPlanId)->getSinks();
        for (auto& sink : sinks) {
            sink->notifyEpochTermination(timestamp);
        }
    }

    auto currentTimestamp = crd->getReplicationService()->getCurrentEpochBarrier(queryId);
    while (currentTimestamp == -1) {
        NES_INFO("UpstreamBackupTest: current timestamp: " << currentTimestamp);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        currentTimestamp = crd->getReplicationService()->getCurrentEpochBarrier(queryId);
    }

    //check if the method was called
    EXPECT_TRUE(currentTimestamp == timestamp);

    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}

/*
 * @brief test if upstream backup doesn't fail
 */
TEST_F(UpstreamBackupTest, testUpstreamBackupTest) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("A", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    //    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeFinite);
    //    workerConfig1->physicalSources.add(physicalSource1);

//    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
//    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
//    EXPECT_TRUE(retStart1);
//    NES_INFO("UpstreamBackupTest: Worker1 started successfully");
//
//    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
//    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
//    EXPECT_TRUE(retStart2);
//    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

//    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig2));
//    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
//    EXPECT_TRUE(retStart3);
//    NES_INFO("UpstreamBackupTest: Worker1 started successfully");
//
//    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig2));
//    bool retStart4 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
//    EXPECT_TRUE(retStart4);
//    NES_INFO("UpstreamBackupTest: Worker2 started successfully");
//
//    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(std::move(workerConfig2));
//    bool retStart5 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
//    EXPECT_TRUE(retStart5);
//    NES_INFO("UpstreamBackupTest: Worker1 started successfully");
//
//    NesWorkerPtr wrk6 = std::make_shared<NesWorker>(std::move(workerConfig2));
//    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
//    EXPECT_TRUE(retStart6);
//    NES_INFO("UpstreamBackupTest: Worker2 started successfully");
//


    workerConfig7->lambdaSource = 1;
    NesWorkerPtr wrk7 = std::make_shared<NesWorker>(std::move(workerConfig7));
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

//    crd->getTopologyManagerService()->removeParent(3, 1);
//    crd->getTopologyManagerService()->removeParent(4, 1);
//    crd->getTopologyManagerService()->removeParent(5, 1);
//    crd->getTopologyManagerService()->removeParent(6, 1);
//    crd->getTopologyManagerService()->removeParent(7, 1);
//    crd->getTopologyManagerService()->removeParent(8, 1);
//    crd->getTopologyManagerService()->addParent(3, 2);
//    crd->getTopologyManagerService()->addParent(4, 3);
//    crd->getTopologyManagerService()->addParent(5, 4);
//    crd->getTopologyManagerService()->addParent(6, 5);
//    crd->getTopologyManagerService()->addParent(7, 6);
//    crd->getTopologyManagerService()->addParent(8, 7);

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
//    string query = "Query::from(\"A\").sink(NullOutputSinkDescriptor::create());";

    string query = "Query::from(\"A\").sink(NullOutputSinkDescriptor::create());";


    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    "BottomUp",
                                                                    FaultToleranceType::AT_LEAST_ONCE,
                                                                    LineageType::IN_MEMORY,
                                                                    FaultTolerancePlacement::NONE);



//    queryId = queryService->validateAndQueueAddQueryRequest(query1,
//                                                                    "BottomUp",
//                                                                    FaultToleranceType::AT_LEAST_ONCE,
//                                                                    LineageType::IN_MEMORY,
//                                                                    FaultTolerancePlacement::MFTPH);
//
//    queryId = queryService->validateAndQueueAddQueryRequest(query1,
//                                                            "BottomUp",
//                                                            FaultToleranceType::AT_LEAST_ONCE,
//                                                            LineageType::IN_MEMORY,
//                                                            FaultTolerancePlacement::MFTPH);


    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
//    crd->getReplicationService()->resendDataToAllSources(queryId);
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk7, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::this_thread::sleep_for(std::chrono::milliseconds(10000000));
    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk1);

    //    NES_INFO("UpstreamBackupTest: Stop worker 2");
    //    bool retStopWrk2 = wrk2->stop(true);
    //    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}

TEST_F(UpstreamBackupTest, testDecisionTime) {
    auto topology = Topology::create();
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    uint64_t var = 1;
    TopologyNodePtr rootNode = TopologyNode::create(var++, "localhost", 123, 124, 4, properties);
    rootNode->addNodeProperty("tf_installed", true);
    rootNode->addNodeProperty("slots", 100);
    rootNode->addNodeProperty("reliability", 100);
    topology->setAsRoot(rootNode);

    std::string schema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
    const std::string sourceName = "car";

    sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    sourceCatalog->addLogicalSource(sourceName, schema);
    auto logicalSource = sourceCatalog->getLogicalSource(sourceName);

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    auto physicalSource = PhysicalSource::create(sourceName, "test2", csvSourceType);

    while (var < numberOfNodes) {
        TopologyNodePtr childNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
        childNode->addNodeProperty("tf_installed", true);
        topology->addNewTopologyNodeAsChild(rootNode, childNode);
        LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
        childNode->addLinkProperty(rootNode, linkProperty);
        childNode->addNodeProperty("slots", 100);
        childNode->addNodeProperty("reliability", 100);
        for (uint64_t j = 0; j < numberOfNodesPerLevel1; j++) {
            TopologyNodePtr subChildNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
            subChildNode->addNodeProperty("tf_installed", true);
            topology->addNewTopologyNodeAsChild(childNode, subChildNode);
            LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
            subChildNode->addLinkProperty(childNode, linkProperty);
            subChildNode->addNodeProperty("slots", 100);
            subChildNode->addNodeProperty("reliability", 100);
            for (uint64_t k = 0; k < numberOfNodesPerLevel2; k++) {
                TopologyNodePtr subSubChildNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
                subSubChildNode->addNodeProperty("tf_installed", true);
                topology->addNewTopologyNodeAsChild(subChildNode, subSubChildNode);
                LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
                subSubChildNode->addLinkProperty(subChildNode, linkProperty);
                subSubChildNode->addNodeProperty("slots", 100);
                subSubChildNode->addNodeProperty("reliability", 100);
                for (uint64_t l = 0; l < numberOfNodesPerLevel3; l++) {
                    TopologyNodePtr sourceNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
                    sourceNode->addNodeProperty("tf_installed", true);
                    topology->addNewTopologyNodeAsChild(subSubChildNode, sourceNode);
                    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
                    sourceNode->addLinkProperty(subSubChildNode, linkProperty);
                    sourceNode->addNodeProperty("slots", 100);
                    sourceNode->addNodeProperty("reliability", 100);

                    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
                        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);


                    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);
                }
            }
        }
    }
    std::cout << "numberOfNodes" << var;

//    TopologyNodePtr childNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
//    childNode->addNodeProperty("tf_installed", true);
//    topology->addNewTopologyNodeAsChild(rootNode, childNode);
//    LinkPropertyPtr linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
//    childNode->addLinkProperty(rootNode, linkProperty);
//    childNode->addNodeProperty("slots", 100);
//    childNode->addNodeProperty("reliability", 100);
//
//    TopologyNodePtr subChildNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
//    subChildNode->addNodeProperty("tf_installed", true);
//    topology->addNewTopologyNodeAsChild(childNode, subChildNode);
//    linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
//    subChildNode->addLinkProperty(childNode, linkProperty);
//    subChildNode->addNodeProperty("slots", 100);
//    subChildNode->addNodeProperty("reliability", 100);
//
//    TopologyNodePtr subSubChildNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
//    subSubChildNode->addNodeProperty("tf_installed", true);
//    topology->addNewTopologyNodeAsChild(subChildNode, subSubChildNode);
//    linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
//    subSubChildNode->addLinkProperty(subChildNode, linkProperty);
//    subSubChildNode->addNodeProperty("slots", 100);
//    subSubChildNode->addNodeProperty("reliability", 100);
//
//    TopologyNodePtr sourceNode = TopologyNode::create(var++, "localhost", 123, 124, 300, properties);
//    sourceNode->addNodeProperty("tf_installed", true);
//    topology->addNewTopologyNodeAsChild(subSubChildNode, sourceNode);
//    linkProperty = std::make_shared<LinkProperty>(LinkProperty(512, 100));
//    sourceNode->addLinkProperty(subSubChildNode, linkProperty);
//    sourceNode->addNodeProperty("slots", 100);
//    sourceNode->addNodeProperty("reliability", 100);

//    Catalogs::Source::SourceCatalogEntryPtr sourceCatalogEntry =
//        std::make_shared<Catalogs::Source::SourceCatalogEntry>(physicalSource, logicalSource, sourceNode);


//    sourceCatalog->addPhysicalSource(sourceName, sourceCatalogEntry);

    globalExecutionPlan = GlobalExecutionPlan::create();
    typeInferencePhase = Optimizer::TypeInferencePhase::create(sourceCatalog, udfCatalog);

    Query query = Query::from("car").filter(Attribute("id") < 45).sink(PrintSinkDescriptor::create());
    QueryPlanPtr queryPlan = query.getQueryPlan();

    auto queryReWritePhase = Optimizer::QueryRewritePhase::create(false);
    queryPlan = queryReWritePhase->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);
//    queryPlan->setFaultTolerancePlacement(FaultTolerancePlacement::NAIVE);

    auto topologySpecificQueryRewrite =
        Optimizer::TopologySpecificQueryRewritePhase::create(topology, sourceCatalog, Configurations::OptimizerConfiguration());
    topologySpecificQueryRewrite->execute(queryPlan);
    typeInferencePhase->execute(queryPlan);

    auto sharedQueryPlan = SharedQueryPlan::create(queryPlan);
    auto queryId = sharedQueryPlan->getSharedQueryId();
    auto queryPlacementPhase = Optimizer::QueryPlacementPhase::create(globalExecutionPlan, topology, typeInferencePhase, false);

    queryPlacementPhase->execute(NES::PlacementStrategy::BottomUp, sharedQueryPlan);
}
}// namespace NES