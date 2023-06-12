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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
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

class UpstreamBackupTest : public Testing::NESBaseTest {
  public:
    std::string ipAddress = "127.0.0.1";
    CoordinatorConfigurationPtr coordinatorConfig;
    WorkerConfigurationPtr workerConfig1;
    WorkerConfigurationPtr workerConfig2;
    WorkerConfigurationPtr workerConfig3;
    CSVSourceTypePtr csvSourceTypeInfinite;
    CSVSourceTypePtr csvSourceTypeFinite;
    SchemaPtr inputSchema;
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("UpstreamBackupTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UpstreamBackupTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();

        bufferManager = std::make_shared<Runtime::BufferManager>(1024, 1);

        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->numberOfBuffersPerEpoch = 4;
        coordinatorConfig->numberOfBuffersInGlobalBufferManager = 65536;
        coordinatorConfig->numberOfBuffersInSourceLocalBufferPool = 1024;
        coordinatorConfig->numWorkerThreads = 4;
        coordinatorConfig->bufferSizeInBytes = 131072;
        coordinatorConfig->numberOfSlots = 0;
        coordinatorConfig->worker.memoryCapacity = 0;
        coordinatorConfig->worker.networkCapacity = 0;
        coordinatorConfig->worker.mtbfValue = 386228;
        coordinatorConfig->worker.launchTime = 1652692028;

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
        workerConfig1->numberOfSlots = 2;
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
        workerConfig2->numberOfSlots = 2;
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
        workerConfig3->numberOfBuffersPerEpoch = 4;
        workerConfig3->bufferSizeInBytes = 131072;
        workerConfig3->numberOfSlots = 2;
        workerConfig3->memoryCapacity = 350;
        workerConfig3->networkCapacity = 100;
        workerConfig3->mtbfValue = 15000;
        workerConfig3->launchTime = 1652692028;
        workerConfig3->ingestionRate = 900;

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
                          ->addField("g", DataTypeFactory::createUInt64())
                          ->addField("h", DataTypeFactory::createUInt64())
                          ->addField("i", DataTypeFactory::createUInt64())
                          ->addField("j", DataTypeFactory::createUInt64())
                          ->addField("k", DataTypeFactory::createUInt64())
                          ->addField("l", DataTypeFactory::createUInt64())
                          ->addField("m", DataTypeFactory::createUInt64())
                          ->addField("n", DataTypeFactory::createUInt64())
                          ->addField("o", DataTypeFactory::createUInt64())
                          ->addField("p", DataTypeFactory::createUInt64())
                          ->addField("q", DataTypeFactory::createUInt64())
                          ->addField("r", DataTypeFactory::createUInt64())
                          ->addField("s", DataTypeFactory::createUInt64())
                          ->addField("t", DataTypeFactory::createUInt64())
                          ->addField("u", DataTypeFactory::createUInt64())
                          ->addField("v", DataTypeFactory::createUInt64())
                          ->addField("w", DataTypeFactory::createUInt64())
                          ->addField("x", DataTypeFactory::createUInt64())
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

    workerConfig3->lambdaSource = 1;
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

//    crd->getTopologyManagerService()->removeParent(4, 1);
//    crd->getTopologyManagerService()->removeParent(3, 1);
//    crd->getTopologyManagerService()->addParent(3, 2);
//    crd->getTopologyManagerService()->addParent(4, 3);

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query = "Query::from(\"A\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp1\")), "
                   "Seconds(1))).byKey(Attribute(\"a\")).apply(Sum(Attribute(\"b\"))).sink(NullOutputSinkDescriptor::create());";

    string query1 = "Query::from(\"A\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp1\")), "
                   "Seconds(1))).byKey(Attribute(\"a\")).apply(Sum(Attribute(\"b\"))).sink(NullOutputSinkDescriptor::create());";


    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    "BottomUp",
                                                                    FaultToleranceType::AT_LEAST_ONCE,
                                                                    LineageType::IN_MEMORY,
                                                                    FaultTolerancePlacement::MFTP);

    queryId = queryService->validateAndQueueAddQueryRequest(query1,
                                                                    "BottomUp",
                                                                    FaultToleranceType::AT_LEAST_ONCE,
                                                                    LineageType::IN_MEMORY,
                                                                    FaultTolerancePlacement::MFTP);

    queryId = queryService->validateAndQueueAddQueryRequest(query1,
                                                            "BottomUp",
                                                            FaultToleranceType::AT_LEAST_ONCE,
                                                            LineageType::IN_MEMORY,
                                                            FaultTolerancePlacement::MFTP);


    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::this_thread::sleep_for(std::chrono::milliseconds(1000000));
    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopQueryRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UpstreamBackupTest: Stop worker 1");
    bool retStopWrk1 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk1);

    //    NES_INFO("UpstreamBackupTest: Stop worker 2");
    //    bool retStopWrk2 = wrk2->stop(true);
    //    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UpstreamBackupTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UpstreamBackupTest: Test finished");
}
}// namespace NES