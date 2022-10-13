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
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <cpprest/filestream.h>
#include <cpprest/http_client.h>
#include <gtest/gtest.h>
#include <thread>

using namespace utility;
using namespace std;
using namespace web;
// Common features like URIs.
using namespace web::http;
// Common HTTP functionality
using namespace web::http::client;
// HTTP client features
using namespace concurrency::streams;

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
    WorkerConfigurationPtr workerConfig4;
    WorkerConfigurationPtr workerConfig5;
    WorkerConfigurationPtr workerConfig6;
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
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->numberOfBuffersPerEpoch = 900;
        coordinatorConfig->numberOfBuffersInGlobalBufferManager = 65536;
        coordinatorConfig->numberOfBuffersInSourceLocalBufferPool = 1024;
        coordinatorConfig->numWorkerThreads = 4;
        coordinatorConfig->replicationLevel = 1;

        workerConfig1 = WorkerConfiguration::create();
        workerConfig1->numberOfBuffersPerEpoch = 900;
        workerConfig1->numberOfBuffersInSourceLocalBufferPool = 1024;
        workerConfig1->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig1->coordinatorPort = *rpcCoordinatorPort;
        workerConfig1->enableStatisticOutput = true;
        workerConfig1->numberOfBuffersToProduce = 5000000;
        workerConfig1->sourceGatheringInterval = 10;
        workerConfig1->numWorkerThreads = 4;
        workerConfig1->replicationLevel = 1;

        workerConfig2 = WorkerConfiguration::create();
        workerConfig2->numberOfBuffersPerEpoch = 900;
        workerConfig2->numberOfBuffersInSourceLocalBufferPool = 1024;
        workerConfig2->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig2->coordinatorPort = *rpcCoordinatorPort;
        workerConfig2->enableStatisticOutput = true;
        workerConfig2->numberOfBuffersToProduce = 5000000;
        workerConfig2->sourceGatheringInterval = 10;
        workerConfig2->numWorkerThreads = 4;
        workerConfig2->replicationLevel = 1;

        workerConfig3 = WorkerConfiguration::create();
        workerConfig3->numberOfBuffersPerEpoch = 900;
        workerConfig3->numWorkerThreads = 4;
        workerConfig3->numberOfBuffersInSourceLocalBufferPool = 1024;
        workerConfig3->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig3->coordinatorPort = *rpcCoordinatorPort;
        workerConfig3->enableStatisticOutput = true;
        workerConfig3->numberOfBuffersToProduce = 5000000;
        workerConfig3->sourceGatheringInterval = 10;
        workerConfig3->replicationLevel = 1;

        workerConfig4 = WorkerConfiguration::create();
        workerConfig4->numberOfBuffersPerEpoch = 900;
        workerConfig4->numWorkerThreads = 4;
        workerConfig4->numberOfBuffersInSourceLocalBufferPool = 11;
        workerConfig4->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig4->coordinatorPort = *rpcCoordinatorPort;
        workerConfig4->enableStatisticOutput = true;
        workerConfig4->numberOfBuffersToProduce = 5000000;
        workerConfig4->sourceGatheringInterval = 10;
        workerConfig4->replicationLevel = 1;

        workerConfig5 = WorkerConfiguration::create();
        workerConfig5->numberOfBuffersPerEpoch = 10;
        workerConfig5->numWorkerThreads = 4;
        workerConfig5->numberOfBuffersInSourceLocalBufferPool = 1024;
        workerConfig5->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig5->coordinatorPort = *rpcCoordinatorPort;
        workerConfig5->enableStatisticOutput = true;
        workerConfig5->numberOfBuffersToProduce = 5000000;
        workerConfig5->sourceGatheringInterval = 10;

        workerConfig6 = WorkerConfiguration::create();
        workerConfig6->numberOfBuffersPerEpoch = 10;
        workerConfig6->numWorkerThreads = 4;
        workerConfig6->numberOfBuffersInSourceLocalBufferPool = 1024;
        workerConfig6->numberOfBuffersInGlobalBufferManager = 65536;
        workerConfig6->coordinatorPort = *rpcCoordinatorPort;
        workerConfig6->enableStatisticOutput = true;
        workerConfig6->numberOfBuffersToProduce = 5000000;
        workerConfig6->sourceGatheringInterval = 10;

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
                          ->addField("timestamp1", DataTypeFactory::createUInt64())
                          ->addField("timestamp2", DataTypeFactory::createUInt64());
    }
};

/*
 * @brief test data buffering at all nodes
 */
TEST_F(UpstreamBackupTest, DISABLED_testDataBufferingAtAllNodes) {
    NES_INFO("UpstreamBackupTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("window", inputSchema);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("UpstreamBackupTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("UpstreamBackupTest: Start worker 1");
    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeFinite);
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
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::AT_LEAST_ONCE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    EXPECT_TRUE(wrk1->getNodeEngine()->getBufferStorage()->getStorageSize() == numberOfTupleBuffers);
    EXPECT_TRUE(wrk2->getNodeEngine()->getBufferStorage()->getStorageSize() == numberOfTupleBuffers);
    wrk1->getNodeEngine()->getBufferStorage()->trimBuffer(queryId, timestamp);
    wrk2->getNodeEngine()->getBufferStorage()->trimBuffer(queryId, timestamp);

    NES_INFO("UpstreamBackupTest: Remove query");
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
 * @brief test timestamp of watermark processor
 */
TEST_F(UpstreamBackupTest, DISABLED_testTimestampWatermarkProcessor) {
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
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

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
    queryService->validateAndQueueStopRequest(queryId);
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
TEST_F(UpstreamBackupTest, DISABLED_testMessagePassingSinkCoordinatorSources) {
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
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

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
    queryService->validateAndQueueStopRequest(queryId);
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
 * @brief test if data is deleted after propagate epoch is called
 */
TEST_F(UpstreamBackupTest, DISABLED_testTrimmingAfterPropagateEpoch) {
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
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    //insert tuple buffers to check later if they are trimmed
    for (uint64_t i = 0; i < numberOfTupleBuffers; i++) {
        auto buffer = bufferManager->getUnpooledBuffer(timestamp);
        buffer->setWatermark(timestamp + i);
        buffer->setOriginId(1);
        buffer->setSequenceNumber(1 + i);
        wrk1->getNodeEngine()->getBufferStorage()->insertBuffer(queryId, 0, buffer.value());
        wrk2->getNodeEngine()->getBufferStorage()->insertBuffer(queryId, 0, buffer.value());
    }
    EXPECT_TRUE(wrk1->getNodeEngine()->getBufferStorage()->getStorageSize() == numberOfTupleBuffers);
    EXPECT_TRUE(wrk2->getNodeEngine()->getBufferStorage()->getStorageSize() == numberOfTupleBuffers);

    //get sink
    auto querySubPlanIds = crd->getNodeEngine()->getSubQueryIds(queryId);
    for (auto querySubPlanId : querySubPlanIds) {
        auto sinks = crd->getNodeEngine()->getExecutableQueryPlan(querySubPlanId)->getSinks();
        for (auto& sink : sinks) {
            sink->notifyEpochTermination(timestamp + numberOfTupleBuffers);
        }
    }
    //check if buffers got trimmed
    EXPECT_TRUE(wrk1->getNodeEngine()->getBufferStorage()->getStorageSize() == 0);
    EXPECT_TRUE(wrk2->getNodeEngine()->getBufferStorage()->getStorageSize() == 0);

    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
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
//    auto physicalSource1 = PhysicalSource::create("window", "x1", csvSourceTypeInfinite);
//    workerConfig1->physicalSources.add(physicalSource1);


//    workerConfig1->lambdaSource = 3;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UpstreamBackupTest: Worker1 started successfully");


//    workerConfig2->lambdaSource = 2;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UpstreamBackupTest: Worker2 started successfully");

    workerConfig3->lambdaSource = 2;
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("UpstreamBackupTest: Worker3 started successfully");



    crd->getTopologyManagerService()->removeParent(4,1);
    crd->getTopologyManagerService()->addParent(4,2);
    crd->getTopologyManagerService()->addParent(4,3);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testUpstreamBackup.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("UpstreamBackupTest: Submit query");
    string query =
        "Query::from(\"A\").sink(NullOutputSinkDescriptor::create());";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::AT_MOST_ONCE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    wrk3->getNodeEngine()->updateNetworkSink(2, workerConfig2->localWorkerIp, workerConfig2->dataPort, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000000));
    NES_INFO("UpstreamBackupTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
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
}// namespace NES