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

#include <gtest/gtest.h>
#include <Util/TestHarness/TestHarness.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class AssignWatermarkTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("AssignWatermarkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AssignWatermarkTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down AssignWatermarkTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

/*
 * @brief test event time watermark for central tumbling window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentCentralTumblingWindow) {
    //Setup Coordinator
    std::string window = R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))
                                            ->addField(createField("timestamp", UINT64));)";
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    crd->getStreamCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    workerConfig = WorkerConfiguration::create();
    workerConfig->setCoordinatorPort(rpcPort);
    CSVSourceTypePtr srcConf = CSVSourceType::create();
    srcConf->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(4);
    auto windowStream = PhysicalSource::create("window", "test_stream", srcConf);
    workerConfig->addPhysicalSource(windowStream);

    // register physical stream with 4 buffers, each contains 3 tuples (12 tuples in total)
    // window-out-of-order.csv contains 12 rows
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testWatermarkAssignmentCentralTumblingWindow.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,12\n"
                             "2000,3000,1,24\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for distributed tumbling window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentDistributedTumblingWindow) {
    //Setup Coordinator
    std::string window = R"(Schema::create()->addField(createField("value", UINT64))
                                            ->addField(createField("id", UINT64))
                                            ->addField(createField("timestamp", UINT64));)";
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    crd->getStreamCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    crd->getStreamCatalogService()->registerLogicalSource("window", window);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(rpcPort);
    //Add Source To Worker
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(4);
    auto windowStream = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig1->addPhysicalSource(windowStream);
    //Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker 1 started successfully");
    //Setup Worker 2
    NES_INFO("AssignWatermarkTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setRpcPort(port + 20);
    workerConfig2->setDataPort(port + 21);
    workerConfig2->addPhysicalSource(windowStream);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig2);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AssignWatermarkTest: Worker 2 started successfully");
    //Setup Worker 3
    NES_INFO("AssignWatermarkTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    workerConfig3->setRpcPort(port + 30);
    workerConfig3->setDataPort(port + 31);
    workerConfig3->addPhysicalSource(windowStream);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(workerConfig3);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AssignWatermarkTest: Worker 3 started successfully");

    std::string outputFilePath = "testWatermarkAssignmentDistributedTumblingWindow.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("AssignWatermarkTest: Submit query");

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,36\n"
                             "2000,3000,1,72\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for central sliding window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentCentralSlidingWindow) {
    //Setup Coordinator
    std::string window = R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))
                                            ->addField(createField("timestamp", UINT64));)";
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    crd->getStreamCatalogService()->registerLogicalSource("window", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
    workerConfig = WorkerConfiguration::create();
    workerConfig->setCoordinatorPort(rpcPort);
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    //Add Source to Worker
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(4);
    auto windowStream = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig->addPhysicalSource(windowStream);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testWatermarkAssignmentCentralSlidingWindow.out";
    remove(outputFilePath.c_str());

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50), "
                   "Milliseconds()))"
                   ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1),Milliseconds(500))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "2500,3500,1,10\n"
                             "2000,3000,1,24\n"
                             "1500,2500,1,30\n"
                             "1000,2000,1,12\n"
                             "500,1500,1,6\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

/*
 * @brief test event time watermark for distributed sliding window with 50 ms allowed lateness
 */
TEST_F(AssignWatermarkTest, testWatermarkAssignmentDistributedSlidingWindow) {
    //Setup Coordinator
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))
                           ->addField(createField("timestamp", UINT64));)";
    CoordinatorConfigurationPtr crdConf = CoordinatorConfiguration::create();
    crdConf = CoordinatorConfiguration::create();
    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    NES_INFO("AssignWatermarkTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    crd->getStreamCatalogService()->registerLogicalSource("window", window);
    EXPECT_NE(port, 0UL);
    NES_INFO("AssignWatermarkTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("AssignWatermarkTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1->setCoordinatorPort(rpcPort);
    //Add Source to Worker
    CSVSourceTypePtr sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window-out-of-order.csv");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(4);
    auto windowStream1 = PhysicalSource::create("window", "test_stream", sourceConfig);
    workerConfig1->addPhysicalSource(windowStream1);
    //Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AssignWatermarkTest: Worker 1 started successfully");

    //Setup Worker 2
    NES_INFO("AssignWatermarkTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setRpcPort(port + 20);
    workerConfig2->setDataPort(port + 21);
    workerConfig2->addPhysicalSource(windowStream1);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig2);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AssignWatermarkTest: Worker 2 started successfully");

    NES_INFO("AssignWatermarkTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    workerConfig3->setRpcPort(port + 30);
    workerConfig3->setDataPort(port + 31);
    workerConfig3->addPhysicalSource(windowStream1);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(workerConfig3);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AssignWatermarkTest: Worker 3 started successfully");

    std::string outputFilePath = "testWatermarkAssignmentDistributedSlidingWindow.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("AssignWatermarkTest: Submit query");

    // The query contains a watermark assignment with 50 ms allowed lateness
    NES_INFO("AssignWatermarkTest: Submit query");
    string query =
        "Query::from(\"window\")"
        ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(50),Milliseconds()))"
        ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1),Milliseconds(500)))"
        ".byKey(Attribute(\"id\"))"
        ".apply(Sum(Attribute(\"value\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "500,1500,1,18\n"
                             "1000,2000,1,36\n"
                             "1500,2500,1,90\n"
                             "2000,3000,1,72\n"
                             "2500,3500,1,30\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("AssignWatermarkTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("AssignWatermarkTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("AssignWatermarkTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("AssignWatermarkTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("AssignWatermarkTest: Test finished");
}

}// namespace NES
