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
#include "../util/NesBaseTest.hpp"

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;
class MultipleWindowsTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultipleWindowsTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultipleWindowsTest test class.");
    }
};

TEST_F(MultipleWindowsTest, testTwoCentralTumblingWindows) {
    auto coordinatorConfig = CoordinatorConfiguration::create();
    auto workerConfig = WorkerConfiguration::create();
    auto srcConf = CSVSourceType::create();

    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    workerConfig->setCoordinatorPort(*rpcCoordinatorPort);

    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,2000,1,1\n"
                             "0,2000,4,1\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoDistributedTumblingWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    coordinatorConfig->setNumberOfSlots(12);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,2000,1,2\n"
                             "0,2000,4,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(MultipleWindowsTest, testTwoCentralSlidingWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    coordinatorConfig->setNumberOfSlots(12);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query =
        "Query::from(\"window\")"
        ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(5),Seconds(5)))"
        ".byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\")))"
        ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"start\"), Milliseconds(0), Milliseconds()))"
        ".window(SlidingWindow::of(EventTime(Attribute(\"start\")),Seconds(10),Seconds(5))) "
        ".byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\")))"
        ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,51\n"
                             "5000,15000,1,95\n"
                             "0,10000,4,1\n"
                             "0,10000,11,5\n"
                             "0,10000,12,1\n"
                             "0,10000,16,2\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(MultipleWindowsTest, testTwoDistributedSlidingWindowEventTime) {
    auto coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType2->setNumberOfBuffersToProduce(1);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(5),Seconds(5))).byKey(Attribute(\"id\")"
                   ").apply(Sum(Attribute(\"value\")))"
                   ".window(SlidingWindow::of(EventTime(Attribute(\"end\")),Seconds(10),Seconds(5))).byKey(Attribute(\"id\")). "
                   "apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(","CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,10000,1,32\n"
                             "5000,15000,1,102\n"
                             "10000,20000,1,190\n"
                             "0,10000,4,2\n"
                             "5000,15000,4,2\n"
                             "0,10000,11,10\n"
                             "5000,15000,11,10\n"
                             "0,10000,12,2\n"
                             "5000,15000,12,2\n"
                             "0,10000,16,4\n"
                             "5000,15000,16,4\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoCentralTumblingAndSlidingWindows) {
    auto coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(SlidingWindow::of(EventTime(Attribute("start")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "16000,17000,1,33\n"
                             "15500,16500,1,33\n"
                             "14000,15000,1,29\n"
                             "13500,14500,1,29\n"
                             "12000,13000,1,25\n"
                             "11500,12500,1,25\n"
                             "10000,11000,1,21\n"
                             "9500,10500,1,21\n"
                             "8000,9000,1,17\n"
                             "7500,8500,1,17\n"
                             "6000,7000,1,13\n"
                             "5500,6500,1,13\n"
                             "4000,5000,1,9\n"
                             "3500,4500,1,9\n"
                             "2000,3000,1,11\n"
                             "1500,2500,1,11\n"
                             "0,1000,1,1\n"
                             "0,1000,4,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoDistributedTumblingAndSlidingWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(3);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "0,2000,1,8\n"
                             "0,2000,4,4\n"
                             "0,2000,11,4\n"
                             "0,2000,12,4\n"
                             "0,2000,16,4\n"
                             "2000,4000,1,48\n"
                             "4000,6000,1,40\n"
                             "2000,4000,11,16\n"
                             "2000,4000,16,4\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

/**
 * @brief Test all three windows in a row
 */
TEST_F(MultipleWindowsTest, testThreeDifferentWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(10);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("MultipleWindowsTest: Submit query");

    NES_DEBUG("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute("start"), Milliseconds(0), Milliseconds()))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$value:INTEGER\n"
                             "0,2000,6\n"
                             "2000,4000,24\n"
                             "4000,6000,20\n"
                             "6000,8000,28\n"
                             "8000,10000,36\n"
                             "10000,12000,44\n"
                             "12000,14000,52\n"
                             "14000,16000,60\n"
                             "16000,18000,68\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleWindowsTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleWindowsTest, DISABLED_testSeparatedWindow) {
    /**
     * @Ankit this should lso not happen, node 2 has a slot count of 3 but 5 operators are deployed
     * ExecutionNode(id:1, ip:127.0.0.1, topologyId:1)
| QuerySubPlan(queryId:1, querySubPlanId:4)
|  SINK(6)
|    SOURCE(17,)
|--ExecutionNode(id:2, ip:127.0.0.1, topologyId:2)
|  | QuerySubPlan(queryId:1, querySubPlanId:3)
|  |  SINK(18)
|  |    CENTRALWINDOW(9)
|  |      WATERMARKASSIGNER(4)
|  |        WindowComputationOperator(10)
|  |          SOURCE(13,)
|  |          SOURCE(15,)
|  |--ExecutionNode(id:3, ip:127.0.0.1, topologyId:3)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:1)
|  |  |  SINK(14)
|  |  |    SliceCreationOperator(11)
|  |  |      WATERMARKASSIGNER(2)
|  |  |        SOURCE(1,window)
|  |--ExecutionNode(id:4, ip:127.0.0.1, topologyId:4)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:2)
|  |  |  SINK(16)
|  |  |    SliceCreationOperator(12)
|  |  |      WATERMARKASSIGNER(7)
|  |  |        SOURCE(8,window)
     */
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(3);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setSourceFrequency(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 3");

    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    workerConfig3->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType3->setSourceFrequency(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    auto physicalSource3 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("MultipleWindowsTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleWindowsTest: Submit query");

    string query = R"(Query::from("window")
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleWindowsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleWindowsTest: Test finished");
}

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(MultipleWindowsTest, DISABLED_testNotVaildQuery) {
    /**
     * @Ankit this plan should not happen, it spearates the slicer from the source
     * ExecutionNode(id:1, ip:127.0.0.1, topologyId:1)
| QuerySubPlan(queryId:1, querySubPlanId:5)
|  SINK(8)
|    CENTRALWINDOW(12)
|      WATERMARKASSIGNER(6)
|        FILTER(5)
|          WindowComputationOperator(13)
|            SOURCE(18,)
|            SOURCE(22,)
|--ExecutionNode(id:2, ip:127.0.0.1, topologyId:2)
|  | QuerySubPlan(queryId:1, querySubPlanId:2)
|  |  SINK(19)
|  |    SliceCreationOperator(14)
|  |      SOURCE(16,)
|  | QuerySubPlan(queryId:1, querySubPlanId:4)
|  |  SINK(23)
|  |    SliceCreationOperator(15)
|  |      SOURCE(20,)
|  |--ExecutionNode(id:3, ip:127.0.0.1, topologyId:3)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:1)
|  |  |  SINK(17)
|  |  |    WATERMARKASSIGNER(3)
|  |  |      FILTER(2)
|  |  |        SOURCE(1,window)
|  |--ExecutionNode(id:4, ip:127.0.0.1, topologyId:4)
|  |  | QuerySubPlan(queryId:1, querySubPlanId:3)
|  |  |  SINK(21)
|  |  |    WATERMARKASSIGNER(9)
|  |  |      FILTER(11)
|  |  |        SOURCE(10,window)
     */
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", testSchema);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumberOfSlots(3);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setSourceFrequency(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    auto physicalSource2 = PhysicalSource::create("window", "test_stream", csvSourceType2);
    workerConfig2->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 3");

    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->setCoordinatorPort(port);
    workerConfig3->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType3->setSourceFrequency(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    auto physicalSource3 = PhysicalSource::create("window", "test_stream", csvSourceType3);
    workerConfig3->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("MultipleWindowsTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleWindowsTest: Submit query");

    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleWindowsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleWindowsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleWindowsTest: Test finished");
}

}// namespace NES
