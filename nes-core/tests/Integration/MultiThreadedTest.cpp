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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

uint64_t rpcPort = 4000;
uint64_t restPort = 8081;
uint64_t numberOfWorkerThreads = 2;
uint64_t numberOfCoordinatorThreads = 2;

class MultiThreadedTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultiWorkerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultiWorkerTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    static void TearDownTestCase() { NES_INFO("Tear down MultiWorkerTest class."); }
};

TEST_F(MultiThreadedTest, testFilterQuery) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("stream", stream);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setNumberOfBuffersToProduce(210);
    csvSourceType1->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("stream", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog(); /*register logical stream qnv*/

    std::string outputFilePath = "MultiThreadedTest_testFilterQuery.out";

    NES_INFO("MultiThreadedTest: Submit query");
    string query = R"(Query::from("stream")
        .filter(Attribute("value") < 2)
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "stream$value:INTEGER,stream$id:INTEGER,stream$timestamp:INTEGER\n"
                             "1,1,1000\n"
                             "1,12,1001\n"
                             "1,4,1002\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultiThreadedTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultiThreadedTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultiThreadedTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultiThreadedTest: Test finished");
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(MultiThreadedTest, testProjectQuery) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("stream", stream);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setNumberOfBuffersToProduce(210);
    csvSourceType1->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("stream", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog(); /*register logical stream qnv*/

    std::string outputFilePath = "MultiThreadedTest_testProjectQuery.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream")
        .filter(Attribute("value") < 2)
        .project(Attribute("id"))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "stream$id:INTEGER\n"
                             "1\n"
                             "12\n"
                             "4\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
    int response = remove(outputFilePath.c_str());
    EXPECT_TRUE(response == 0);
}

TEST_F(MultiThreadedTest, testCentralWindowEventTime) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", stream);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setSourceFrequency(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    csvSourceType1->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\")."
                   "window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))\n"
                   "        .byKey(Attribute(\"id\")).apply(Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window$start:INTEGER,window$end:INTEGER,window$id:INTEGER,window$value:INTEGER\n"
                             "1000,2000,1,1\n"
                             "2000,3000,1,2\n"
                             "1000,2000,4,1\n"
                             "2000,3000,11,2\n"
                             "1000,2000,12,1\n"
                             "2000,3000,16,2\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * This test only test if there is something crash but not the result
 */
TEST_F(MultiThreadedTest, testMultipleWindows) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setNumberOfSlots(12);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", stream);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    csvSourceType1->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(1)))
        .byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")),Seconds(2)))
        .byKey(Attribute("id")).apply(Sum(Attribute("value")))
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

TEST_F(MultiThreadedTest, testMultipleWindowsCrashTest) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setNumberOfSlots(12);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window", stream);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(12);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window", "test_stream", csvSourceType1);
    workerConfig1->addPhysicalSource(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Seconds(1)))
        .byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .window(TumblingWindow::of(EventTime(Attribute("start")),Seconds(2)))
        .byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    sleep(5);

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
 * Test deploying join with different three sources
 */
TEST_F(MultiThreadedTest, DISABLED_testOneJoin) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setNumberOfSlots(16);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", stream);
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(8);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig1->addPhysicalSource(physicalSource1);
    workerConfig1->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
                             "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,3,4,1102\n"
                             "1000,2000,4,1,4,1002,3,4,1112\n"
                             "1000,2000,12,1,12,1001,5,12,1011\n"
                             "2000,3000,1,2,1,2000,2,1,2010\n"
                             "2000,3000,11,2,11,2001,2,11,2301\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

TEST_F(MultiThreadedTest, DISABLED_test2Joins) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setNumberOfSlots(16);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", stream);
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);
    std::string window3 =
        R"(Schema::create()->addField(createField("win3", INT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(8);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setSourceFrequency(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setSourceFrequency(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setSourceFrequency(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    workerConfig1->addPhysicalSource(physicalSource1);
    workerConfig1->addPhysicalSource(physicalSource2);
    workerConfig1->addPhysicalSource(physicalSource3);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    std::string outputFilePath = "testTwoJoinsWithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    /**
  * @brief 1000,2000,4,1000,2000,4,3,4,1102,4,4,1001,1,4,1002\n"
        "1000,2000,4,1000,2000,4,3,4,1112,4,4,1001,1,4,1002\n"
        "1000,2000,12,1000,2000,12,5,12,1011,1,12,1300,1,12,1001
  */
    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,4,4,1001,1,4,1002,3,4,1102\n"
        "1000,2000,4,1000,2000,4,4,4,1001,1,4,1002,3,4,1112\n"
        "1000,2000,12,1000,2000,12,1,12,1300,1,12,1001,5,12,1011\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultiThreadedTest, DISABLED_threeJoins) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setNumberOfSlots(16);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", stream);
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);
    std::string window3 =
        R"(Schema::create()->addField(createField("win3", INT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window3", window3);
    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window4", window4);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(8);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setSourceFrequency(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setSourceFrequency(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType3->setSourceFrequency(1);
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType4 = CSVSourceType::create();
    csvSourceType4->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window4.csv");
    csvSourceType4->setSourceFrequency(1);
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    csvSourceType4->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    auto physicalSource3 = PhysicalSource::create("window3", "test_stream", csvSourceType3);
    auto physicalSource4 = PhysicalSource::create("window4", "test_stream", csvSourceType4);
    workerConfig1->addPhysicalSource(physicalSource1);
    workerConfig1->addPhysicalSource(physicalSource2);
    workerConfig1->addPhysicalSource(physicalSource3);
    workerConfig1->addPhysicalSource(physicalSource4);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    std::string outputFilePath = "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3"), Attribute("id1"), Attribute("id3"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window4"), Attribute("id1"), Attribute("id4"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,"
        "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:"
        "INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:"
        "INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:"
        "INTEGER\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,4,4,1001,4,4,1001,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1000,2000,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,500,1500,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1000,2000,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,500,1500,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1000,2000,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,500,1500,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1000,2000,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,500,1500,12,1,12,1300,1,12,1300,1,12,1300,1,12,1300\n"
        "12000,13000,1,12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "12000,13000,1,12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "12000,13000,1,11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "12000,13000,1,11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "11500,12500,1,12000,13000,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "11500,12500,1,12000,13000,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "11500,12500,1,11500,12500,1,12000,13000,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "11500,12500,1,11500,12500,1,11500,12500,1,12,1,12000,12,1,12000,12,1,12000,12,1,12000\n"
        "13000,14000,1,13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "13000,14000,1,13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "13000,14000,1,12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "13000,14000,1,12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "12500,13500,1,13000,14000,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "12500,13500,1,13000,14000,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "12500,13500,1,12500,13500,1,13000,14000,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "12500,13500,1,12500,13500,1,12500,13500,1,13,1,13000,13,1,13000,13,1,13000,13,1,13000\n"
        "3000,4000,11,3000,4000,11,3000,4000,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "3000,4000,11,3000,4000,11,2500,3500,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "3000,4000,11,2500,3500,11,3000,4000,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "3000,4000,11,2500,3500,11,2500,3500,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "2500,3500,11,3000,4000,11,3000,4000,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "2500,3500,11,3000,4000,11,2500,3500,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "2500,3500,11,2500,3500,11,3000,4000,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n"
        "2500,3500,11,2500,3500,11,2500,3500,11,9,11,3000,9,11,3000,9,11,3000,9,11,3000\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}
/**
 * Test deploying join with different three sources
 */
TEST_F(MultiThreadedTest, DISABLED_joinCrashTest) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setNumberOfSlots(16);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumWorkerThreads(numberOfCoordinatorThreads);
    NES_INFO("MultiThreadedTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");
    //register logical stream
    std::string stream =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window1", stream);
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("window2", window2);
    NES_DEBUG("MultiThreadedTest: Coordinator started successfully");

    NES_DEBUG("MultiThreadedTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setNumWorkerThreads(numberOfWorkerThreads);
    workerConfig1->setNumberOfSlots(8);
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setSourceFrequency(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setSkipHeader(false);
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setSourceFrequency(0);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setSkipHeader(false);
    auto physicalSource1 = PhysicalSource::create("window1", "test_stream", csvSourceType1);
    auto physicalSource2 = PhysicalSource::create("window2", "test_stream", csvSourceType2);
    workerConfig1->addPhysicalSource(physicalSource1);
    workerConfig1->addPhysicalSource(physicalSource2);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultiThreadedTest: Worker1 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    sleep(5);
    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

}// namespace NES