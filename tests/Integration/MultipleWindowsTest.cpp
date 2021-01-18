/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <iostream>

using namespace std;

namespace NES {

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class MultipleWindowsTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultipleWindowsTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultipleWindowsTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down MultipleWindowsTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

TEST_F(MultipleWindowsTest, testTwoCentralTumblingWindows) {
    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MultipleWindowsTest: Coordinator started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleWindowsTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)), Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")" +
        outputFilePath  + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "0,2000,1,1\n"
                             "0,2000,4,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}


TEST_F(MultipleWindowsTest, testTwoDistributedTumblingWindows) {
    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MultipleWindowsTest: Coordinator started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleWindowsTest: Worker1 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleWindowsTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)), Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")" +
        outputFilePath  + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "0,2000,1,2\n"
                             "0,2000,4,2\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MultipleWindowsTest: Coordinator started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleWindowsTest: Worker 1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 0, 1, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(5),Seconds(5)), Sum(Attribute(\"value\")))"
                   ".windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"start\")),Seconds(10),Seconds(5)), Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("wakeup");

    ifstream my_file("outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "0,10000,1,307\n"
                             "0,10000,4,6\n"
                             "0,10000,11,30\n"
                             "0,10000,12,7\n"
                             "0,10000,16,12\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MultipleWindowsTest: Coordinator started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleWindowsTest: Worker 1 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleWindowsTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 0, 1, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = "Query::from(\"window\")"
                   ".windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(5),Seconds(5)), Sum(Attribute(\"value\")))"
                   ".windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"end\")),Seconds(10),Seconds(5)), Sum(Attribute(\"value\")))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_DEBUG("wakeup");

    ifstream my_file("outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "0,10000,1,194\n"
                             "0,10000,4,12\n"
                             "0,10000,11,60\n"
                             "0,10000,12,14\n"
                             "0,10000,16,24\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MultipleWindowsTest: Coordinator started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleWindowsTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 10, 3, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)), Sum(Attribute("value")))
        .filter(Attribute("id") < 10)
        .windowByKey(Attribute("id"), SlidingWindow::of(EventTime(Attribute("start")),Seconds(1),Milliseconds(500)), Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")" +
        outputFilePath  + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
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

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MultipleWindowsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MultipleWindowsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MultipleWindowsTest: Test finished");
}

TEST_F(MultipleWindowsTest, testTwoDistributedTumblingAndSlidingWindows) {
    NES_INFO("MultipleWindowsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("MultipleWindowsTest: Coordinator started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleWindowsTest: Worker1 started successfully");

    NES_INFO("MultipleWindowsTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleWindowsTest: Worker2 started successfully");


    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 5, 3, "test_stream", "window", false);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("MultipleWindowsTest: Submit query");
    string query = R"(Query::from("window")
        .windowByKey(Attribute("id"), SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)), Sum(Attribute("value")))
        .windowByKey(Attribute("id"), TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)), Sum(Attribute("value")))
        .sink(FileSinkDescriptor::create(")" +
        outputFilePath  + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "0,2000,1,8\n"
                             "2000,4000,1,48\n"
                             "4000,6000,1,40\n"
                             "0,2000,4,4\n"
                             "0,2000,11,4\n"
                             "2000,4000,11,16\n"
                             "0,2000,12,4\n"
                             "0,2000,16,4\n"
                             "2000,4000,16,4\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("MultipleWindowsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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


}// namespace NES
