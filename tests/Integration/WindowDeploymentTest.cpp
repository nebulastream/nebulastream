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

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class WindowDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("WindowDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WindowDeploymentTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down WindowDeploymentTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerCentralTumblingWindowQueryEventTimeForExdra) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register physical stream
    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/exdra.csv", 1, 0, 1, "test_stream", "exdra", true);

    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query =
        "Query::from(\"exdra\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"metadata_generated\")), "
        "Seconds(10)), Sum(Attribute(\"features_properties_capacity\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,features_properties_capacity:INTEGER\n"
                             "1262343610000,1262343620000,1,736\n"
                             "1262343620000,1262343630000,2,1348\n"
                             "1262343630000,1262343640000,3,4575\n"
                             "1262343640000,1262343650000,4,1358\n"
                             "1262343650000,1262343660000,5,1288\n"
                             "1262343660000,1262343670000,6,3458\n"
                             "1262343670000,1262343680000,7,1128\n"
                             "1262343680000,1262343690000,8,1079\n"
                             "1262343690000,1262343700000,9,2071\n"
                             "1262343700000,1262343710000,10,2632\n";

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

TEST_F(WindowDeploymentTest, testCentralWindowEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

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
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query =
        "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
        "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
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
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralSlidingWindowEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

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
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 0, 1, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), "
                   "SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), "
                   "Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
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
                             "10000,20000,1,870\n"
                             "5000,15000,1,570\n"
                             "0,10000,1,307\n"
                             "0,10000,4,6\n"
                             "0,10000,11,30\n"
                             "0,10000,12,7\n"
                             "0,10000,16,12\n";

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
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedTumblingWindowQueryEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "window", true);
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "1000,2000,1,34\n"
                             "2000,3000,2,56\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed sliding window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerDistributedSlidingWindowQueryEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

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
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 0, 1, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), "
                   "SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), "
                   "Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    ifstream my_file("outputLog.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("outputLog.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
                             "5000,15000,1,1140\n"
                             "0,10000,1,614\n"
                             "0,10000,4,12\n"
                             "0,10000,11,60\n"
                             "0,10000,12,14\n"
                             "0,10000,16,24\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "windowStream", true);

    wrk1->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "start:INTEGER,end:INTEGER,value:INTEGER\n"
                             "1000,2000,3\n"
                             "2000,3000,6\n";

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
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

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
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 0, 1, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)),"
                   " Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
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

    string expectedContent = "start:INTEGER,end:INTEGER,value:INTEGER\n"
                             "10000,20000,870\n"
                             "5000,15000,570\n"
                             "0,10000,362\n";

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
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeyTumblingWindowEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "windowStream", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "start:INTEGER,end:INTEGER,value:INTEGER\n"
                             "1000,2000,6\n"
                             "2000,3000,12\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeySlidingWindowEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

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
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 0, 1, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath = "outputLog.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    //size slide
    string query = "Query::from(\"window\").window(SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)),"
                   " Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "start:INTEGER,end:INTEGER,value:INTEGER\n"
                             "5000,15000,1140\n"
                             "0,10000,724\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testCentralWindowIngestionTimeIngestionTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

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
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 5, 3, 3, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    ASSERT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

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

TEST_F(WindowDeploymentTest, testDistributedWindowIngestionTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 13, port + 15, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

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

    //register physical stream
    PhysicalStreamConfigPtr conf70 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 5, 3, 3, "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath = "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

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
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowIngestionTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 6, 3, "test_stream", "windowStream", true);

    wrk1->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

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
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDistributedNonKeyTumblingWindowIngestionTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("windowStream", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "windowStream", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);

    std::string outputFilePath = "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"windowStream\").window(TumblingWindow::of(IngestionTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\",\"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

/**
 * @brief test distributed tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployDistributedWithMergingTumblingWindowQueryEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11,  NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21,  NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);//id=3
    wrk2->replaceParent(1, 2);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 3");
    NesWorkerPtr wrk3 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    wrk3->replaceParent(1, 2);

    EXPECT_TRUE(retStart3);
    NES_INFO("WindowDeploymentTest: Worker 3 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 4");
    NesWorkerPtr wrk4 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 40, port + 41, NodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    wrk4->replaceParent(1, 2);
    EXPECT_TRUE(retStart4);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 5");
    NesWorkerPtr wrk5 =
        std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 60, port + 61, NodeType::Sensor);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    wrk5->replaceParent(1, 2);
    EXPECT_TRUE(retStart5);
    NES_INFO("WindowDeploymentTest: Worker 6 started successfully");

    std::string outputFilePath = "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema =
        R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 3, "test_stream", "window", true);
//    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);
    wrk3->registerPhysicalStream(conf);
    wrk4->registerPhysicalStream(conf);
    wrk5->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
//    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
//    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 4));
//    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    string expectedContent = "start:INTEGER,end:INTEGER,cnt:INTEGER,id:INTEGER,value:INTEGER\n"
                             "1000,2000,3,1,17\n"
                             "2000,3000,3,2,28\n";


    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("WindowDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("WindowDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("WindowDeploymentTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

}// namespace NES
