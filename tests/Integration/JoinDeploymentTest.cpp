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

class JoinDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("JoinDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup JoinDeploymentTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down JoinDeploymentTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

/**
 * Test deploying merge query with source on two different worker node using top down strategy.
 */
//TODO: this test will be enabled once we have the renaming function using as
//TODO: prevent self join
TEST_F(JoinDeploymentTest, DISABLED_testSelfJoinTumblingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream2", "window", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window").as("w1").join(Query::from("window").as("w2"), Attribute("id"), Attribute("id"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "_$start:INTEGER,_$end:INTEGER,key:INTEGER,w1$value:INTEGER,w1$id:INTEGER,w1$timestamp:INTEGER,"
                             "w2$value:INTEGER,w2$id:INTEGER,w2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,1,4,1002\n"
                             "1000,2000,12,1,12,1001,1,12,1001\n"
                             "2000,3000,1,2,1,2000,2,1,2000\n"
                             "2000,3000,11,2,11,2001,2,11,2001\n"
                             "2000,3000,16,2,16,2002,2,16,2002\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
* Test deploying join with same data and same schema
 * */
TEST_F(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id"), Attribute("id"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$value:INTEGER,window1$id:INTEGER,window1$timestamp:INTEGER,"
        "window2$value:INTEGER,window2$id:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,1,4,1002\n"
        "1000,2000,12,1,12,1001,1,12,1001\n"
        "2000,3000,1,2,1,2000,2,1,2000\n"
        "2000,3000,11,2,11,2001,2,11,2001\n"
        "2000,3000,16,2,16,2002,2,16,2002\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,1,4,1002\n"
        "1000,2000,12,1,12,1001,1,12,1001\n"
        "2000,3000,1,2,1,2000,2,1,2000\n"
        "2000,3000,11,2,11,2001,2,11,2001\n"
        "2000,3000,16,2,16,2002,2,16,2002\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentStreamTumblingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,11,2,11,2001,2,11,2301\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win", INT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window3.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,4,1002\n"
        "1000,2000,12,1,12,1001,12,1001\n"
        "2000,3000,1,2,1,2000,1,2000\n"
        "2000,3000,11,2,11,2001,11,2001\n"
        "2000,3000,16,2,16,2002,16,2002\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different streams and different Speed
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentStreamDifferentSpeedTumblingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 0, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 2, 3, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "2000,3000,11,2,11,2001,2,11,2301\n"
        "1000,2000,12,1,12,1001,5,12,1011\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different three sources
 */
TEST_F(JoinDeploymentTest, testJoinWithThreeSources) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort, 16);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 8);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor, 8);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor, 8);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,11,2,11,2001,2,11,2301\n"
        "2000,3000,11,2,11,2001,2,11,2301\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different three sources
 */
TEST_F(JoinDeploymentTest, testJoinWithFourSources) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("JoinDeploymentTest: Worker4 started successfully");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);
    wrk4->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "2000,3000,11,2,11,2001,2,11,2301\n"
        "2000,3000,11,2,11,2001,2,11,2301\n"
        "2000,3000,11,2,11,2001,2,11,2301\n"
        "2000,3000,11,2,11,2001,2,11,2301\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("JoinDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentStreamSlidingWindow) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win1", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("win2", INT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"),
         SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "2000,3000,1,2,1,2000,2,1,2010\n"
        "1500,2500,1,2,1,2000,2,1,2010\n"
        "1000,2000,4,1,4,1002,3,4,1102\n"
        "1000,2000,4,1,4,1002,3,4,1112\n"
        "500,1500,4,1,4,1002,3,4,1102\n"
        "500,1500,4,1,4,1002,3,4,1112\n"
        "2000,3000,11,2,11,2001,2,11,2301\n"
        "1500,2500,11,2,11,2001,2,11,2301\n"
        "1000,2000,12,1,12,1001,5,12,1011\n"
        "500,1500,12,1,12,1001,5,12,1011\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testSlidingWindowDifferentAttributes) {
    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    std::string outputFilePath = "testDeployTwoWorkerJoinUsingTopDownOnSameSchema.out";
    remove(outputFilePath.c_str());

    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win", INT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << window;
    out.close();
    wrk1->registerLogicalStream("window1", testSchemaFileName);

    //register logical stream qnv
    std::string window2 =
        R"(Schema::create()->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", true);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window3.csv", 1, 3, 2, "test_stream", "window2", true);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"),
         SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "_$start:INTEGER,_$end:INTEGER,_$key:INTEGER,window1$win:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
        "window2$id2:INTEGER,window2$timestamp:INTEGER\n"
        "1000,2000,4,1,4,1002,4,1002\n"
        "500,1500,4,1,4,1002,4,1002\n"
        "1000,2000,12,1,12,1001,12,1001\n"
        "500,1500,12,1,12,1001,12,1001\n"
        "2000,3000,1,2,1,2000,1,2000\n"
        "1500,2500,1,2,1,2000,1,2000\n"
        "1000,2000,4,1,4,1002,4,1002\n"
        "2000,3000,11,2,11,2001,11,2001\n"
        "1500,2500,11,2,11,2001,11,2001\n"
        "1000,2000,12,1,12,1001,12,1001\n"
        "2000,3000,16,2,16,2002,16,2002\n"
        "1500,2500,16,2,16,2002,16,2002\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("JoinDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("JoinDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("JoinDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("JoinDeploymentTest: Test finished");
}
}// namespace NES
