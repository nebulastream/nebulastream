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

class RenameTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("RenameTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup RenameTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down RenameTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

TEST_F(RenameTest, testAttributeRenameAndProjection) {
    remove("test.out");

    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("RenameTest: Submit query");
    string query = "Query::from(\"default_logical\").project(Attribute(\"id\").rename(\"NewName\")).sink(FileSinkDescriptor::"
                   "create(\"test.out\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("RenameTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    ifstream my_file("test.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("test.out");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|NewName:UINT32|\n"
                             "+----------------------------------------------------+\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "|1|\n"
                             "+----------------------------------------------------+";
    NES_INFO("RenameTest (testDeployOneWorkerFileOutput): content=" << content);
    NES_INFO("RenameTest (testDeployOneWorkerFileOutput): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(RenameTest, testAttributeRenameAndFilter) {
    remove("test.out");

    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("RenameTest: Submit query");
    string query = "Query::from(\"default_logical\").filter(Attribute(\"id\").rename(\"NewName\") < "
                   "2).sink(FileSinkDescriptor::create(\"test.out\", \"CSV_FORMAT\", \"APPEND\"));";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("RenameTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent = "NewName:INTEGER,value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, "test.out"));

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
    int response = remove("test.out");
    EXPECT_TRUE(response == 0);
}

TEST_F(RenameTest, testCentralWindowEventTime) {
    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

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

    NES_INFO("RenameTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\").rename(\"newId\"), "
                   "TumblingWindow::of(EventTime(Attribute(\"timestamp\")), "
                   "Seconds(1)), Sum(Attribute(\"value\").rename(\"newValue\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 4));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    string expectedContent = "start:INTEGER,end:INTEGER,newId:INTEGER,newValue:INTEGER\n"
                             "1000,2000,1,1\n"
                             "2000,3000,1,2\n"
                             "1000,2000,4,1\n"
                             "2000,3000,11,2\n"
                             "1000,2000,12,1\n"
                             "2000,3000,16,2\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_INFO("RenameTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}

/**
 * Test deploying join with different streams
 */
TEST_F(RenameTest, testJoinWithDifferentStreamTumblingWindow) {
    NES_INFO("RenameTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("RenameTest: Coordinator started successfully");

    NES_INFO("RenameTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RenameTest: Worker1 started successfully");

    NES_INFO("RenameTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("RenameTest: Worker2 started SUCCESSFULLY");

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

    NES_INFO("RenameTest: Submit query");
    string query =
        R"(Query::from("window1").join(Query::from("window2"), Attribute("id1").rename("id1New"), Attribute("id2").rename("id2New"), TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "start:INTEGER,end:INTEGER,key:INTEGER,left_win1:INTEGER,left_id1New:INTEGER,left_timestamp:INTEGER,"
                             "right_win2:INTEGER,right_id2New:INTEGER,right_timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,3,4,1102\n"
                             "1000,2000,4,1,4,1002,3,4,1112\n"
                             "1000,2000,12,1,12,1001,5,12,1011\n"
                             "2000,3000,1,2,1,2000,2,1,2010\n"
                             "2000,3000,11,2,11,2001,2,11,2301\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("RenameTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("RenameTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("RenameTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("RenameTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RenameTest: Test finished");
}
}// namespace NES
