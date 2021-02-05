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

class MultipleJoinsTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultipleJoinsTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultipleJoinsTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() { std::cout << "Tear down MultipleJoinsTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};


TEST_F(MultipleJoinsTest, testTwoJoinsWithDifferentStreamTumblingWindowOnCoodinator) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Sensor, 1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

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
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName3 = "window3.hpp";
    std::ofstream out3(testSchemaFileName3);
    out3 << window3;
    out3.close();
    wrk1->registerLogicalStream("window3", testSchemaFileName3);

    //register physical stream
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", false);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 3, 2, "test_stream", "window2", false);

    PhysicalStreamConfigPtr windowStream3 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window4.csv", 1, 3, 2, "test_stream", "window3", false);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .join(Query::from("window3"), Attribute("id1"), Attribute("id3"), TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window3$start:INTEGER,window3$end:INTEGER,window3$key:INTEGER,window2$start:INTEGER,window2$end:INTEGER,window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
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
TEST_F(MultipleJoinsTest, testTwoJoinsWithDifferentStreamTumblingWindowDistributed) {
    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 10, port + 11, NodeType::Worker, 200);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 30, port + 31, NodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>("127.0.0.1", port, "127.0.0.1", port + 40, port + 41, NodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

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
        R"(Schema::create()->addField(createField("win2", UINT64))->addField(createField("id2", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName2 = "window.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << window2;
    out2.close();
    wrk1->registerLogicalStream("window2", testSchemaFileName2);

    std::string window3 =
        R"(Schema::create()->addField(createField("win3", UINT64))->addField(createField("id3", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName3 = "window3.hpp";
    std::ofstream out3(testSchemaFileName3);
    out3 << window3;
    out3.close();
    wrk3->registerLogicalStream("window3", testSchemaFileName3);

    //register physical stream
    PhysicalStreamConfigPtr windowStream =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv", 1, 3, 2, "test_stream", "window1", false);

    PhysicalStreamConfigPtr windowStream2 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window2.csv", 1, 3, 2, "test_stream", "window2", false);

    PhysicalStreamConfigPtr windowStream3 =
        PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window4.csv", 1, 3, 2, "test_stream", "window3", false);

    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);
    wrk4->registerPhysicalStream(windowStream3);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .join(Query::from("window3"), Attribute("id1"), Attribute("id3"), TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
            + outputFilePath + "\", \"CSV_FORMAT\", \"APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window3$start:INTEGER,window3$end:INTEGER,window3$key:INTEGER,window2$start:INTEGER,window2$end:INTEGER,window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);


    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}
}// namespace NES
