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

    void TearDown() {
        std::cout << "Tear down WindowDeploymentTest class." << std::endl;
    }

    std::string ipAddress = "127.0.0.1";
};

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerCentralTumblingWindowQueryEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/exdra.csv",
                                                                1, 0, 1,
                                                                "test_stream", "exdra", true);

    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testDeployOneWorkerCentralWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"exdra\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"metadata_generated\")), "
                   "Seconds(10)), Sum(Attribute(\"features_properties_capacity\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"DONT_APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    sleep(17);

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

#if 0
    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|key:UINT64|features_properties_capacity:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1262343610000|1262343620000|1|736|\n"
        "|1262343620000|1262343630000|1|0|\n"
        "|1262343630000|1262343640000|1|0|\n"
        "|1262343640000|1262343650000|1|0|\n"
        "|1262343650000|1262343660000|1|0|\n"
        "|1262343660000|1262343670000|1|0|\n"
        "|1262343670000|1262343680000|1|0|\n"
        "|1262343680000|1262343690000|1|0|\n"
        "|1262343620000|1262343630000|2|1348|\n"
        "|1262343630000|1262343640000|2|0|\n"
        "|1262343640000|1262343650000|2|0|\n"
        "|1262343650000|1262343660000|2|0|\n"
        "|1262343660000|1262343670000|2|0|\n"
        "|1262343670000|1262343680000|2|0|\n"
        "|1262343680000|1262343690000|2|0|\n"
        "|1262343630000|1262343640000|3|4575|\n"
        "|1262343640000|1262343650000|3|0|\n"
        "|1262343650000|1262343660000|3|0|\n"
        "|1262343660000|1262343670000|3|0|\n"
        "|1262343670000|1262343680000|3|0|\n"
        "|1262343680000|1262343690000|3|0|\n"
        "|1262343640000|1262343650000|4|1358|\n"
        "|1262343650000|1262343660000|4|0|\n"
        "|1262343660000|1262343670000|4|0|\n"
        "|1262343670000|1262343680000|4|0|\n"
        "|1262343680000|1262343690000|4|0|\n"
        "|1262343650000|1262343660000|5|1288|\n"
        "|1262343660000|1262343670000|5|0|\n"
        "|1262343670000|1262343680000|5|0|\n"
        "|1262343680000|1262343690000|5|0|\n"
        "|1262343660000|1262343670000|6|3458|\n"
        "|1262343670000|1262343680000|6|0|\n"
        "|1262343680000|1262343690000|6|0|\n"
        "|1262343670000|1262343680000|7|1128|\n"
        "|1262343680000|1262343690000|7|0|\n"
        "|1262343680000|1262343690000|8|1079|\n"
        "|1262343600000|1262343610000|10|2632|\n"
        "|1262343610000|1262343620000|10|0|\n"
        "|1262343620000|1262343630000|10|0|\n"
        "|1262343630000|1262343640000|10|0|\n"
        "|1262343640000|1262343650000|10|0|\n"
        "|1262343650000|1262343660000|10|0|\n"
        "|1262343660000|1262343670000|10|0|\n"
        "|1262343670000|1262343680000|10|0|\n"
        "|1262343680000|1262343690000|10|0|\n"
        "|1262343600000|1262343610000|11|4653|\n"
        "|1262343610000|1262343620000|11|0|\n"
        "|1262343620000|1262343630000|11|0|\n"
        "|1262343630000|1262343640000|11|0|\n"
        "|1262343640000|1262343650000|11|0|\n"
        "|1262343650000|1262343660000|11|0|\n"
        "|1262343660000|1262343670000|11|0|\n"
        "|1262343670000|1262343680000|11|0|\n"
        "|1262343680000|1262343690000|11|0|\n"
        "+----------------------------------------------------+";
#else
    string expectedContent = "start:INTEGER,end:INTEGER,id:INTEGER,features_properties_capacity:INTEGER\n"
                             "1262343610000,1262343620000,1,736\n"
                             "1262343620000,1262343630000,1,0\n"
                             "1262343630000,1262343640000,1,0\n"
                             "1262343640000,1262343650000,1,0\n"
                             "1262343650000,1262343660000,1,0\n"
                             "1262343660000,1262343670000,1,0\n"
                             "1262343670000,1262343680000,1,0\n"
                             "1262343680000,1262343690000,1,0\n"
                             "1262343620000,1262343630000,2,1348\n"
                             "1262343630000,1262343640000,2,0\n"
                             "1262343640000,1262343650000,2,0\n"
                             "1262343650000,1262343660000,2,0\n"
                             "1262343660000,1262343670000,2,0\n"
                             "1262343670000,1262343680000,2,0\n"
                             "1262343680000,1262343690000,2,0\n"
                             "1262343630000,1262343640000,3,4575\n"
                             "1262343640000,1262343650000,3,0\n"
                             "1262343650000,1262343660000,3,0\n"
                             "1262343660000,1262343670000,3,0\n"
                             "1262343670000,1262343680000,3,0\n"
                             "1262343680000,1262343690000,3,0\n"
                             "1262343640000,1262343650000,4,1358\n"
                             "1262343650000,1262343660000,4,0\n"
                             "1262343660000,1262343670000,4,0\n"
                             "1262343670000,1262343680000,4,0\n"
                             "1262343680000,1262343690000,4,0\n"
                             "1262343650000,1262343660000,5,1288\n"
                             "1262343660000,1262343670000,5,0\n"
                             "1262343670000,1262343680000,5,0\n"
                             "1262343680000,1262343690000,5,0\n"
                             "1262343660000,1262343670000,6,3458\n"
                             "1262343670000,1262343680000,6,0\n"
                             "1262343680000,1262343690000,6,0\n"
                             "1262343670000,1262343680000,7,1128\n"
                             "1262343680000,1262343690000,7,0\n"
                             "1262343680000,1262343690000,8,1079\n"
                             "1262343600000,1262343610000,10,2632\n"
                             "1262343610000,1262343620000,10,0\n"
                             "1262343620000,1262343630000,10,0\n"
                             "1262343630000,1262343640000,10,0\n"
                             "1262343640000,1262343650000,10,0\n"
                             "1262343650000,1262343660000,10,0\n"
                             "1262343660000,1262343670000,10,0\n"
                             "1262343670000,1262343680000,10,0\n"
                             "1262343680000,1262343690000,10,0\n"
                             "1262343600000,1262343610000,11,4653\n"
                             "1262343610000,1262343620000,11,0\n"
                             "1262343620000,1262343630000,11,0\n"
                             "1262343630000,1262343640000,11,0\n"
                             "1262343640000,1262343650000,11,0\n"
                             "1262343650000,1262343660000,11,0\n"
                             "1262343660000,1262343670000,11,0\n"
                             "1262343670000,1262343680000,11,0\n"
                             "1262343680000,1262343690000,11,0\n";
#endif
    NES_INFO("WindowDeploymentTest(testDeployOneWorkerCentralWindowQueryEventTime): content=" << content);
    NES_INFO("WindowDeploymentTest(testDeployOneWorkerCentralWindowQueryEventTime): expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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
 * @brief test central tumbling window and processing time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerCentralTumblingWindowQueryProcessingTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath =
        "testDeployOneWorkerCentralWindowQueryProcessingTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/exdra.csv",
                                                                1, 0, 2,
                                                                "test_stream", "exdra", true);
    wrk1->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"exdra\").windowByKey(Attribute(\"id\"), TumblingWindow::of(ProcessingTime(), "
                   "Seconds(1)), Sum(Attribute(\"metadata_generated\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    //todo will be removed once the new window source is in place
    sleep(20);
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    std::ifstream f(outputFilePath);
    std::cout << "file content=" << std::endl;
    std::cout << f.rdbuf() << std::endl;

    std::ifstream ifs(outputFilePath);
    std::string line;

    bool found1 = false;
    bool found2 = false;
    bool found3 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = UtilityFunctions::split(line, '|');
        for (auto word : content) {
            if (word == "2524687220000") {
                found1 = true;
            }

            if (word == "1262343620010") {
                found2 = true;
            }
            if (word == "1262343630020") {
                found3 = true;
            }
        }
    }
    EXPECT_TRUE(found1 && found2 && found3);

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
TEST_F(WindowDeploymentTest, testDeployOneWorkerCentralSlidingWindowQueryEventTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv",
                                                                  1, 0, 1,
                                                                  "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);

    std::string outputFilePath =
        "testDeployOneWorkerCentralSlidingWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    sleep(25);
    NES_DEBUG("wakeup");

    ifstream my_file("testDeployOneWorkerCentralSlidingWindowQueryEventTime.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("testDeployOneWorkerCentralSlidingWindowQueryEventTime.out");
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|id:UINT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|10000|20000|1|870|\n"
        "|5000|15000|1|570|\n"
        "|0|10000|1|307|\n"
        "|10000|20000|4|0|\n"
        "|5000|15000|4|0|\n"
        "|0|10000|4|6|\n"
        "|10000|20000|11|0|\n"
        "|5000|15000|11|0|\n"
        "|0|10000|11|30|\n"
        "|10000|20000|12|0|\n"
        "|5000|15000|12|0|\n"
        "|0|10000|12|7|\n"
        "|10000|20000|16|0|\n"
        "|5000|15000|16|0|\n"
        "|0|10000|16|12|\n"
        "+----------------------------------------------------+";

    NES_INFO("WindowDeploymentTest: content=" << content);
    NES_INFO("WindowDeploymentTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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
 * @brief test distributed tumbling window and processing time
 */
TEST_F(WindowDeploymentTest, testDeployOneWorkerDistributedTumblingWindowQueryProcessingTime) {
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"default_logical\").windowByKey(Attribute(\"id\"), TumblingWindow::of(ProcessingTime(), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\"query.out\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    cout << "wait start" << endl;
    sleep(10);
    cout << "wakeup" << endl;
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    //    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

    std::ifstream f("query.out");
    std::cout << "file content=" << std::endl;
    std::cout << f.rdbuf() << std::endl;

    std::ifstream ifs("query.out");
    std::string line;
    int rowNumber = 0;
    //TODO that query result is empty?
    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        rowNumber++;
    }
    NES_INFO("WindowDeploymentTest(testDeployOneWorkerDistributedWindowQueryProcessingTime): content=" << rowNumber);
    NES_INFO("WindowDeploymentTest(testDeployOneWorkerDistributedWindowQueryProcessingTime): expContent=0");
    EXPECT_EQ(rowNumber, 0);

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
TEST_F(WindowDeploymentTest, testDeployOneWorkerDistributedTumblingWindowQueryEventTime) {
    ::testing::GTEST_FLAG(throw_on_failure) = true;
    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("WindowDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath =
        "testDeployOneWorkerDistributedWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("WindowDeploymentTest: Submit query");

    //register logical stream
    std::string testSchema = R"(Schema::create()->addField("id", BasicType::UINT64)->addField("value", BasicType::UINT64)->addField("ts", BasicType::UINT64);)";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();
    wrk1->registerLogicalStream("window", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv",
                                                                1, 3, 3,
                                                                "test_stream", "window", true);
    wrk1->registerPhysicalStream(conf);
    wrk2->registerPhysicalStream(conf);

    NES_INFO("WindowDeploymentTest: Submit query");
    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), TumblingWindow::of(EventTime(Attribute(\"ts\")), "
                   "Seconds(1)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\", \"CSV_FORMAT\", \"DONT_APPEND\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    cout << "wait start" << endl;
    sleep(20);
    cout << "wakeup" << endl;

    ifstream outFile(outputFilePath);
    EXPECT_TRUE(outFile.good());

    std::ifstream f(outputFilePath);
    std::cout << "filecontent=" << std::endl;
    std::cout << f.rdbuf() << std::endl;

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

#if 0
    string expectedContent =
        "+----------------------------------------------------+\n"
        "|start:UINT64|end:UINT64|id:UINT64|value:UINT64|\n"
        "+----------------------------------------------------+\n"
        "|1000|2000|1|34|\n"
        "|2000|3000|1|0|\n"
        "|2000|3000|2|56|\n"
        "+----------------------------------------------------+";
#else
    string expectedContent =
        "start:INTEGER,end:INTEGER,id:INTEGER,value:INTEGER\n"
        "1000,2000,1,34\n"
        "2000,3000,1,0\n"
        "2000,3000,2,56\n";
#endif

    NES_INFO("WindowDeploymentTest: content=" << content);
    NES_INFO("WindowDeploymentTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("WindowDeploymentTest: Coordinator started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker 1 started successfully");

    NES_INFO("WindowDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
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
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/window.csv",
                                                                  1, 0, 1,
                                                                  "test_stream", "window", true);

    wrk1->registerPhysicalStream(conf70);
    wrk2->registerPhysicalStream(conf70);

    std::string outputFilePath =
        "testDeployOneWorkerCentralSlidingWindowQueryEventTime.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    string query = "Query::from(\"window\").windowByKey(Attribute(\"id\"), SlidingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(10),Seconds(5)), Sum(Attribute(\"value\"))).sink(FileSinkDescriptor::create(\"" + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    NES_DEBUG("wait start");
    sleep(25);
    NES_DEBUG("wakeup");

    ifstream my_file("testDeployOneWorkerCentralSlidingWindowQueryEventTime.out");
    EXPECT_TRUE(my_file.good());

    std::ifstream ifs("testDeployOneWorkerCentralSlidingWindowQueryEventTime.out");
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    string expectedContent =
        "+----------------------------------------------------+\n|start:UINT64|end:UINT64|id:UINT64|value:UINT64|\n+----------------------------------------------------+\n|10000|20000|1|1740|\n|5000|15000|1|1140|\n|10000|20000|4|0|\n|5000|15000|4|0|\n|10000|20000|11|0|\n|5000|15000|11|0|\n|10000|20000|12|0|\n|5000|15000|12|0|\n|10000|20000|16|0|\n|5000|15000|16|0|\n+----------------------------------------------------+";

    NES_INFO("WindowDeploymentTest: content=" << content);
    NES_INFO("WindowDeploymentTest: expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

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

}// namespace NES
