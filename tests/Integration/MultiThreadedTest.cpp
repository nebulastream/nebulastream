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
#include <iostream>

using namespace std;

namespace NES {
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class MultiThreadedTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("MultiThreadedTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultiThreadedTest test class.");
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    static void TearDownTestCase() { NES_INFO("Tear down MultiWorkerTest class."); }
};

TEST_F(MultiThreadedTest, testFilterQuery) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    crdConf->setNumWorkerThreads(3);
    wrkConf->setCoordinatorPort(rpcPort);
    wrkConf->setNumWorkerThreads(3);

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog(); /*register logical stream qnv*/
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    wrk1->registerLogicalStream("stream", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->setNumberOfBuffersToProduce(210);
    srcConf->setSourceFrequency(0); /*TODO: change this to 0 if we have the the end of stream*/
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("stream");
    srcConf->setSkipHeader(false); /*register physical stream*/
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(streamConf);

    std::string outputFilePath = "MultiThreadedTest_testFilterQuery.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream")
        .filter(Attribute("value") < 2)
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));


    string expectedContent =
        "stream$value:INTEGER,stream$id:INTEGER,stream$timestamp:INTEGER\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n"
        "1,1,1000\n"
        "1,12,1001\n"
        "1,4,1002\n";

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

TEST_F(MultiThreadedTest, testProjectQuery) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    crdConf->setNumWorkerThreads(3);
    wrkConf->setNumWorkerThreads(3);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog(); /*register logical stream qnv*/
    std::string stream =
        R"(Schema::create()->addField(createField("value", UINT64))->addField(createField("id", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "window.hpp";
    std::ofstream out(testSchemaFileName);
    out << stream;
    out.close();
    wrk1->registerLogicalStream("stream", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(1);
    srcConf->setNumberOfBuffersToProduce(210);
    srcConf->setSourceFrequency(0); /*TODO: change this to 0 if we have the the end of stream*/
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("stream");
    srcConf->setSkipHeader(false); /*register physical stream*/
    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(streamConf);

    std::string outputFilePath = "MultiThreadedTest_testProjectQuery.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("stream")
        .filter(Attribute("value") < 2)
        .project(Attribute("id"))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "stream$id:INTEGER\n"
        "1\n"
        "12\n"
        "4\n"
        "1\n"
        "12\n"
        "4\n"
        "1\n"
        "12\n"
        "4\n"
        "1\n"
        "12\n"
        "4\n"
        "1\n"
        "12\n"
        "4\n"
        "1\n"
        "12\n"
        "4\n"
        "1\n"
        "12\n"
        "4\n"
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
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr sourceConfig = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    coordinatorConfig->setNumWorkerThreads(3);
    workerConfig->setNumWorkerThreads(3);

    NES_INFO("WindowDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
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

    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("../tests/test_data/window.csv");
    sourceConfig->setSourceFrequency(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test_stream");
    sourceConfig->setLogicalStreamName("window");

    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(sourceConfig);
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

}// namespace NES