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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop

#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <QueryCompiler/GeneratableTypes/Array.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
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

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down JoinDeploymentTest class." << std::endl; }
};

/**
 * Test deploying join query with source on two different worker node using top down strategy.
 */
//TODO: this test will be enabled once we have the renaming function using as
//TODO: prevent self join
TEST_F(JoinDeploymentTest, DISABLED_testSelfJoinTumblingWindow) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setPhysicalStreamName("test_stream2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window").as("w1").joinWith(Query::from("window").as("w2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent;
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);
    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$value:"
                             "INTEGER,window1$id:INTEGER,window1$timestamp:INTEGER,"
                             "window2$value:INTEGER,window2$id:INTEGER,window2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,1,4,1002\n"
                             "1000,2000,12,1,12,1001,1,12,1001\n"
                             "2000,3000,1,2,1,2000,2,1,2000\n"
                             "2000,3000,11,2,11,2001,2,11,2001\n"
                             "2000,3000,16,2,16,2002,2,16,2002\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
                             "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,1,4,1002\n"
                             "1000,2000,12,1,12,1001,1,12,1001\n"
                             "2000,3000,1,2,1,2000,2,1,2000\n"
                             "2000,3000,11,2,11,2001,2,11,2001\n"
                             "2000,3000,16,2,16,2002,2,16,2002\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setSourceConfig("../tests/test_data/window2.csv");
    srcConf->setLogicalStreamName("window2");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");
    srcConf->setSourceConfig("../tests/test_data/window3.csv");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
                             "window2$id2:INTEGER,window2$timestamp:INTEGER\n"
                             "1000,2000,4,1,4,1002,4,1002\n"
                             "1000,2000,12,1,12,1001,12,1001\n"
                             "2000,3000,1,2,1,2000,1,2000\n"
                             "2000,3000,11,2,11,2001,11,2001\n"
                             "2000,3000,16,2,16,2002,16,2002\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSourceFrequency(0);
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");
    srcConf->setSourceConfig("../tests/test_data/window2.csv");
    srcConf->setSourceFrequency(1);
    srcConf->setNumberOfTuplesToProducePerBuffer(2);
    srcConf->setNumberOfBuffersToProduce(3);

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
                             "window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER\n"
                             "2000,3000,1,2,1,2000,2,1,2010\n"
                             "1000,2000,4,1,4,1002,3,4,1102\n"
                             "1000,2000,4,1,4,1002,3,4,1112\n"
                             "2000,3000,11,2,11,2001,2,11,2301\n"
                             "1000,2000,12,1,12,1001,5,12,1011\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    crdConf->setNumberOfSlots(16);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");
    srcConf->setSourceConfig("../tests/test_data/window2.csv");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
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

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    crdConf->setNumberOfSlots(8);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    wrkConf->setNumberOfSlots(8);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");
    srcConf->setSourceConfig("../tests/test_data/window2.csv");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);
    wrk4->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
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

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");
    srcConf->setSourceConfig("../tests/test_data/window2.csv");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(
         SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
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
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("JoinDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

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
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
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

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(2);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window1");
    srcConf->setSkipHeader(true);

    //register physical stream R2000070
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    srcConf->setLogicalStreamName("window2");
    srcConf->setSourceConfig("../tests/test_data/window3.csv");

    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("JoinDeploymentTest: Submit query");
    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(
         SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "TopDown");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win:"
                             "INTEGER,window1$id1:INTEGER,window1$timestamp:INTEGER,"
                             "window2$id2:INTEGER,window2$timestamp:INTEGER\n"
                             "2000,3000,1,2,1,2000,1,2000\n"
                             "1500,2500,1,2,1,2000,1,2000\n"
                             "1000,2000,4,1,4,1002,4,1002\n"
                             "500,1500,4,1,4,1002,4,1002\n"
                             "2000,3000,11,2,11,2001,11,2001\n"
                             "1500,2500,11,2,11,2001,11,2001\n"
                             "1000,2000,12,1,12,1001,12,1001\n"
                             "500,1500,12,1,12,1001,12,1001\n"
                             "2000,3000,16,2,16,2002,16,2002\n"
                             "1500,2500,16,2,16,2002,16,2002\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

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

/*
 * @brief Test a join query that uses fixed-array as keys
 */
TEST_F(JoinDeploymentTest, testJoinWithFixedCharKey) {
    struct Window1 {
        NES::QueryCompilation::Array<char, 8> id1;
        uint64_t timestamp;
    };

    struct Window2 {
        NES::QueryCompilation::Array<char, 8> id2;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createFixedChar(8))
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createFixedChar(8))
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinOperator =
        R"(Query::from("window1")
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .project(Attribute("window1window2$start"),Attribute("window1window2$end"), Attribute("window1window2$key"),  Attribute("window1$timestamp"))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);

    testHarness.addMemorySource("window1", window1Schema, "window1");
    testHarness.addMemorySource("window2", window2Schema, "window2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2u);

    testHarness.pushElement<Window1>({"aaaaaaa", 1000u}, 0u);
    testHarness.pushElement<Window2>({"bbbbbbb", 1001u}, 0u);
    testHarness.pushElement<Window2>({"ccccccc", 1002u}, 0u);
    testHarness.pushElement<Window2>({"aaaaaaa", 2000u}, 0u);
    testHarness.pushElement<Window2>({"ddddddd", 2001u}, 0u);
    testHarness.pushElement<Window2>({"eeeeeee", 2002u}, 0u);
    testHarness.pushElement<Window2>({"aaaaaaa", 3000u}, 0u);

    testHarness.pushElement<Window2>({"fffffff", 1003u}, 1u);
    testHarness.pushElement<Window2>({"bbbbbbb", 1011u}, 1u);
    testHarness.pushElement<Window2>({"ccccccc", 1102u}, 1u);
    testHarness.pushElement<Window2>({"ccccccc", 1112u}, 1u);
    testHarness.pushElement<Window2>({"aaaaaaa", 2010u}, 1u);
    testHarness.pushElement<Window2>({"ddddddd", 2301u}, 1u);
    testHarness.pushElement<Window2>({"ggggggg", 3100u}, 1u);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        NES::QueryCompilation::Array<char, 8> window1window2$key;
        uint64_t window2$timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window2$timestamp == rhs.window2$timestamp);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, "ddddddd", 2001},
                                          {1000, 2000, "ccccccc", 1002},
                                          {1000, 2000, "ccccccc", 1002},
                                          {1000, 2000, "bbbbbbb", 1001}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES
