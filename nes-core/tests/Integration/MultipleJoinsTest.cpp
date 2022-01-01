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
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class MultipleJoinsTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("MultipleJoinsTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MultipleJoinsTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down MultipleJoinsTest class." << std::endl; }

    std::string ipAddress = "127.0.0.1";
};

TEST_F(MultipleJoinsTest, testJoins2WithDifferentStreamTumblingWindowOnCoodinator) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0u);
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

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = "testTwoJoinsWithDifferentStreamTumblingWindowOnCoodinator.out";
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
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");

    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");

    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);

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
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
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
TEST_F(MultipleJoinsTest, DISABLED_testJoin2WithDifferentStreamTumblingWindowDistributed) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
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
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);
    wrk4->registerPhysicalStream(windowStream3);

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
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamTumblingWindowOnCoodinatorSequential) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
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
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("JoinDeploymentTest: Worker4 started SUCCESSFULLY");

    std::string outputFilePath = "testJoin4WithDifferentStreamTumblingWindowOnCoodinator.out";
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

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName4 = "window4.hpp";
    std::ofstream out4(testSchemaFileName4);
    out4 << window4;
    out4.close();
    wrk1->registerLogicalStream("window4", testSchemaFileName4);

    //register physical stream
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    SourceConfigPtr srcConf3 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf3->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf3->as<CSVSourceConfig>()->setLogicalStreamName("window4");
    srcConf3->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf3->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf3->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    PhysicalStreamConfigPtr windowStream4 = PhysicalStreamConfig::create(srcConf3);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);
    wrk4->registerPhysicalStream(windowStream4);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith(Query::from("window4")).where(Attribute("id1")).equalsTo(Attribute("id4")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,"
        "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:"
        "INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:"
        "INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:"
        "INTEGER\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamTumblingWindowOnCoodinatorNested) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
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
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("JoinDeploymentTest: Worker4 started SUCCESSFULLY");

    std::string outputFilePath = "testJoin4WithDifferentStreamTumblingWindowOnCoodinator.out";
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

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName4 = "window4.hpp";
    std::ofstream out4(testSchemaFileName4);
    out4 << window4;
    out4.close();
    wrk1->registerLogicalStream("window4", testSchemaFileName4);

    //register physical stream
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    SourceConfigPtr srcConf3 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf3->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf3->as<CSVSourceConfig>()->setLogicalStreamName("window4");
    srcConf3->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf3->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf3->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    PhysicalStreamConfigPtr windowStream4 = PhysicalStreamConfig::create(srcConf3);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);
    wrk4->registerPhysicalStream(windowStream4);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    auto query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .joinWith((Query::from("window3")).joinWith(Query::from("window4")).where(Attribute("id3")).equalsTo(Attribute("id4")).window(TumblingWindow::of(EventTime(Attribute("timestamp"))
        ,Milliseconds(1000)))).where(Attribute("id1")).equalsTo(Attribute("id4")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Milliseconds(1000)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$"
        "id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3window4$"
        "start:INTEGER,window3window4$end:INTEGER,window3window4$key:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$"
        "timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

/**
 *
 *
 * Sliding window joins
 *
 */

TEST_F(MultipleJoinsTest, testJoins2WithDifferentStreamSlidingWindowOnCoodinator) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
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

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = "testTwoJoinsWithDifferentStreamSlidingWindowOnCoodinator.out";
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
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
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
TEST_F(MultipleJoinsTest, DISABLED_testJoin2WithDifferentStreamSlidingWindowDistributed) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
    NES_INFO("JoinDeploymentTest: Coordinator started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("JoinDeploymentTest: Worker1 started successfully");

    NES_INFO("JoinDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(1, 2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(1, 2);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(1, 2);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    std::string outputFilePath = "testTwoJoinsWithDifferentStreamSlidingWindowDistributed.out";
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
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    wrk2->registerPhysicalStream(windowStream);
    wrk3->registerPhysicalStream(windowStream2);
    wrk4->registerPhysicalStream(windowStream3);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,window1window2$"
        "start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:INTEGER,window1$"
        "timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:INTEGER,window3$id3:"
        "INTEGER,window3$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamSlidingWindowOnCoodinatorSequential) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
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
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("JoinDeploymentTest: Worker4 started SUCCESSFULLY");

    std::string outputFilePath = "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
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

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName4 = "window4.hpp";
    std::ofstream out4(testSchemaFileName4);
    out4 << window4;
    out4.close();
    wrk1->registerLogicalStream("window4", testSchemaFileName4);

    //register physical stream
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    SourceConfigPtr srcConf3 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf3->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf3->as<CSVSourceConfig>()->setLogicalStreamName("window4");
    srcConf3->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf3->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf3->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    PhysicalStreamConfigPtr windowStream4 = PhysicalStreamConfig::create(srcConf3);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);
    wrk4->registerPhysicalStream(windowStream4);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    string query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith(Query::from("window4")).where(Attribute("id1")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2window3$start:INTEGER,window1window2window3$end:INTEGER,window1window2window3$key:INTEGER,"
        "window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$id1:"
        "INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3$win3:"
        "INTEGER,window3$id3:INTEGER,window3$timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:"
        "INTEGER\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}

TEST_F(MultipleJoinsTest, testJoin3WithDifferentStreamSlidingWindowOnCoodinatorNested) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_INFO("JoinDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
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
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("JoinDeploymentTest: Worker2 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("JoinDeploymentTest: Worker3 started SUCCESSFULLY");

    NES_INFO("JoinDeploymentTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("JoinDeploymentTest: Worker4 started SUCCESSFULLY");

    std::string outputFilePath = "testJoin4WithDifferentStreamSlidingWindowOnCoodinator.out";
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

    std::string window4 =
        R"(Schema::create()->addField(createField("win4", UINT64))->addField(createField("id4", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName4 = "window4.hpp";
    std::ofstream out4(testSchemaFileName4);
    out4 << window4;
    out4.close();
    wrk1->registerLogicalStream("window4", testSchemaFileName4);

    //register physical stream
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    PhysicalStreamConfigPtr windowStream = PhysicalStreamConfig::create(srcConf);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");

    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    PhysicalStreamConfigPtr windowStream2 = PhysicalStreamConfig::create(srcConf1);

    SourceConfigPtr srcConf2 = SourceConfigFactory::createSourceConfig("CSVSource");

    srcConf2->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf2->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf2->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf2->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf2->as<CSVSourceConfig>()->setLogicalStreamName("window3");
    PhysicalStreamConfigPtr windowStream3 = PhysicalStreamConfig::create(srcConf2);

    SourceConfigPtr srcConf3 = SourceConfigFactory::createSourceConfig("CSVSource");

    srcConf3->as<CSVSourceConfig>()->setLogicalStreamName("window4");
    srcConf3->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window4.csv");
    srcConf3->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf3->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf3->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    PhysicalStreamConfigPtr windowStream4 = PhysicalStreamConfig::create(srcConf3);

    wrk1->registerPhysicalStream(windowStream);
    wrk2->registerPhysicalStream(windowStream2);
    wrk3->registerPhysicalStream(windowStream3);
    wrk4->registerPhysicalStream(windowStream4);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("MultipleJoinsTest: Submit query");

    auto query =
        R"(Query::from("window1")
        .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .joinWith((Query::from("window3")).joinWith(Query::from("window4")).where(Attribute("id3")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp"))
        ,Seconds(1),Milliseconds(500)))).where(Attribute("id1")).equalsTo(Attribute("id4")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "TopDown", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "window1window2window3window4$start:INTEGER,window1window2window3window4$end:INTEGER,window1window2window3window4$key:"
        "INTEGER,window1window2$start:INTEGER,window1window2$end:INTEGER,window1window2$key:INTEGER,window1$win1:INTEGER,window1$"
        "id1:INTEGER,window1$timestamp:INTEGER,window2$win2:INTEGER,window2$id2:INTEGER,window2$timestamp:INTEGER,window3window4$"
        "start:INTEGER,window3window4$end:INTEGER,window3window4$key:INTEGER,window3$win3:INTEGER,window3$id3:INTEGER,window3$"
        "timestamp:INTEGER,window4$win4:INTEGER,window4$id4:INTEGER,window4$timestamp:INTEGER\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1102,500,1500,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,1000,2000,4,4,4,1001,4,4,1001\n"
        "500,1500,4,500,1500,4,1,4,1002,3,4,1112,500,1500,4,4,4,1001,4,4,1001\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1000,2000,12,1,12,1300,1,12,1300\n"
        "500,1500,12,500,1500,12,1,12,1001,5,12,1011,500,1500,12,1,12,1300,1,12,1300\n";
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("MultipleJoinsTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("MultipleJoinsTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("MultipleJoinsTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("MultipleJoinsTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("MultipleJoinsTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("MultipleJoinsTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("MultipleJoinsTest: Test finished");
}
}// namespace NES
