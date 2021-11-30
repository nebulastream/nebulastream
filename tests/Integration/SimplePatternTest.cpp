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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

//used tests: QueryCatalogTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class SimplePatternTest : public testing::Test {
  public:
    CoordinatorConfigPtr coConf;
    WorkerConfigPtr wrkConf;
    CSVSourceConfigPtr srcConf;
    CSVSourceConfigPtr srcConf1;
    CSVSourceConfigPtr srcConf2;

    static void SetUpTestCase() {
        NES::setupLogging("SimplePatternTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SimplePatternTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        coConf = CoordinatorConfig::create();
        wrkConf = WorkerConfig::create();
        srcConf = CSVSourceConfig::create();
        srcConf1 = CSVSourceConfig::create();
        srcConf2 = CSVSourceConfig::create();

        coConf->setRpcPort(rpcPort);
        coConf->setRestPort(restPort);
        wrkConf->setCoordinatorPort(rpcPort);
    }

    void TearDown() override { std::cout << "Tear down SimplePatternTest class." << std::endl; }
    uint64_t restPort = 8081;

    string removeRandomKey(string contentString) {
        std::regex r1("cep_leftkey([0-9]+)");
        std::regex r2("cep_rightkey([0-9]+)");
        contentString = std::regex_replace(contentString, r1, "cep_leftkey");
        contentString = std::regex_replace(contentString, r2, "cep_rightkey");
        return contentString;
    }
};

/* 1.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 * TODO: Ariane
 */
TEST_F(SimplePatternTest, DISABLED_testPatternWithTestStreamSingleOutput) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("coordinator started successfully");

    NES_DEBUG("start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(")).selectionPolicy("Single_Output"); )";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|QnV$sensor_id:CHAR|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "|R2000073|1543624020000|102.629631|8|\n"
                             "|R2000070|1543625280000|108.166664|5|\n"
                             "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 2.Test
  * Iteration Operator
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperator) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", UINT64)->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_intID.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 80).times(3,10).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 0));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|QnV$sensor_id:UINT64|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "|2000070|1543625280000|108.166664|5|\n"
                             "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 3.Test
 * Here, we test if we can use and operator for patterns and create complex events with it
 */
TEST_F(SimplePatternTest, testAndPattern) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SimplePatternTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);
    wrk2->registerLogicalStream("QnV1", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000070");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000073");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testAndPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    NES_INFO("SimplePatternTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV").filter(Attribute("velocity") > 80).andWith(Query::from("QnV1").filter(Attribute("velocity") > 80))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnVQnV1$start:UINT64|QnVQnV1$end:UINT64|QnVQnV1$key:INT32|QnV$sensor_id:CHAR[8]|QnV$timestamp:UINT64|QnV$velocity:"
        "FLOAT32|QnV$quantity:UINT64|QnV$cep_leftkey:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32"
        "|QnV1$quantity:UINT64|QnV1$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543624800000|1543625100000|1|R2000070|1543624980000|90.000000|9|1|R2000070|1543624980000|90.000000|9|1|\n"
        "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * Here, we test if we can use seq operator for patterns and create complex events with it
 */
TEST_F(SimplePatternTest, testSeqPattern) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf2->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SimplePatternTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);
    wrk2->registerLogicalStream("QnV1", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000070");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf2->setSourceType("CSVSource");
    srcConf2->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(0);
    srcConf2->setPhysicalStreamName("test_stream_R2000073");
    srcConf2->setLogicalStreamName("QnV1");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf2);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testSeqPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    NES_INFO("SimplePatternTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity") > 70).seqWith(Query::from("QnV").filter(Attribute("velocity") > 70))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnV1QnV$start:UINT64|QnV1QnV$end:UINT64|QnV1QnV$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:"
        "FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV$sensor_id:CHAR[8]|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|"
        "QnV$quantity:UINT64|QnV$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622400000|1543622700000|1|R2000073|1543622580000|73.166664|5|1|R2000070|1543622640000|70.222221|7|1|\n"
        "|1543623000000|1543623300000|1|R2000073|1543623000000|74.666664|11|1|R2000070|1543623120000|72.611115|6|1|\n"
        "|1543624800000|1543625100000|1|R2000073|1543624860000|70.111115|8|1|R2000070|1543624980000|90.000000|9|1|\n"
        "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);
    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 5.Test
 * Here, we test if we can use and operator for patterns and create complex events with it
 */
//TODO Ariane issue 2303
TEST_F(SimplePatternTest, DISABLED_testMultiAndPattern) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    srcConf2->resetSourceOptions();

    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrkConf->setQueryCompilerCompilationStrategy("DEBUG");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    //    NES_INFO("QueryDeploymentTest: Start worker 2");
    //    wrkConf->setCoordinatorPort(port);
    //    wrkConf->setRpcPort(port + 20);
    //    wrkConf->setDataPort(port + 21);
    //    wrkConf->setQueryCompilerCompilationStrategy("DEBUG");
    //    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    //    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    //    EXPECT_TRUE(retStart2);
    //    NES_INFO("SimplePatternTest: Worker2 started successfully");
    //
    //    NES_INFO("QueryDeploymentTest: Start worker 3");
    //    wrkConf->setCoordinatorPort(port);
    //    wrkConf->setRpcPort(port + 30);
    //    wrkConf->setDataPort(port + 31);
    //    wrkConf->setQueryCompilerCompilationStrategy("DEBUG");
    //    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    //    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    //    EXPECT_TRUE(retStart3);
    //    NES_INFO("SimplePatternTest: Worker3 started successfully");

    std::string outputFilePath = "testMultiAndPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register logical streams qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk1->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(6);
    srcConf->setNumberOfBuffersToProduce(10);
    srcConf->setPhysicalStreamName("test_stream_R2000070");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf2->setSourceType("CSVSource");
    srcConf2->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(6);
    srcConf2->setNumberOfBuffersToProduce(10);
    srcConf2->setPhysicalStreamName("test_stream_R2000073");
    srcConf2->setLogicalStreamName("QnV1");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf2);
    wrk1->registerPhysicalStream(conf73);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(6);
    srcConf1->setNumberOfBuffersToProduce(10);
    srcConf1->setPhysicalStreamName("test_stream_R20000702");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R20000702
    PhysicalStreamConfigPtr conf701 = PhysicalStreamConfig::create(srcConf1);
    wrk1->registerPhysicalStream(conf701);

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("SimplePatternTest: Submit andWith pattern");
    //  Pattern - 1 ANDS - 34 result tuple
    std::string query1 =
        R"(Query::from("QnV").filter(Attribute("velocity") > 65)
        .andWith(Query::from("QnV1").filter(Attribute("velocity") > 65))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    // Pattern - 2 ANDs
    std::string query2 =
        R"(Query::from("QnV").filter(Attribute("velocity") > 70)
        .andWith(Query::from("QnV1").filter(Attribute("velocity") > 70))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .andWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    // join query equivalent to query2
    std::string queryjoin =
        R"(Query::from("QnV").filter(Attribute("velocity") > 70).map(Attribute("key1")=1)
        .joinWith(Query::from("QnV1").filter(Attribute("velocity") > 70)
        .map(Attribute("key2")=1)).where(Attribute("key1")).equalsTo(Attribute("key2"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .joinWith(Query::from("QnV2").filter(Attribute("velocity") > 70).map(Attribute("key4")=1)
        .map(Attribute("key3")=1)).where(Attribute("key1")).equalsTo(Attribute("key3"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    std::string query = queryjoin;

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    //so far from join
    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnVQnV1QnV2$start:UINT64|QnVQnV1QnV2$end:UINT64|QnVQnV1QnV2$key:INT32|QnVQnV1$start:UINT64|QnVQnV1$end:UINT64|QnVQnV1$"
        "key:INT32|QnV$sensor_id:CHAR[8]|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|QnV$key1:INT32|QnV1$"
        "sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$key2:INT32|QnV2$sensor_id:CHAR["
        "8]|QnV2$timestamp:UINT64|QnV2$velocity:FLOAT32|QnV2$quantity:UINT64|QnV2$key3:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622400000|1543622700000|1|1543622400000|1543622700000|1|R2000070|1543622580000|75.111115|6|1|R2000073|"
        "1543622580000|73.166664|5|1|R2000070|1543622580000|75.111115|6|1|\n"
        "|1543622400000|1543622700000|1|1543622400000|1543622700000|1|R2000070|1543622580000|75.111115|6|1|R2000073|"
        "1543622580000|73.166664|5|1|R2000070|1543622640000|70.222221|7|1|\n"
        "|1543622400000|1543622700000|1|1543622400000|1543622700000|1|R2000070|1543622640000|70.222221|7|1|R2000073|"
        "1543622580000|73.166664|5|1|R2000070|1543622580000|75.111115|6|1|\n"
        "|1543622400000|1543622700000|1|1543622400000|1543622700000|1|R2000070|1543622640000|70.222221|7|1|R2000073|"
        "1543622580000|73.166664|5|1|R2000070|1543622640000|70.222221|7|1|\n"
        "+----------------------------------------------------+";

    //EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));
    NES_INFO("QueryDeploymentTest: Remove query");

    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);
    //    NES_INFO("SimplePatternTest: Remove query");

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    //    bool retStopWrk2 = wrk2->stop(false);
    //    EXPECT_TRUE(retStopWrk2);
    //
    //    bool retStopWrk3 = wrk3->stop(false);
    //    EXPECT_TRUE(retStopWrk3);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
/* 6.Test
 * Here, we test if we can use and operator for patterns and create complex events with it
 */
TEST_F(SimplePatternTest, testOrPattern) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf1->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SimplePatternTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000070");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(0);
    srcConf1->setPhysicalStreamName("test_stream_R2000073");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testOrPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    NES_INFO("SimplePatternTest: Submit orWith pattern");

    std::string query =
        R"(Query::from("QnV").filter(Attribute("velocity") > 100).orWith(Query::from("QnV2").filter(Attribute("velocity") > 100))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = Util::splitWithStringDelimiter<std::string>(line, "|");
        if (content.size() > 1 && content.at(1) == "R2000073") {
            NES_INFO("First content=" << content.at(2));
            NES_INFO("First: expContent= 102.629631");
            if (content.at(3) == "102.629631") {
                resultWrk1 = true;
            }
        } else if (content.size() > 1 && content.at(1) == "R2000070") {
            NES_INFO("Second: content=" << content.at(2));
            NES_INFO("Second: expContent= 108.166664");
            if (content.at(3) == "108.166664") {
                resultWrk2 = true;
            }
        }
    }

    EXPECT_TRUE((resultWrk1 && resultWrk2));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES
