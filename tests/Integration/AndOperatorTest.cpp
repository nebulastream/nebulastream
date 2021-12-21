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
#include <chrono> //for timing execution

//used tests: QueryCatalogTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class AndOperatorTest : public testing::Test {
  public:
    CoordinatorConfigPtr coConf;
    WorkerConfigPtr wrkConf;
    CSVSourceConfigPtr srcConf;
    CSVSourceConfigPtr srcConf1;
    CSVSourceConfigPtr srcConf2;

    static void SetUpTestCase() {
        NES::setupLogging("AndOperatorTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AndOperatorTest test class.");
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

    void TearDown() override { std::cout << "Tear down AndOperatorTest class." << std::endl; }
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
 * Here, we test if we can use and operator for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternOneAnd) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(5);
    srcConf->setNumberOfBuffersToProduce(20);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testAndPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV").filter(Attribute("velocity") > 80).andWith(Query::from("QnV1").filter(Attribute("velocity") > 80))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("AndOperatorTest: Remove query");
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

/* 4d.Test
 * Here, we test if we can use and operator with sliding window (5 Minutes, 1 Minute) for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternAndWithSlidingWindowD) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
   NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(5);
    srcConf->setNumberOfBuffersToProduce(20);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternAndSliding.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50).andWith(Query::from("QnV2").filter(Attribute("velocity")>50)).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(5),Minutes(1))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnVQnV1$start:UINT64|QnVQnV1$end:UINT64|QnVQnV1$key:INT32|QnV$sensor_id:CHAR[8]|QnV$timestamp:UINT64|QnV$velocity:"
        "FLOAT32|QnV$quantity:UINT64|QnV$cep_leftkey:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$velocity:FLOAT32"
        "|QnV1$quantity:UINT64|QnV1$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543624800000|1543625100000|1|R2000070|1543624980000|90.000000|9|1|R2000070|1543624980000|90.000000|9|1|\n"
        "+----------------------------------------------------+";

    EXPECT_EQ(removeRandomKey(content),expectedContent);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4d.Test
 * Here, we test if we can use join operator with sliding window (5 Minutes, 1 Minute) for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternJoinWithSlidingWindowD) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    //NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //NES_INFO("AndOperatorTest: Coordinator started successfully");

    //NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    //NES_INFO("AndOperatorTest: Worker1 started successfully");

    //NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    //NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV1.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(20);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(20);
    srcConf1->setNumberOfBuffersToProduce(10000);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternFourCAnd.out";
    remove(outputFilePath.c_str());

    //NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").map(Attribute("key1")=1).filter(Attribute("velocity")>50).joinWith(Query::from("QnV2").map(Attribute("key2")=1).filter(Attribute("velocity")>50)).where(Attribute("key1")).equalsTo(Attribute("key2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(5),Minutes(1))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms";
    NES_DEBUG("TIMER SET!!!!");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    //EXPECT_EQ(removeRandomKey(content), removeRandomKey(content));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4e.Test
 * Here, we test if we can use and operator with sliding window (30 Minutes, 15 Minute) for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternAndWithSlidingWindowE) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    //NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //NES_INFO("AndOperatorTest: Coordinator started successfully");

    //NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    //NES_INFO("AndOperatorTest: Worker1 started successfully");

    //NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    //NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV1.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(20);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(20);
    srcConf1->setNumberOfBuffersToProduce(10000);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternFourBAnd.out";
    remove(outputFilePath.c_str());

    //NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50).andWith(Query::from("QnV2").filter(Attribute("velocity")>50)).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(30),Minutes(15))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms";
    NES_DEBUG("TIMER SET!!!!");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    //EXPECT_EQ(removeRandomKey(content), removeRandomKey(content));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4e.Test
 * Here, we test if we can use join operator with sliding window (30 Minutes, 15 Minute) for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternJoinWithSlidingWindowE) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    //NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //NES_INFO("AndOperatorTest: Coordinator started successfully");

    //NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    //NES_INFO("AndOperatorTest: Worker1 started successfully");

    //NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    //NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV1.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(20);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(20);
    srcConf1->setNumberOfBuffersToProduce(10000);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternFourCAnd.out";
    remove(outputFilePath.c_str());

    //NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").map(Attribute("key1")=1).filter(Attribute("velocity")>50).joinWith(Query::from("QnV2").map(Attribute("key2")=1).filter(Attribute("velocity")>50)).where(Attribute("key1")).equalsTo(Attribute("key2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(30),Minutes(15))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms";
    NES_DEBUG("TIMER SET!!!!");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    //EXPECT_EQ(removeRandomKey(content), removeRandomKey(content));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4f.Test
 * Here, we test if we can use and operator with sliding window (1 Hour, 20 Minutes) for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternAndWithSlidingWindowF) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    //NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //NES_INFO("AndOperatorTest: Coordinator started successfully");

    //NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    //NES_INFO("AndOperatorTest: Worker1 started successfully");

    //NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    //NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV1.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(20);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(20);
    srcConf1->setNumberOfBuffersToProduce(10000);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternFourBAnd.out";
    remove(outputFilePath.c_str());

    //NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50).andWith(Query::from("QnV2").filter(Attribute("velocity")>50)).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Hours(1),Minutes(20))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms";
    NES_DEBUG("TIMER SET!!!!");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    //EXPECT_EQ(removeRandomKey(content), removeRandomKey(content));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4f.Test
 * Here, we test if we can use join operator with sliding window (1 Hour, 20 Minutes) for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, testPatternJoinWithSlidingWindowF) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    //NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //NES_INFO("AndOperatorTest: Coordinator started successfully");

    //NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    //NES_INFO("AndOperatorTest: Worker1 started successfully");

    //NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    //NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV1.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(20);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(20);
    srcConf1->setNumberOfBuffersToProduce(10000);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternFourCAnd.out";
    remove(outputFilePath.c_str());

    //NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").map(Attribute("key1")=1).filter(Attribute("velocity")>50).joinWith(Query::from("QnV2").map(Attribute("key2")=1).filter(Attribute("velocity")>50)).where(Attribute("key1")).equalsTo(Attribute("key2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Hours(1),Minutes(20))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms";
    NES_DEBUG("TIMER SET!!!!");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    //EXPECT_EQ(removeRandomKey(content), removeRandomKey(content));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 6.Test
 * Here, we test if we can use and operator with isEarlyTermination for patterns and create complex events with it
 */
TEST_F(AndOperatorTest, DISABLED_testPatternAndWithEarlyTermination) {
    using std::chrono::high_resolution_clock;
    using std::chrono::duration_cast;
    using std::chrono::duration;
    using std::chrono::milliseconds;

    auto t1 = high_resolution_clock::now();

    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    srcConf1->resetSourceOptions();
    //NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //NES_INFO("AndOperatorTest: Coordinator started successfully");

    //NES_INFO("AndOperatorTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    //NES_INFO("AndOperatorTest: Worker1 started successfully");

    //NES_INFO("QueryDeploymentTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    //NES_INFO("AndOperatorTest: Worker2 started successfully");

    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV1", testSchemaFileName);
    wrk2->registerLogicalStream("QnV2", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setFilePath("../tests/test_data/QnV1.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(20);
    srcConf->setNumberOfBuffersToProduce(10000);
    srcConf->setPhysicalStreamName("test_stream_QnV1");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf1->setSourceType("CSVSource");
    srcConf1->setFilePath("../tests/test_data/QnV2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(20);
    srcConf1->setNumberOfBuffersToProduce(10000);
    srcConf1->setPhysicalStreamName("test_stream_QnV2");
    srcConf1->setLogicalStreamName("QnV2");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf1);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternFourBAnd.out";
    remove(outputFilePath.c_str());

    //NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50).andWith(Query::from("QnV2").filter(Attribute("quantity")>5)).isEarlyTermination(true).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    //NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    auto t2 = high_resolution_clock::now();

    /* Getting number of milliseconds as a double. */
    duration<double, std::milli> ms_double = t2 - t1;
    std::cout << ms_double.count() << "ms";
    NES_DEBUG("TIMER SET!!!!");
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER=" << n);

    //EXPECT_EQ(removeRandomKey(content), removeRandomKey(content));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}