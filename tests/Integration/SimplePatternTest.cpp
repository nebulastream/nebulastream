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
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>

//used tests: QueryCatalogTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class SimplePatternTest : public testing::Test {
  public:
    CoordinatorConfigPtr coConf;
    WorkerConfigPtr wrkConf;
    SourceConfigPtr srcConf;

    static void SetUpTestCase() {
        NES::setupLogging("SimplePatternTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SimplePatternTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        coConf = CoordinatorConfig::create();
        wrkConf = WorkerConfig::create();
        srcConf = SourceConfig::create();

        coConf->setRpcPort(rpcPort);
        coConf->setRestPort(restPort);
        wrkConf->setCoordinatorPort(rpcPort);
    }

    void TearDown() override { std::cout << "Tear down SimplePatternTest class." << std::endl; }
    uint64_t restPort = 8081;
};

/* 1. Test
 * Translation of a simple pattern (1 Stream) into a query
 */
TEST_F(SimplePatternTest, testPatternWithFilter) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    NES_DEBUG("SimplePatternTest: start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0u);
    NES_DEBUG("SimplePatternTest: coordinator started successfully");

    NES_DEBUG("SimplePatternTest: start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("SimplePatternTest: worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string query =
        R"(Pattern::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("SimplePatternTest: stop worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("SimplePatternTest: stop coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("SimplePatternTest: test finished");
}

/* 2.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 */
TEST_F(SimplePatternTest, testPatternWithTestStream) {
    coConf->resetCoordinatorOptions();
    wrkConf->resetWorkerOptions();
    srcConf->resetSourceOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0U);
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
    srcConf->setSourceConfig("../tests/test_data/QnV_short.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    //TODO Patternname waiting for String support in map operator
    /// XXX:
    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnV$sensor_id:CHAR[8]|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|QnV$PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543624020000|102.629631|8|1|\n"
        "|R2000070|1543625280000|108.166664|5|1|\n"
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
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 */
TEST_F(SimplePatternTest, testPatternWithTestStreamAndMultiWorkers) {
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

    NES_INFO("SimplePatternTest: Start worker 2");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SimplePatternTest: Worker2 started successfully");

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
    srcConf->setSourceConfig("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000070");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/QnV_short_R2000073.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000073");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;
    bool resultWrk3 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = UtilityFunctions::splitWithStringDelimiter(line, "|");
        for (const auto& keyWord : content) {
            if (keyWord == "R2000073") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): found=R2000073");
                resultWrk1 = true;
            } else if (keyWord == "1543625280000") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): found= 108.166664");
                resultWrk2 = true;
            } else if (keyWord == "102.629631") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): found= 102.629631");
                resultWrk3 = true;
            }
        }
    }

    EXPECT_TRUE((resultWrk1 && resultWrk2 && resultWrk3));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 */
TEST_F(SimplePatternTest, testPatternWithWindowandAggregation) {
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
    srcConf->setSourceConfig("../tests/test_data/QnV_short_intID.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testPatternWithWindowandAggregation.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query =
        R"(Pattern::from("QnV").window(SlidingWindow::of(EventTime(Attribute("timestamp")), Minutes(15), Minutes(5))).byKey(Attribute("sensor_id")).apply(Sum(Attribute("quantity"))).filter(Attribute("quantity") > 105).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 0));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    //TODO Patternname waiting for String support in map operator

    string expectedContent = "+----------------------------------------------------+\n"
                             "|QnV$start:UINT64|QnV$end:UINT64|QnV$sensor_id:UINT64|QnV$quantity:UINT64|QnV$PatternId:INT32|\n"
                             "+----------------------------------------------------+\n"
                             "|1543622400000|1543623300000|2000073|107|1|\n"
                             "|1543623600000|1543624500000|2000073|107|1|\n"
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

/* 5.Test
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
    srcConf->setSourceConfig("../tests/test_data/QnV_short.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(")).selectionPolicy("Single_Output"); )";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    EXPECT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    //TODO Patternname waiting for String support in map operator

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnV$sensor_id:CHAR|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|_$PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543624020000|102.629631|8|1|\n"
        "|R2000070|1543625280000|108.166664|5|1|\n"
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

/* 6.Test
 * Here, we test if we can use merge operator for patterns and create complex events with it
 */
TEST_F(SimplePatternTest, testPatternWithTestStreamAndMultiWorkerMerge) {
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
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);
    wrk2->registerLogicalStream("QnV1", testSchemaFileName);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/QnV_short_R2000070.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000070");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf70);

    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/QnV_short_R2000073.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream_R2000073");
    srcConf->setLogicalStreamName("QnV1");
    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create(srcConf);
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath = "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    NES_INFO("SimplePatternTest: Submit unionWith pattern");
    std::string query =
        R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).unionWith(Pattern::from("QnV1").filter(Attribute("velocity") > 100)).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    bool resultWrk1 = false;
    bool resultWrk2 = false;
    bool resultWrk3 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        std::vector<string> content = UtilityFunctions::splitWithStringDelimiter(line, "|");
        for (const auto& keyWord : content) {
            if (keyWord == "R2000073") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): found=R2000073");
                resultWrk1 = true;
            } else if (keyWord == "1543625280000") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): found= 108.166664");
                resultWrk2 = true;
            } else if (keyWord == "102.629631") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): found= 102.629631");
                resultWrk3 = true;
            }
        }
    }

    EXPECT_TRUE((resultWrk1 && resultWrk2 && resultWrk3));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 8.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
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
    srcConf->setSourceConfig("../tests/test_data/QnV_short_intID.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(0);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("QnV");
    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath = "testPatternWithWindowandAggregation.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query =
        R"(Pattern::from("QnV").filter(Attribute("velocity") > 80).iter(3,10).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 0));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    //TODO Patternname waiting for String support in map operator

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnV$sensor_id:UINT64|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|QnV$PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|2000070|1543625280000|108.166664|5|1|\n"
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
}// namespace NES
