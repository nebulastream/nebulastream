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
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
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
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 80).iter(3,10).sink(FileSinkDescriptor::create(")"
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
}// namespace NES
