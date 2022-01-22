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

//#include "Configurations/Sources/SourceConfigFactory.hpp"
#include <Util/TestHarness/TestHarness.hpp>

#include <API/QueryAPI.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
//#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <chrono>//for timing execution
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

class AndOperatorTest : public testing::Test {
  public:
    CoordinatorConfigurationPtr coConf;
    CSVSourceTypePtr srcConf1;
    CSVSourceTypePtr srcConf2;
    CSVSourceTypePtr srcConf3;

    static void SetUpTestCase() {
        NES::setupLogging("AndOperatorTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup AndOperatorTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        coConf = CoordinatorConfiguration::create();
        srcConf1 = CSVSourceType::create();
        srcConf2 = CSVSourceType::create();

        coConf->setRpcPort(rpcPort);
        coConf->setRestPort(restPort);
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
 * AND operator standalone with Tumbling Window
 */
TEST_F(AndOperatorTest, testPatternOneSimpleAnd) {
    // Setup Coordinator
    std::string window = R"(Schema::create()->addField(createField("win", UINT64))->addField(createField("id1", UINT64))
                                            ->addField(createField("timestamp", UINT64));)";
    coConf->resetCoordinatorOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getStreamCatalogService()->registerLogicalStream("Win1", window);
    crd->getStreamCatalogService()->registerLogicalStream("Win2", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    srcConf1->setFilePath("../tests/test_data/window.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(2);
    auto windowStream = PhysicalSource::create("Win1", "test_stream1", srcConf1);
    workerConfig1->addPhysicalSource(windowStream);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    // Setup Worker 2
    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setRpcPort(port + 20);
    workerConfig2->setDataPort(port + 21);
    srcConf1->setFilePath("../tests/test_data/window2.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(2);
    auto windowStream2 = PhysicalSource::create("Win2", "test_stream2", srcConf1);
    workerConfig2->addPhysicalSource(windowStream2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig2);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    std::string outputFilePath = "testAndPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("Win1").andWith(Query::from("Win2"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    NES_INFO("AndOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 2.Test
 * And operator in combination with filter
 */
TEST_F(AndOperatorTest, testPatternOneAnd) {
    // Setup Coordinator
    std::string qnv = R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                                         ->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))
                                         ->addField(createField("quantity", UINT64));)";
    coConf->resetCoordinatorOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getStreamCatalogService()->registerLogicalStream("QnV1", qnv);
    crd->getStreamCatalogService()->registerLogicalStream("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    auto windowStream1 = PhysicalSource::create("QnV1", "test_stream_QnV1", srcConf1);
    workerConfig1->addPhysicalSource(windowStream1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    // Setup Worker 2
    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setRpcPort(port + 20);
    workerConfig2->setDataPort(port + 21);
    srcConf2->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(5);
    srcConf2->setNumberOfBuffersToProduce(20);
    auto windowStream2 = PhysicalSource::create("QnV2", "test_stream_QnV2", srcConf2);
    workerConfig2->addPhysicalSource(windowStream2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig2);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");


    std::string outputFilePath = "testAndPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity") > 70).andWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
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
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622400000|1543622700000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622400000|1543622700000|1|R2000070|1543622640000|70.222221|7|1|R2000073|1543622580000|73.166664|5|1|\n"
        "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 3. Test
 * And operator in combination with sliding window, currently disabled as output is inconsistent
 * TODO: sliding windows create different outputs #2357
 *  - test still inconsistent, but working after refactoring of #2312
 */
TEST_F(AndOperatorTest, DISABLED_testPatternAndWithSlidingWindow) {
    // Setup Coordinator
    std::string qnv = R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                                         ->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))
                                         ->addField(createField("quantity", UINT64));)";
    coConf->resetCoordinatorOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getStreamCatalogService()->registerLogicalStream("QnV1", qnv);
    crd->getStreamCatalogService()->registerLogicalStream("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    // Add source 1
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    auto windowStream1 = PhysicalSource::create("QnV1", "test_stream_QnV1", srcConf1);
    workerConfig1->addPhysicalSource(windowStream1);
    // Add source 2
    srcConf2->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(5);
    srcConf2->setNumberOfBuffersToProduce(20);
    auto windowStream2 = PhysicalSource::create("QnV2", "test_stream_QnV2", srcConf2);
    workerConfig1->addPhysicalSource(windowStream2);
    // Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    std::string outputFilePath = "testPatternAndSliding.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>70).andWith(Query::from("QnV2").filter(Attribute("velocity")>70)).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(5),Minutes(1))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("AndOperatorTest: Remove query");
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
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622580000|1543622880000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622520000|1543622820000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622460000|1543622760000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622400000|1543622700000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622340000|1543622640000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "+----------------------------------------------------+";

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * And Operator in combination with early termination strategy, currently disabled as early termination implementation is in PR
 */
//TODO Ariane issue 2339
// - test runs through and always fails after refactoring of #2312
TEST_F(AndOperatorTest, DISABLED_testPatternAndWithEarlyTermination) {
    std::string qnv = R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                                         ->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))
                                         ->addField(createField("quantity", UINT64));)";
    //Setup Coordinator
    coConf->resetCoordinatorOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getStreamCatalogService()->registerLogicalStream("QnV1", qnv);
    crd->getStreamCatalogService()->registerLogicalStream("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    auto windowStream1 = PhysicalSource::create("QnV1", "test_stream_QnV1", srcConf1);
    workerConfig1->addPhysicalSource(windowStream1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    //Setup Worker 2
    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->setCoordinatorPort(port);
    workerConfig2->setRpcPort(port + 20);
    workerConfig2->setDataPort(port + 21);
    srcConf2->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(5);
    srcConf2->setNumberOfBuffersToProduce(20);
    auto windowStream2 = PhysicalSource::create("QnV2", "test_stream_QnV2", srcConf2);
    workerConfig2->addPhysicalSource(windowStream2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig2);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    std::string outputFilePath = "testPatternAndEarlyTermination.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

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

    NES_INFO("AndOperatorTest: Remove query");
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

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 5.Test
 * Multi-AND Operators in one Query
 */
//TODO Ariane issue 2303
// - test always fails after refactoring of #2312
TEST_F(AndOperatorTest, DISABLED_testMultiAndPattern) {
    //Setup Coordinator
    std::string qnv = R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                                         ->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))
                                         ->addField(createField("quantity", UINT64));)";
    coConf->resetCoordinatorOptions();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getStreamCatalogService()->registerLogicalStream("QnV", qnv);
    crd->getStreamCatalogService()->registerLogicalStream("QnV2", qnv);
    crd->getStreamCatalogService()->registerLogicalStream("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->setCoordinatorPort(port);
    workerConfig1->setRpcPort(port + 10);
    workerConfig1->setDataPort(port + 11);
    workerConfig1->setQueryCompilerCompilationStrategy("DEBUG");
    //Add Source 1
    srcConf1->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(6);
    srcConf1->setNumberOfBuffersToProduce(10);
    auto windowStream1 = PhysicalSource::create("QnV", "test_stream_R2000070", srcConf1);
    workerConfig1->addPhysicalSource(windowStream1);
    //Add Source 2
    srcConf2->setFilePath("../tests/test_data/QnV_short_R2000070.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(6);
    srcConf2->setNumberOfBuffersToProduce(10);
    auto windowStream2 = PhysicalSource::create("QnV2", "test_stream_R20000702", srcConf2);
    workerConfig1->addPhysicalSource(windowStream2);
    //Add Source 3
    srcConf3->setFilePath("../tests/test_data/QnV_short_R2000073.csv");
    srcConf3->setNumberOfTuplesToProducePerBuffer(6);
    srcConf3->setNumberOfBuffersToProduce(10);
    auto windowStream3 = PhysicalSource::create("QnV1", "test_stream_R2000073", srcConf3);
    workerConfig1->addPhysicalSource(windowStream3);
    //Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig1);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    std::string outputFilePath = "testMultiAndPatternWithTestStream.out";
    remove(outputFilePath.c_str());

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

    NES_INFO("QueryDeploymentTest: Remove query");

    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES