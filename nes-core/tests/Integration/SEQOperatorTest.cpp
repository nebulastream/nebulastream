/*
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
#include "../util/NesBaseTest.hpp"
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <API/QueryAPI.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
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

class SeqOperatorTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("SeqOperatorTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SeqOperatorTest test class.");
    }

    string removeRandomKey(string contentString) {
        std::regex r1("cep_leftkey([0-9]+)");
        std::regex r2("cep_rightkey([0-9]+)");
        contentString = std::regex_replace(contentString, r1, "cep_leftkey");
        contentString = std::regex_replace(contentString, r2, "cep_rightkey");
        return contentString;
    }
};

/* 1.Test
 * Seq operator standalone with Tumbling Window
 */
TEST_F(SeqOperatorTest, testPatternOneSimpleSeq) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField(createField("win", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("Win1", window);

    std::string window2 =
        R"(Schema::create()->addField(createField("win", UINT64))->addField(createField("id1", UINT64))->addField(createField("timestamp", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("Win2", window2);

    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("Win1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    auto physicalSource2 = PhysicalSource::create("Win2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testSeqPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("Win1").seqWith(Query::from("Win2"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    NES_INFO("SeqOperatorTest: Remove query");
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
 * SEQ operator in combination with filter
 * TODO: output changes #2357
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternOneSeq) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV1", window);

    std::string window2 =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV2", window2);

    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(20);
    auto physicalSource1 = PhysicalSource::create("QnV1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(20);
    auto physicalSource2 = PhysicalSource::create("QnV2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testSeqPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity") > 70).seqWith(Query::from("QnV2").filter(Attribute("velocity") > 60))
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

    NES_INFO("SeqOperatorTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622400000|1543622700000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622640000|64.777779|10|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622700000|1543623000000|1|R2000070|1543622820000|70.074074|4|1|R2000073|1543622880000|69.388885|7|1|\n"
        "|1543622700000|1543623000000|1|R2000070|1543622820000|70.074074|4|1|R2000073|1543622940000|66.222221|12|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543623300000|1543623600000|1|R2000070|1543623480000|78.555557|5|1|R2000073|1543623540000|62.055557|10|1|\n"
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
 * Sequence operator in combination with sliding window, currently disabled as output is inconsistent
 * TODO: output changes #2357
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternSeqWithSlidingWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV1", window);

    std::string window2 =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV2", window2);

    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(20);
    auto physicalSource1 = PhysicalSource::create("QnV1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(20);
    auto physicalSource2 = PhysicalSource::create("QnV2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternSeqSliding.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>66).seqWith(Query::from("QnV2").filter(Attribute("velocity")>62)).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2))).sink(FileSinkDescriptor::create(")"
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
        "|1543622040000|1543622640000|1|R2000070|1543622520000|66.566666|3|1|R2000073|1543622580000|73.166664|5|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622280000|1543622880000|1|R2000070|1543622520000|66.566666|3|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622160000|1543622760000|1|R2000070|1543622520000|66.566666|3|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622280000|1543622880000|1|R2000070|1543622640000|70.222221|7|1|R2000073|1543622700000|64.111115|7|1|\n"
        "|1543622160000|1543622760000|1|R2000070|1543622640000|70.222221|7|1|R2000073|1543622700000|64.111115|7|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622400000|1543623000000|1|R2000070|1543622520000|66.566666|3|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622400000|1543623000000|1|R2000070|1543622640000|70.222221|7|1|R2000073|1543622700000|64.111115|7|1|\n"
        "|1543622400000|1543623000000|1|R2000070|1543622880000|68.944443|8|1|R2000073|1543622940000|66.222221|12|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622520000|1543623120000|1|R2000070|1543622520000|66.566666|3|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622520000|1543623120000|1|R2000070|1543622640000|70.222221|7|1|R2000073|1543622700000|64.111115|7|1|\n"
        "|1543622520000|1543623120000|1|R2000070|1543622880000|68.944443|8|1|R2000073|1543622940000|66.222221|12|1|\n"
        "+----------------------------------------------------++----------------------------------------------------+\n"
        "|QnV1QnV2$start:UINT64|QnV1QnV2$end:UINT64|QnV1QnV2$key:INT32|QnV1$sensor_id:CHAR[8]|QnV1$timestamp:UINT64|QnV1$"
        "velocity:FLOAT32|QnV1$quantity:UINT64|QnV1$cep_leftkey:INT32|QnV2$sensor_id:CHAR[8]|QnV2$timestamp:UINT64|QnV2$velocity:"
        "FLOAT32|QnV2$quantity:UINT64|QnV2$cep_rightkey:INT32|\n"
        "+----------------------------------------------------+\n"
        "|1543622640000|1543623240000|1|R2000070|1543622640000|70.222221|7|1|R2000073|1543622700000|64.111115|7|1|\n"
        "|1543622880000|1543623480000|1|R2000070|1543622880000|68.944443|8|1|R2000073|1543622940000|66.222221|12|1|\n"
        "|1543622760000|1543623360000|1|R2000070|1543622880000|68.944443|8|1|R2000073|1543622940000|66.222221|12|1|\n"
        "|1543622640000|1543623240000|1|R2000070|1543622880000|68.944443|8|1|R2000073|1543622940000|66.222221|12|1|\n"
        "+----------------------------------------------------+";

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * Sequence Operator in combination with early termination strategy, currently disabled as early termination implementation is in PR
 * //TODO Ariane issue 2339
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternSeqWithEarlyTermination) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv
    std::string window =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV1", window);

    std::string window2 =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV2", window2);

    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(20);
    auto physicalSource1 = PhysicalSource::create("QnV1", "test_stream", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(20);
    auto physicalSource2 = PhysicalSource::create("QnV2", "test_stream", csvSourceType2);
    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternSeqEarlyTermination.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50).seqWith(Query::from("QnV2").filter(Attribute("quantity")>5)).isEarlyTermination(true).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
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

    NES_INFO("SeqOperatorTest: Remove query");
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

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 5.Test
 * Multi-Sequence Operators in one Query
 */
//TODO Ariane issue 2303
TEST_F(SeqOperatorTest, DISABLED_testMultiSeqPattern) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical stream qnv

    std::string window1 =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV", window1);

    std::string window2 =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV1", window2);

    std::string window3 =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    crd->getStreamCatalogService()->registerLogicalSource("QnV2", window3);

    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(6);
    csvSourceType1->setNumberOfBuffersToProduce(10);
    auto physicalSource1 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    auto csvSourceType2 = CSVSourceType::create();
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(6);
    csvSourceType2->setNumberOfBuffersToProduce(10);
    auto physicalSource2 = PhysicalSource::create("QnV1", "test_stream", csvSourceType2);
    auto csvSourceType3 = CSVSourceType::create();
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "QnV_short_R2000070.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(6);
    csvSourceType3->setNumberOfBuffersToProduce(10);
    auto physicalSource3 = PhysicalSource::create("QnV2", "test_stream", csvSourceType3);
    workerConfig1->physicalSources.add(physicalSource1);
    workerConfig1->physicalSources.add(physicalSource2);
    workerConfig1->physicalSources.add(physicalSource3);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;

    workerConfig2->physicalSources.add(physicalSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("MultipleJoinsTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultiSeqPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("SeqOperatorTest: Submit andWith pattern");
    //  Pattern - 1 SeqS - 34 result tuple
    std::string query1 =
        R"(Query::from("QnV").filter(Attribute("velocity") > 65)
        .seqWith(Query::from("QnV1").filter(Attribute("velocity") > 65))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    // Pattern - 2 Seqs
    std::string query2 =
        R"(Query::from("QnV").filter(Attribute("velocity") > 70)
        .seqWith(Query::from("QnV1").filter(Attribute("velocity") > 70))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .seqWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
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

    NES_INFO("SeqOperatorTest: Remove query");

    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents=" << content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES