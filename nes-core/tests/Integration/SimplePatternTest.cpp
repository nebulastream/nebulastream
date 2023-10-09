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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

//used tests: QueryCatalogServiceTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

using namespace Configurations;

class SimplePatternTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coConf;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SimplePatternTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SimplePatternTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        coConf = CoordinatorConfiguration::createDefault();

        coConf->rpcPort = (*rpcCoordinatorPort);
        coConf->restPort = *restPort;
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
 * Here, we test the translation of a simple pattern (1 Source) into a query using a real data set (QnV) and check the output
 * TODO: Ariane
 */
TEST_F(SimplePatternTest, DISABLED_testPatternWithTestSourceSingleOutput) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    //register logical source qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))->addField(createField("quantity", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    //register physical source
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + R"(")).selectionPolicy("Single_Output"); )";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    //    ASSERT_TRUE(queryService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent = "+----------------------------------------------------+\n"
                             "|QnV$sensor_id:CHAR|QnV$timestamp:UINT64|QnV$velocity:FLOAT32|QnV$quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "|R2000073|1543624020000|102.629631|8|\n"
                             "|R2000070|1543625280000|108.166664|5|\n"
                             "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content={}", content);
    NES_DEBUG("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/* 2.Test
  * Iteration Operator with min and max occurrences of the event
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperator) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    //register logical source qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))->addField(createField("quantity", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(35);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    //register physical source
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV")
                            .filter(Attribute("velocity") > 70)
                            .times(3,10)
                            .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
                            .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    //    NES_INFO("SimplePatternTest: Remove query");
    //    queryService->validateAndQueueStopQueryRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "QnV$start:INTEGER(64 bits),QnV$end:INTEGER(64 bits),QnV$Count:INTEGER(32 bits),QnV$timestamp:INTEGER(64 bits)\n"
        "1543622280000,1543622880000,3,0\n"
        "1543622400000,1543623000000,3,0\n"
        "1543622520000,1543623120000,3,0\n"
        "1543622640000,1543623240000,3,0\n"
        "1543623120000,1543623720000,4,0\n"
        "1543623240000,1543623840000,3,0\n"
        "1543623360000,1543623960000,3,0\n"
        "1543623480000,1543624080000,3,0\n"
        "1543624440000,1543625040000,3,0\n"
        "1543624560000,1543625160000,3,0\n"
        "1543624680000,1543625280000,3,0\n"
        "1543624800000,1543625400000,3,0\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content={}", content);
    NES_DEBUG("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/* 3.Test
  * Iteration Operator exact number of event occurrences
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperatorExactOccurance) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))->addField(createField("quantity", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(35);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    //register physical source
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV")
                            .filter(Attribute("velocity") > 65)
                            .times(5)
                            .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
                            .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    //    queryService->validateAndQueueStopQueryRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "QnV$start:INTEGER(64 bits),QnV$end:INTEGER(64 bits),QnV$Count:INTEGER(32 bits),QnV$timestamp:INTEGER(64 bits)\n"
        "1543622280000,1543622880000,5,0\n"
        "1543622640000,1543623240000,5,0\n"
        "1543623240000,1543623840000,5,0\n"
        "1543623360000,1543623960000,5,0\n"
        "1543623600000,1543624200000,5,0\n"
        "1543624440000,1543625040000,5,0\n"
        "1543624560000,1543625160000,5,0\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content={}", content);
    NES_DEBUG("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/* 4.Test
  * Iteration Operator unbounded event occurrences
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperatorUnbounded) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))->addField(createField("quantity", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(35);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    //register physical source
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV")
                            .filter(Attribute("quantity") > 9)
                            .times()
                            .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
                            .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    //    queryService->validateAndQueueStopQueryRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "QnV$start:INTEGER(64 bits),QnV$end:INTEGER(64 bits),QnV$Count:INTEGER(32 bits),QnV$timestamp:INTEGER(64 bits)\n"
        "1543622160000,1543622760000,1,0\n"
        "1543622280000,1543622880000,1,0\n"
        "1543622400000,1543623000000,1,0\n"
        "1543622520000,1543623120000,1,0\n"
        "1543622640000,1543623240000,1,0\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content={}", content);
    NES_DEBUG("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/* 5.Test
  * Iteration Operator unbounded event occurrences, special case (0,5)
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperator0Max) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))->addField(createField("quantity", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(35);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    //register physical stream
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV")
                            .filter(Attribute("velocity") > 65)
                            .times(0,5)
                            .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
                            .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    //    queryService->validateAndQueueStopQueryRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "QnV$start:INTEGER(64 bits),QnV$end:INTEGER(64 bits),QnV$Count:INTEGER(32 bits),QnV$timestamp:INTEGER(64 bits)\n"
        "1543622280000,1543622880000,5,0\n"
        "1543622640000,1543623240000,5,0\n"
        "1543623240000,1543623840000,5,0\n"
        "1543623360000,1543623960000,5,0\n"
        "1543623600000,1543624200000,5,0\n"
        "1543624440000,1543625040000,5,0\n"
        "1543624560000,1543625160000,5,0\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content={}", content);
    NES_DEBUG("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/* 6.Test
  * Iteration Operator unbounded event occurrences, special case (5,0)
 */
TEST_F(SimplePatternTest, testPatternWithIterationOperatorMin0) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    //register logical stream qnv
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", BasicType::UINT64))->addField(createField("velocity", BasicType::FLOAT32))->addField(createField("quantity", BasicType::UINT64));)";
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    NES_DEBUG("coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = port;
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(35);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    //register physical source
    PhysicalSourcePtr conf70 = PhysicalSource::create("QnV", "test_stream", csvSourceType1);
    worker1Configuration->physicalSources.add(conf70);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Query::from("QnV")
                            .filter(Attribute("velocity") > 65)
                            .times(5,0)
                            .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
                            .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    //    queryService->validateAndQueueStopQueryRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "QnV$start:INTEGER(64 bits),QnV$end:INTEGER(64 bits),QnV$Count:INTEGER(32 bits),QnV$timestamp:INTEGER(64 bits)\n"
        "1543622280000,1543622880000,5,0\n"
        "1543622400000,1543623000000,6,0\n"
        "1543622520000,1543623120000,6,0\n"
        "1543622640000,1543623240000,5,0\n"
        "1543623240000,1543623840000,5,0\n"
        "1543623360000,1543623960000,5,0\n"
        "1543623480000,1543624080000,6,0\n"
        "1543623600000,1543624200000,5,0\n"
        "1543624440000,1543625040000,5,0\n"
        "1543624560000,1543625160000,5,0\n";

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_DEBUG("content={}", content);
    NES_DEBUG("expContent={}", expectedContent);
    ASSERT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}
}// namespace NES
