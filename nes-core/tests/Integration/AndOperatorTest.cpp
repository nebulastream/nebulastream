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
#include <Catalogs/Query/QueryCatalogService.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>//for timing execution
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

namespace NES {

using namespace Configurations;

class AndOperatorTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coConf;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AndOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AndOperatorTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        coConf = CoordinatorConfiguration::createDefault();
        coConf->rpcPort = (*rpcCoordinatorPort);
        coConf->restPort = *restPort;
    }

    string removeRandomKey(string contentString) {
        std::regex r2("cep_rightKey([0-9]+)");
        contentString = std::regex_replace(contentString, r2, "cep_rightKey");

        uint64_t start = contentString.find("|QnV1QnV2$start:UINT64");
        uint64_t end = contentString.find("QnV2$cep_rightKey:INT32|\n");
        // Repeat till end is reached
        while (start != std::string::npos) {
            // Replace this occurrence of Sub String
            contentString = contentString.replace(start, end, "");
            // Get the next occurrence from the current position
            start = contentString.find("|QnV1QnV2$start:UINT64", end);
        }

        std::regex r3("\\+?[-]+\\+\\n?");
        contentString = std::regex_replace(contentString, r3, "");
        return contentString;
    }
};

/* 1.Test
 * AND operator standalone with Tumbling Window
 */
TEST_F(AndOperatorTest, testPatternOneSimpleAnd) {
    // Setup Coordinator
    auto window = Schema::create()
                      ->addField(createField("win", BasicType::UINT64))
                      ->addField(createField("id1", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("Win1", window);
    crd->getSourceCatalogService()->registerLogicalSource("Win2", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto srcConf1 = CSVSourceType::create("Win1", "test_stream1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(srcConf1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    // Setup Worker 2
    NES_INFO("AndOperatorTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto srcConf2 = CSVSourceType::create("Win2", "test_stream2");
    srcConf2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(5);
    srcConf2->setNumberOfBuffersToProduce(2);
    auto windowSource2 = PhysicalSource::create(srcConf1);
    workerConfig2->physicalSourceTypes.add(srcConf2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testAndPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("Win1").andWith(Query::from("Win2"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);

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
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto srcConf1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    workerConfig1->physicalSourceTypes.add(srcConf1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    // Setup Worker 2
    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto srcConf2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    srcConf2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(5);
    srcConf2->setNumberOfBuffersToProduce(20);
    workerConfig2->physicalSourceTypes.add(srcConf2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternWithIterationOperator.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity") > 70).andWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "1543622400000,1543622700000,1,R2000070,1543622580000,75.111115,6,1,R2000073,1543622580000,73.166664,5,1\n"
        "1543622400000,1543622700000,1,R2000070,1543622640000,70.222221,7,1,R2000073,1543622580000,73.166664,5,1\n";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);

    ASSERT_TRUE(Util::endsWith(content, expectedContent));

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 3. Test
 * And operator in combination with sliding window, currently disabled as output is inconsistent (order of tuples varies)
 */
TEST_F(AndOperatorTest, DISABLED_testPatternAndWithSlidingWindow) {
    // Setup Coordinator
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    // Add source 1
    auto srcConf1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(12);
    srcConf1->setNumberOfBuffersToProduce(5);
    workerConfig1->physicalSourceTypes.add(srcConf1);
    // Add source 2
    auto srcConf2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    srcConf2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(12);
    srcConf2->setNumberOfBuffersToProduce(5);
    workerConfig1->physicalSourceTypes.add(srcConf2);
    // Start Worker
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternAndSliding.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>70).andWith(Query::from("QnV2").filter(Attribute("velocity")>70)).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(5),Minutes(1))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("AndOperatorTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER={}", n);

    string expectedContent =
        "|1543622520000|1543622820000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622460000|1543622760000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622400000|1543622700000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622340000|1543622640000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n"
        "|1543622580000|1543622880000|1|R2000070|1543622580000|75.111115|6|1|R2000073|1543622580000|73.166664|5|1|\n";

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * And Operator in combination with early termination strategy, currently disabled as early termination implementation is in PR (issue 2339)
 */
TEST_F(AndOperatorTest, DISABLED_testPatternAndWithEarlyTermination) {
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    //Setup Coordinator
    coConf->clear();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    //Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto srcConf1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(5);
    srcConf1->setNumberOfBuffersToProduce(20);
    workerConfig1->physicalSourceTypes.add(srcConf1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    //Setup Worker 2
    NES_INFO("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto srcConf2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    srcConf2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(5);
    srcConf2->setNumberOfBuffersToProduce(20);
    workerConfig2->physicalSourceTypes.add(srcConf2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternAndEarlyTermination.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50).andWith(Query::from("QnV2").filter(Attribute("quantity")>5)).isEarlyTermination(true).window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("AndOperatorTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER={}", n);

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
//
TEST_F(AndOperatorTest, DISABLED_testMultiAndPattern) {
    //Setup Coordinator
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    coConf->clear();
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto srcConf1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    srcConf1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(12);
    srcConf1->setNumberOfBuffersToProduce(5);
    workerConfig1->physicalSourceTypes.add(srcConf1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("AndOperatorTest: Worker1 started successfully");

    // Setup Worker 2
    NES_INFO("AndOperatorTest:  Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto srcConf2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    srcConf2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    srcConf2->setNumberOfTuplesToProducePerBuffer(12);
    srcConf2->setNumberOfBuffersToProduce(5);
    workerConfig2->physicalSourceTypes.add(srcConf2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("AndOperatorTest: Worker2 started successfully");

    // Setup Worker 3
    NES_INFO("AndOperatorTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto srcConf3 = CSVSourceType::create("QnV3", "test_stream_QnV3");
    srcConf3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    srcConf3->setNumberOfTuplesToProducePerBuffer(12);
    srcConf3->setNumberOfBuffersToProduce(5);
    workerConfig3->physicalSourceTypes.add(srcConf3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("AndOperatorTest: Worker3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultiANDPattern.out";
    remove(outputFilePath.c_str());

    NES_INFO("AndOperatorTest: Submit andWith pattern");
    //  Pattern - 1 ANDS - 34 result tuple
    std::string query1 =
        R"(Query::from("QnV")
        .andWith(Query::from("QnV1"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    // Pattern - 2 ANDs
    std::string query2 =
        R"(Query::from("QnV").filter(Attribute("velocity") > 70)
        .andWith(Query::from("QnV1").filter(Attribute("velocity") > 70))
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
        .andWith(Query::from("QnV2").filter(Attribute("velocity") > 70))
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(10),Minutes(2)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    // join query equivalent to query2
    std::string queryjoin =
        R"(Query::from("QnV").filter(Attribute("velocity") > 70).map(Attribute("key1")=1)
        .joinWith(Query::from("QnV1").filter(Attribute("velocity") > 70)
        .map(Attribute("key2")=1)).where(Attribute("key1")).equalsTo(Attribute("key2"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5))).map(Attribute("key4")=1)
        .joinWith(Query::from("QnV2").filter(Attribute("velocity") > 70)
        .map(Attribute("key3")=1)).where(Attribute("key4")).equalsTo(Attribute("key3"))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    std::string query = query2;

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("AndOperatorTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

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

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES