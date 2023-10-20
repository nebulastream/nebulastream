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
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Common.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>//for timing execution
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

//used tests: QueryCatalogServiceTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

using namespace Configurations;

class OrOperatorTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coordinatorConfiguration;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("OrOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup OrOperatorTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        coordinatorConfiguration = CoordinatorConfiguration::createDefault();
        coordinatorConfiguration->rpcPort = (*rpcCoordinatorPort);
        coordinatorConfiguration->restPort = *restPort;
    }
};

/* 1.Test
 * OR operator standalone
 */
TEST_F(OrOperatorTest, testPatternOneOr) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("OrOperatorTest: Coordinator started successfully");

    //register logical source qnv
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));

    auto sourceCatalogService = crd->getSourceCatalogService();
    sourceCatalogService->registerLogicalSource("QnV1", qnv);
    sourceCatalogService->registerLogicalSource("QnV2", qnv);

    NES_INFO("OrOperatorTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(20);
    worker1Configuration->physicalSourceTypes.add(csvSourceType1);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("OrOperatorTest: Worker1 started successfully");

    NES_INFO("OrOperatorTest: Start worker 2 with physical source");
    auto worker2Configuration = WorkerConfiguration::create();
    worker2Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(20);
    worker2Configuration->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(worker2Configuration));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("OrOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternOr1.out";
    remove(outputFilePath.c_str());

    NES_INFO("OrOperatorTest: Submit orWith pattern");

    std::string query =
        R"(Query::from("QnV1").orWith(Query::from("QnV2")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";

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

    NES_INFO("OrOperatorTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER={}", n);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 2.Test
 * OR operator in combination with additional map and filter
 */
TEST_F(OrOperatorTest, testPatternOrMap) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    NES_INFO("OrOperatorTest: Coordinator started successfully");

    NES_INFO("OrOperatorTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(40);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    worker1Configuration->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("OrOperatorTest: Worker1 started successfully");

    NES_INFO("OrOperatorTest: Start worker 2 with physical source");
    auto worker2Configuration = WorkerConfiguration::create();
    worker2Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(35);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    worker2Configuration->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(worker2Configuration));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("OrOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternOr2.out";
    remove(outputFilePath.c_str());

    NES_INFO("OrOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").map(Attribute("Name")="test").orWith(Query::from("QnV2").map(Attribute("Name")="test").filter(Attribute("velocity")>60)).sink(FileSinkDescriptor::create(")"
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

    NES_INFO("OrOperatorTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER={}", n);
    size_t expResult = 119L;

    EXPECT_EQ(n, expResult);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 3.Test
 * Multi-OR Operators in one Query
 * //TODO Disabled waiting for issue #2600
 */
TEST_F(OrOperatorTest, DISABLED_testPatternMultiOr) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV3", qnv);
    NES_INFO("OrOperatorTest: Coordinator started successfully");

    NES_INFO("OrOperatorTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream_QnV1");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(2);
    csvSourceType1->setNumberOfBuffersToProduce(35);
    worker1Configuration->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("OrOperatorTest: Worker1 started successfully");

    NES_INFO("OrOperatorTest: Start worker 2 with physical source");
    auto worker2Configuration = WorkerConfiguration::create();
    worker2Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream_QnV2");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(2);
    csvSourceType2->setNumberOfBuffersToProduce(35);
    worker2Configuration->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(worker2Configuration));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("OrOperatorTest: Worker2 started successfully");

    NES_INFO("OrOperatorTest: Start worker 3 with physical source");
    auto worker3Configuration = WorkerConfiguration::create();
    worker3Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType3 = CSVSourceType::create("QnV3", "test_stream_QnV3");
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(2);
    csvSourceType3->setNumberOfBuffersToProduce(35);
    worker3Configuration->physicalSourceTypes.add(csvSourceType3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(worker3Configuration));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("OrOperatorTest: Worker3 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternOR3.out";
    remove(outputFilePath.c_str());

    NES_INFO("OrOperatorTest: Submit andWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>50)
        .orWith(Query::from("QnV2").filter(Attribute("quantity")>5)
        .orWith(Query::from("QnV3").filter(Attribute("quantity")>7)))
        .sink(FileSinkDescriptor::create(")"
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

    NES_INFO("OrOperatorTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER={}", n);

    EXPECT_EQ(n, 365L);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * OR Operators with filters left and right source
 * //TODO Disabled waiting for issue #4195
 */
TEST_F(OrOperatorTest, DISABLED_testOrPatternFilter) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfiguration);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto qnv = Schema::create()
                   ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                   ->addField(createField("timestamp", BasicType::UINT64))
                   ->addField(createField("velocity", BasicType::FLOAT32))
                   ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV", qnv);
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", qnv);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("OrOperatorTest: Start worker 1 with physical source");
    auto worker1Configuration = WorkerConfiguration::create();
    worker1Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType1 = CSVSourceType::create("QnV", "test_stream_R2000070");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    worker1Configuration->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(worker1Configuration));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("OrOperatorTest: Worker1 started successfully");

    NES_INFO("OrOperatorTest: Start worker 2 with physical source");
    auto worker2Configuration = WorkerConfiguration::create();
    worker2Configuration->coordinatorPort = (port);
    //Add Physical source
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream_R2000073");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(0);
    worker2Configuration->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(worker2Configuration));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("OrOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testOrPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    NES_INFO("SimplePatternTest: Submit orWith pattern");

    std::string query =
        R"(Query::from("QnV").filter(Attribute("velocity") > 100)
        .orWith(Query::from("QnV2").filter(Attribute("velocity") > 100))
        .sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    NES_INFO("SimplePatternTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string resultContent;
    std::string line;
    while (std::getline(ifs, line)) {
        resultContent += line + "\n";
    }
    ifs.close();

    NES_DEBUG("OrOperatorTest: Result produced content {}", resultContent);

    std::string expectedContent =
        "QnV$sensor_id:ArrayType,QnV$timestamp:INTEGER(64 bits),QnV$velocity:Float(32 bits),QnV$quantity:INTEGER(64 bits)\n"
        "R2000070,1543625280000,108.166664,5\n"
        "R2000073,1543624020000,102.629631,8\n";

    EXPECT_EQ(expectedContent, resultContent);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES
