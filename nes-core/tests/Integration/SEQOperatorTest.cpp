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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <regex>

namespace fs = std::filesystem;
namespace NES {

using namespace Configurations;

class SeqOperatorTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coConf;
    CSVSourceTypePtr srcConf1;
    CSVSourceTypePtr srcConf2;
    CSVSourceTypePtr srcConf3;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("SeqOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SeqOperatorTest test class.");
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
 * Seq operator standalone with Tumbling Window
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternOneSimpleSeq) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto window = Schema::create()
                      ->addField(createField("win", BasicType::UINT64))
                      ->addField(createField("id1", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("Win1", window);

    auto window2 = Schema::create()
                       ->addField(createField("win", BasicType::UINT64))
                       ->addField(createField("id1", BasicType::UINT64))
                       ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("Win2", window2);

    NES_DEBUG("SeqOperatorTest: Coordinator started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto csvSourceType1 = CSVSourceType::create("Win1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create("Win2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
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
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    NES_INFO("SeqOperatorTest: Remove query");

    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 2.Test
 * SEQ operator in combination with filter
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternOneSeq) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto window = Schema::create()
                      ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                      ->addField(createField("timestamp", BasicType::UINT64))
                      ->addField(createField("velocity", BasicType::FLOAT32))
                      ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", window);

    auto window2 = Schema::create()
                       ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                       ->addField(createField("timestamp", BasicType::UINT64))
                       ->addField(createField("velocity", BasicType::FLOAT32))
                       ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", window2);

    NES_DEBUG("SeqOperatorTest: Coordinator started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType1->setNumberOfBuffersToProduce(5);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SeqOperatorTest: Worker1 started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType2->setNumberOfBuffersToProduce(5);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SeqOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testSeqPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity") > 70).seqWith(Query::from("QnV2").filter(Attribute("velocity") > 65))
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
        "1543622700000,1543623000000,1,R2000070,1543622820000,70.074074,4,1,R2000073,1543622880000,69.388885,7,1\n"
        "1543622700000,1543623000000,1,R2000070,1543622820000,70.074074,4,1,R2000073,1543622940000,66.222221,12,1\n";

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
 * Sequence operator in combination with sliding window, currently disabled as output is inconsistent #2357
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternSeqWithSlidingWindow) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("SeqOperatorTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto window = Schema::create()
                      ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                      ->addField(createField("timestamp", BasicType::UINT64))
                      ->addField(createField("velocity", BasicType::FLOAT32))
                      ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", window);

    auto window2 = Schema::create()
                       ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                       ->addField(createField("timestamp", BasicType::UINT64))
                       ->addField(createField("velocity", BasicType::FLOAT32))
                       ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", window2);

    NES_DEBUG("SeqOperatorTest: Coordinator started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType1->setNumberOfBuffersToProduce(5);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SeqOperatorTest: Worker1 started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType2->setNumberOfBuffersToProduce(5);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SeqOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPatternSeqSliding.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity")>70)
            .seqWith(Query::from("QnV2").filter(Attribute("velocity")>65))
            .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Minutes(5),Minutes(1)))
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
        "|1543626120000|1543626720000|1|R2000070|1543626480000|73.722221|5|1|R2000073|1543626540000|71.444443|10|1|\n"
        "|1543626000000|1543626600000|1|R2000070|1543626480000|73.722221|5|1|R2000073|1543626540000|71.444443|10|1|\n";

    EXPECT_EQ(removeRandomKey(content), expectedContent);

    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
}

/* 4.Test
 * Sequence Operator in combination with early termination strategy, currently disabled as early termination implementation is in PR (issue 2339)
 */
TEST_F(SeqOperatorTest, DISABLED_testPatternSeqWithEarlyTermination) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("MultipleJoinsTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto window = Schema::create()
                      ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                      ->addField(createField("timestamp", BasicType::UINT64))
                      ->addField(createField("velocity", BasicType::FLOAT32))
                      ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", window);

    auto window2 = Schema::create()
                       ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                       ->addField(createField("timestamp", BasicType::UINT64))
                       ->addField(createField("velocity", BasicType::FLOAT32))
                       ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", window2);

    NES_DEBUG("MultipleJoinsTest: Coordinator started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType1->setNumberOfBuffersToProduce(20);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MultipleJoinsTest: Worker1 started successfully");

    NES_DEBUG("MultipleJoinsTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(5);
    csvSourceType2->setNumberOfBuffersToProduce(20);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
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

    NES_INFO("SeqOperatorTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    NES_DEBUG("contents={}", content);
    size_t n = std::count(content.begin(), content.end(), '\n');
    NES_DEBUG("TUPLE NUMBER={}", n);

    string expectedContent =
        "|1543624800000|1543625100000|1|R2000070|1543624980000|90.000000|9|1|R2000070|1543624980000|90.000000|9|1|\n";

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
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("SeqOperatorTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv

    auto window1 = Schema::create()
                       ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                       ->addField(createField("timestamp", BasicType::UINT64))
                       ->addField(createField("velocity", BasicType::FLOAT32))
                       ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV", window1);

    auto window2 = Schema::create()
                       ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                       ->addField(createField("timestamp", BasicType::UINT64))
                       ->addField(createField("velocity", BasicType::FLOAT32))
                       ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV1", window2);

    auto window3 = Schema::create()
                       ->addField("sensor_id", DataTypeFactory::createFixedChar(8))
                       ->addField(createField("timestamp", BasicType::UINT64))
                       ->addField(createField("velocity", BasicType::FLOAT32))
                       ->addField(createField("quantity", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("QnV2", window3);

    NES_DEBUG("SeqOperatorTest: Coordinator started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create("QnV1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType1->setNumberOfBuffersToProduce(5);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SeqOperatorTest: Worker1 started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create("QnV2", "test_stream2");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType2->setNumberOfBuffersToProduce(5);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SeqOperatorTest: Worker2 started successfully");

    NES_DEBUG("SeqOperatorTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto csvSourceType3 = CSVSourceType::create("QnV", "test_stream");
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000070.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(12);
    csvSourceType3->setNumberOfBuffersToProduce(5);
    workerConfig3->physicalSourceTypes.add(csvSourceType3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("SeqOperatorTest: Worker2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testSeqPatternWithTestStream1.out";
    remove(outputFilePath.c_str());

    NES_INFO("SeqOperatorTest: Submit seqWith pattern");

    std::string query =
        R"(Query::from("QnV1").filter(Attribute("velocity") > 70)
        .seqWith(Query::from("QnV2").filter(Attribute("velocity") > 65))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
        .seqWith(Query::from("QnV").filter(Attribute("velocity") > 70))
        .window(TumblingWindow::of(EventTime(Attribute("timestamp")),Minutes(5)))
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

    NES_INFO("SeqOperatorTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    string expectedContent =
        "|1543622700000|1543623000000|1|R2000070|1543622820000|70.074074|4|1|R2000073|1543622880000|69.388885|7|1|\n"
        "|1543622700000|1543623000000|1|R2000070|1543622820000|70.074074|4|1|R2000073|1543622940000|66.222221|12|1|\n";

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