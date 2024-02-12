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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <BaseIntegrationTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop

#include <API/QueryAPI.hpp>
#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

#define DEBUG_OUTPUT
namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case

class ContinuousSourceTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ContinuousSourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ContinuousSourceTest test class.");
    }
};

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourcePrint) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto defaultSourceType1 = DefaultSourceType::create("testStream", "test_stream");
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(PrintSinkDescriptor::create());

    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourcePrintWithLargerFrequency) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto defaultSourceType1 = DefaultSourceType::create("testStream", "test_stream");
    defaultSourceType1->setSourceGatheringInterval(3);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(PrintSinkDescriptor::create());

    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteFile) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto defaultSourceType1 = DefaultSourceType::create("testStream", "test_stream");
    defaultSourceType1->setSourceGatheringInterval(1);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultipleOutputBufferFromDefaultSourceWriteFile.txt";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(FileSinkDescriptor::create(outputFilePath));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    //ASSERT_TRUE(requestHandlerService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "testStream$campaign_id:INTEGER(64 bits)\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n";

    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;

    std::string testOut = "expect.txt";
    std::ofstream outT(testOut);
    outT << expectedContent;
    outT.close();

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromDefaultSourceWriteFileWithLargerFrequency) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    auto defaultSourceType1 = DefaultSourceType::create("testStream", "test_stream");
    defaultSourceType1->setSourceGatheringInterval(1);
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath =
        getTestResourceFolder() / "testMultipleOutputBufferFromDefaultSourceWriteFileWithLargerFrequency.txt";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("campaign_id") < 42).sink(FileSinkDescriptor::create(outputFilePath));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 3));

    NES_INFO("QueryDeploymentTest: Remove query");
    //ASSERT_TRUE(requestHandlerService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "testStream$campaign_id:INTEGER(64 bits)\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n"
                             "1\n";

    cout << "content=" << content << endl;
    cout << "expContent=" << expectedContent << endl;

    std::string testOut = "expect.txt";
    std::ofstream outT(testOut);
    outT << expectedContent;
    outT.close();

    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromCSVSourcePrint) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()
                          ->addField(createField("val1", BasicType::UINT64))
                          ->addField(createField("val2", BasicType::UINT64))
                          ->addField(createField("val3", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    std::string testCSV = "1,2,3\n"
                          "1,2,4\n"
                          "4,3,6";
    std::string testCSVFileName = "testCSV.csv";
    std::ofstream outCsv(testCSVFileName);
    outCsv << testCSV;
    outCsv.close();
    auto csvSourceType1 = CSVSourceType::create("testStream", "test_stream");
    csvSourceType1->setFilePath("testCSV.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("val1") < 2).sink(PrintSinkDescriptor::create());
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    //ASSERT_TRUE(requestHandlerService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testMultipleOutputBufferFromCSVSourceWrite) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()
                          ->addField(createField("val1", BasicType::UINT64))
                          ->addField(createField("val2", BasicType::UINT64))
                          ->addField(createField("val3", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    std::string testCSV = "1,2,3\n"
                          "1,2,4\n"
                          "4,3,6";
    std::string testCSVFileName = "testCSV.csv";
    std::ofstream outCsv(testCSVFileName);
    outCsv << testCSV;
    outCsv.close();
    auto csvSourceType1 = CSVSourceType::create("testStream", "test_stream");
    csvSourceType1->setFilePath("testCSV.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testMultipleOutputBufferFromCSVSourceWriteTest.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("val1") < 10).sink(FileSinkDescriptor::create(outputFilePath));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    //ASSERT_TRUE(requestHandlerService->validateAndQueueStopQueryRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent =
        "testStream$val1:INTEGER(64 bits),testStream$val2:INTEGER(64 bits),testStream$val3:INTEGER(64 bits)\n"
        "1,2,3\n"
        "1,2,4\n"
        "4,3,6\n";
    NES_INFO("ContinuousSourceTest: content={}", content);
    NES_INFO("ContinuousSourceTest: expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(ContinuousSourceTest, testTimestampCsvSink) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("ContinuousSourceTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    auto testSchema = Schema::create()
                          ->addField(createField("val1", BasicType::UINT64))
                          ->addField(createField("val2", BasicType::UINT64))
                          ->addField(createField("val3", BasicType::UINT64));
    crd->getSourceCatalogService()->registerLogicalSource("testStream", testSchema);
    NES_DEBUG("ContinuousSourceTest: Coordinator started successfully");

    NES_DEBUG("ContinuousSourceTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    std::string testCSV = "1,2,3\n"
                          "1,2,4\n"
                          "4,3,6";
    std::string testCSVFileName = "testCSV.csv";
    std::ofstream outCsv(testCSVFileName);
    outCsv << testCSV;
    outCsv.close();
    auto csvSourceType1 = CSVSourceType::create("testStream", "test_stream");
    csvSourceType1->setFilePath("testCSV.csv");
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("ContinuousSourceTest: Worker1 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTimestampCsvSink.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    //register query
    auto query = Query::from("testStream").filter(Attribute("val1") < 10).sink(FileSinkDescriptor::create(outputFilePath, true));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("QueryDeploymentTest: Remove query");
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    ASSERT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    NES_INFO("ContinuousSourceTest: content=\n{}", content);
    EXPECT_EQ(countOccurrences("\n", content), 4);
    EXPECT_EQ(countOccurrences(",", content), 3 * 4);

    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

/*
 * Testing test harness CSV source
 */
TEST_F(ContinuousSourceTest, testWithManyInputBuffer) {
    uint64_t numBufferToProduce = 1000;

    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create("car", "car_p1");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "long_running.csv");
    csvSourceType->setGatheringInterval(0);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType->setNumberOfBuffersToProduce(numBufferToProduce);
    csvSourceType->setSkipHeader(false);

    auto queryWithFilterOperator = Query::from("car");
    TestHarness testHarness = TestHarness(queryWithFilterOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1ULL);

    // Expected output
    std::stringstream expectedOutput;
    for (uint64_t i = 0; i < numBufferToProduce; i++) {
        expectedOutput << "1, 1, " << (i + 1) * 100 << "\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

}// namespace NES
