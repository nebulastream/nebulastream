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
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class QueryFailureTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("QueryFailureTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(QueryFailureTest, testQueryFailureForFaultySource) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryFailureTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryFailureTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("test", testSchema);
    NES_DEBUG("QueryFailureTest: Coordinator started successfully");

    NES_DEBUG("QueryFailureTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    CSVSourceTypePtr csvSourceType = CSVSourceType::create("test", "physical_test");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "malformed_csv_test.csv");
    csvSourceType->setGatheringInterval(1);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
    csvSourceType->setNumberOfBuffersToProduce(6);
    csvSourceType->setSkipHeader(false);
    workerConfig1->coordinatorPort = port;
    workerConfig1->physicalSourceTypes.add(csvSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryFailureTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("QueryFailureTest: Submit query");
    string query = R"(Query::from("test").filter(Attribute("value")>2).sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query={}", query);
    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    EXPECT_TRUE(TestUtils::checkFailedOrTimeout(queryId, queryCatalog));
}

/**
 * This test checks if we can run a valid query after a query failed
 */
TEST_F(QueryFailureTest, testExecutingOneFaultAndOneCorrectQuery) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryFailureTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryFailureTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("test", testSchema);
    NES_DEBUG("QueryFailureTest: Coordinator started successfully");

    NES_DEBUG("QueryFailureTest: Start worker 1");
    auto workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType = CSVSourceType::create("test", "physical_test");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "malformed_csv_test.csv");
    csvSourceType->setGatheringInterval(1);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(2);
    csvSourceType->setNumberOfBuffersToProduce(6);
    csvSourceType->setSkipHeader(false);
    auto defaultSourceType = DefaultSourceType::create("default_logical", "default_source");
    workerConfig1->physicalSourceTypes.add(csvSourceType);
    workerConfig1->physicalSourceTypes.add(defaultSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryFailureTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    NES_INFO("QueryFailureTest: Submit query");
    string query1 =
        R"(Query::from("test").sink(FileSinkDescriptor::create(")" + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";
    NES_DEBUG("query={}", query1);
    QueryId queryId1 = requestHandlerService->validateAndQueueAddQueryRequest(query1, Optimizer::PlacementStrategy::BottomUp);
    EXPECT_TRUE(TestUtils::checkFailedOrTimeout(queryId1, queryCatalog));

    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryFailureTest: Submit query");
    string query2 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath2
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 = requestHandlerService->validateAndQueueAddQueryRequest(query2, Optimizer::PlacementStrategy::BottomUp);

    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath2));

    NES_INFO("QueryFailureTest: Remove query");
    requestHandlerService->validateAndQueueStopQueryRequest(queryId2);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_INFO("QueryFailureTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryFailureTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryFailureTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

// This test will be enabled when fixing #2857
TEST_F(QueryFailureTest, DISABLED_failRunningQuery) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.bufferSizeInBytes = 2;
    NES_INFO("QueryFailureTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryFailureTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("test", testSchema);
    NES_DEBUG("QueryFailureTest: Coordinator started successfully");

    NES_DEBUG("QueryFailureTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto defaultSourceType = DefaultSourceType::create("default_logical", "default_source");
    defaultSourceType->setNumberOfBuffersToProduce(1000);
    workerConfig1->physicalSourceTypes.add(defaultSourceType);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryFailureTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";

    auto query = Query::from("default_logical").filter(Attribute("value") < 42).sink(FileSinkDescriptor::create(outputFilePath));

    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest("", query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    EXPECT_TRUE(TestUtils::checkFailedOrTimeout(queryId, queryCatalog));
}

}// namespace NES