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
#include <gtest/gtest.h>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Enums/QueryCompilerType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

// Todo: #4069 addresses re-writing this test using the TestHarness
class UnionDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("UnionDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup UnionDeploymentTest test class.");
    }
};

/**
 * Test deploying unionWith query with source on two different worker node using bottom up strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("car", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("truck", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create("car", "physical_car");
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create("truck", "physical_truck");
    defaultSourceType2->setNumberOfBuffersToProduce(3);
    workerConfig2->physicalSourceTypes.add(defaultSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingBottomUp.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "car$id:INTEGER(32 bits),car$value:INTEGER(64 bits)\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
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

    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): content={}", content);
    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingBottomUp): expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("UnionDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("car", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("truck", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType = DefaultSourceType::create("car", "physical_car");
    defaultSourceType->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType);
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create("truck", "physical_truck");
    defaultSourceType2->setNumberOfBuffersToProduce(3);
    workerConfig2->physicalSourceTypes.add(defaultSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::TopDown,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "car$id:INTEGER(32 bits),car$value:INTEGER(64 bits)\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
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

    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content={}", content);
    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("UnionDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(UnionDeploymentTest, testDeployTwoWorkerMergeUsingTopDownWithDifferentSpeed) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("car", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("truck", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create("car", "physical_car");
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create("truck", "physical_truck");
    defaultSourceType2->setNumberOfBuffersToProduce(3);
    workerConfig2->physicalSourceTypes.add(defaultSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());
    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::TopDown,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 3));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 6));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    string expectedContent = "car$id:INTEGER(32 bits),car$value:INTEGER(64 bits)\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
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

    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): content={}", content);
    NES_INFO("UnionDeploymentTest(testDeployTwoWorkerMergeUsingTopDown): expContent={}", expectedContent);
    EXPECT_EQ(content, expectedContent);

    NES_INFO("UnionDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(UnionDeploymentTest, testMergeTwoDifferentSources) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    crd->getSourceCatalogService()->registerLogicalSource("car", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("truck", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create("car", "physical_car");
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create("truck", "physical_truck");
    defaultSourceType2->setNumberOfBuffersToProduce(3);
    workerConfig2->physicalSourceTypes.add(defaultSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath = getTestResourceFolder() / "testDeployTwoWorkerMergeUsingTopDown.out";
    remove(outputFilePath.c_str());

    NES_INFO("UnionDeploymentTest: Submit query");
    string query =
        R"(Query::from("car").unionWith(Query::from("truck")).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";
    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::TopDown,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    cout << "queryid=" << queryId << endl;
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    NES_INFO("UnionDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("UnionDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("UnionDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest: Test finished");
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are above a unionWith operator and will be pushed down towards both of the available sources.
 *       2 filter operators are already below unionWith operator and need to be pushed down normally towards its respective source.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()
                          ->addField(createField("value", BasicType::UINT32))
                          ->addField(createField("id", BasicType::UINT32))
                          ->addField(createField("timestamp", BasicType::INT32));
    crd->getSourceCatalogService()->registerLogicalSource("ruby", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("diamond", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("ruby", "physical_ruby");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(28);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("diamond", "physical_diamond");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(28);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath =
        getTestResourceFolder() / "testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".filter(Attribute(\"id\") < 12)"
                   ".unionWith(Query::from(\"diamond\")"
                   ".filter(Attribute(\"value\") < 15))"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"value\") < 17)"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"value\") > 1)"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));
    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|2|1|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|1|2|\n"
                                        "|3|11|2|\n"
                                        "|3|1|2|\n"
                                        "|3|1|2|\n"
                                        "|4|1|2|\n"
                                        "|5|1|2|\n"
                                        "|6|1|2|\n"
                                        "|7|1|2|\n"
                                        "|8|1|2|\n"
                                        "|9|1|2|\n"
                                        "|10|1|2|\n"
                                        "|11|1|2|\n"
                                        "|12|1|2|\n"
                                        "|13|1|2|\n"
                                        "|14|1|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|2|1|2|\n"
                                         "|2|11|2|\n"
                                         "|3|1|2|\n"
                                         "|3|11|2|\n"
                                         "|3|1|2|\n"
                                         "|3|1|2|\n"
                                         "|4|1|2|\n"
                                         "|5|1|2|\n"
                                         "|6|1|2|\n"
                                         "|7|1|2|\n"
                                         "|8|1|2|\n"
                                         "|9|1|2|\n"
                                         "|10|1|2|\n"
                                         "|11|1|2|\n"
                                         "|12|1|2|\n"
                                         "|13|1|2|\n"
                                         "|14|1|2|\n"
                                         "|15|1|2|\n"
                                         "|16|1|2|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO(
        "UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): content={}",
        content);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): "
             "expectedContentSubQry={}",
             expectedContentSubQry);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): "
             "expectedContentMainQry={}",
             expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Test finished");
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 1 filter operator is above a unionWith operator and will be pushed down towards both of the available sources.
 *       1 filter operator is already below unionWith operator and needs to be pushed down normally towards its own source.
 */
TEST_F(UnionDeploymentTest, testOneFilterPushDownWithMergeOfTwoDifferentSources) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()
                          ->addField(createField("value", BasicType::UINT32))
                          ->addField(createField("id", BasicType::UINT32))
                          ->addField(createField("timestamp", BasicType::INT32));
    crd->getSourceCatalogService()->registerLogicalSource("ruby", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("diamond", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("ruby", "physical_ruby");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(28);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("diamond", "physical_diamond");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(28);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testOneFilterPushDownWithMergeOfTwoDifferentSources.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".unionWith(Query::from(\"diamond\")"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"id\") > 3))"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"id\") > 4)"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|1|12|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|11|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|1|12|2|\n"
                                         "|2|11|2|\n"
                                         "|2|16|2|\n"
                                         "|3|11|2|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("UnionDeploymentTest(testOneFilterPushDownWithMergeOfTwoDifferentSources): content={}", content);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): "
             "expectedContentSubQry={}",
             expectedContentSubQry);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): "
             "expectedContentMainQry={}",
             expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Test finished");
}

/**
 * Test deploying filter-push-down on unionWith query with source on two different worker node using top down strategy.
 * Case: 2 filter operators are already below unionWith operator and needs to be pushed down normally towards their respective source.
 *       Here the filters don't need to be pushed down over an existing unionWith operator.
 */
TEST_F(UnionDeploymentTest, testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentSources) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NES_INFO("UnionDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = Schema::create()
                          ->addField(createField("value", BasicType::UINT32))
                          ->addField(createField("id", BasicType::UINT32))
                          ->addField(createField("timestamp", BasicType::INT32));
    crd->getSourceCatalogService()->registerLogicalSource("ruby", testSchema);
    crd->getSourceCatalogService()->registerLogicalSource("diamond", testSchema);
    NES_DEBUG("UnionDeploymentTest: Coordinator started successfully");

    NES_DEBUG("UnionDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    workerConfig1->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("ruby", "physical_ruby");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(28);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("UnionDeploymentTest: Worker1 started successfully");

    NES_INFO("UnionDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("diamond", "physical_diamond");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(28);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    workerConfig2->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("UnionDeploymentTest: Worker 2 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentSources.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Submit query");
    string query = "Query::from(\"ruby\")"
                   ".map(Attribute(\"timestamp\") = 2)"
                   ".filter(Attribute(\"value\") < 9)"
                   ".unionWith(Query::from(\"diamond\")"
                   ".map(Attribute(\"timestamp\") = 1)"
                   ".filter(Attribute(\"id\") < 12)"
                   ".filter(Attribute(\"value\") < 6))"
                   ".sink(FileSinkDescriptor::create(\""
        + outputFilePath + "\"));";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query,
                                                                    Optimizer::PlacementStrategy::BottomUp,
                                                                    FaultToleranceType::NONE,
                                                                    LineageType::IN_MEMORY);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    std::ifstream ifs(outputFilePath);
    std::string content((std::istreambuf_iterator<char>(ifs)), (std::istreambuf_iterator<char>()));

    std::string expectedContentSubQry = "+----------------------------------------------------+\n"
                                        "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                        "+----------------------------------------------------+\n"
                                        "|1|1|2|\n"
                                        "|1|12|2|\n"
                                        "|1|4|2|\n"
                                        "|2|1|2|\n"
                                        "|2|11|2|\n"
                                        "|2|16|2|\n"
                                        "|3|1|2|\n"
                                        "|3|11|2|\n"
                                        "|3|1|2|\n"
                                        "|3|1|2|\n"
                                        "|4|1|2|\n"
                                        "|5|1|2|\n"
                                        "|6|1|2|\n"
                                        "|7|1|2|\n"
                                        "|8|1|2|\n"
                                        "+----------------------------------------------------+\n";
    std::string expectedContentMainQry = "+----------------------------------------------------+\n"
                                         "|value:UINT32|id:UINT32|timestamp:INT32|\n"
                                         "+----------------------------------------------------+\n"
                                         "|1|1|1|\n"
                                         "|1|4|1|\n"
                                         "|2|1|1|\n"
                                         "|2|11|1|\n"
                                         "|3|1|1|\n"
                                         "|3|11|1|\n"
                                         "|3|1|1|\n"
                                         "|3|1|1|\n"
                                         "|4|1|1|\n"
                                         "|5|1|1|\n"
                                         "+----------------------------------------------------+\n";

    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersAlreadyBelowAndMergeOfTwoDifferentSources): content={}", content);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): "
             "expectedContentSubQry={}",
             expectedContentSubQry);
    NES_INFO("UnionDeploymentTest(testPushingTwoFiltersBelowAndTwoFiltersAlreadyAtBottomWithMergeOfTwoDifferentSources): "
             "expectedContentMainQry={}",
             expectedContentMainQry);
    EXPECT_TRUE(content.find(expectedContentSubQry));
    EXPECT_TRUE(content.find(expectedContentMainQry));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("UnionDeploymentTest For Filter-Push-Down: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("UnionDeploymentTest For Filter-Push-Down: Test finished");
}
}// namespace NES
