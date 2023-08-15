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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/QueryService.hpp>
#include <Util/Core.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class GrpcTests : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GrpcTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup grpc test class.");
    }
};

/**
* Test of Notification from Worker to Coordinator of a failed Query.
*/
TEST_F(GrpcTests, DISABLED_testGrpcNotifyQueryFailure) {
    // Setup Coordinator
    std::string window =
        R"(Schema::create()->addField(createField("win", BasicType::UINT64))->addField(createField("id1", BasicType::UINT64))
                                            ->addField(createField("timestamp", BasicType::UINT64));)";
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    NES_INFO("GrpcNotifyQueryFailureTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("Win1", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("GrpcNotifyQueryFailureTest: Coordinator started successfully");

    NES_INFO("GrpcNotifyQueryFailureTest: Start worker");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    auto srcConf1 = CSVSourceType::create();
    srcConf1->setFilePath("../tests/test_data/window.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(0);
    srcConf1->setNumberOfBuffersToProduce(0);
    auto windowSource = PhysicalSource::create("Win1", "test_stream1", srcConf1);
    wrkConf->physicalSources.add(windowSource);
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("GrpcNotifyQueryFailureTest: Worker started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    NES_INFO("GrpcNotifyQueryFailureTest: Submit query");
    string query =
        R"(Query::from("Win1").sink(FileSinkDescriptor::create(")" + outputFilePath1 + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    QueryId subQueryId = 1;
    std::string errormsg = "Query failed.";
    bool successOfNotifyingQueryFailure = wrk->notifyQueryFailure(queryId, subQueryId, errormsg);

    EXPECT_TRUE(successOfNotifyingQueryFailure);

    EXPECT_TRUE(TestUtils::checkFailedOrTimeout(queryId, queryCatalogService));

    // stop coordinator and worker
    NES_INFO("GrpcNotifyQueryFailureTest: Stop worker");
    bool retStopWrk = wrk->stop(true);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("GrpcNotifyQueryFailureTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("GrpcNotifyQueryFailureTest: Test finished");

    int response = remove(outputFilePath1.c_str());
    EXPECT_TRUE(response == 0);
}

/**
* Test if errors are transferred from Worker to Coordinator.
*/
TEST_F(GrpcTests, DISABLED_testGrpcSendErrorNotification) {

    // Setup Coordinator
    std::string window =
        R"(Schema::create()->addField(createField("win", BasicType::UINT64))->addField(createField("id1", BasicType::UINT64))
                                            ->addField(createField("timestamp", BasicType::UINT64));)";
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.numberOfBuffersInGlobalBufferManager = 2 * 1024;
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("Win1", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("AndOperatorTest: Coordinator started successfully");

    // Setup Worker 1
    NES_INFO("AndOperatorTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    auto srcConf1 = CSVSourceType::create();
    srcConf1->setFilePath("../tests/test_data/window.csv");
    srcConf1->setNumberOfTuplesToProducePerBuffer(0);
    srcConf1->setNumberOfBuffersToProduce(0);
    auto windowSource = PhysicalSource::create("Win1", "test_stream1", srcConf1);
    workerConfig1->physicalSources.add(windowSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("GrpcNotifyErrorTest: Worker started successfully");

    uint64_t workerId = wrk1->getWorkerId();
    std::string errormsg = "Too much memory allocation";
    bool successOfTransferringErrors = wrk1->notifyErrors(workerId, errormsg);
    EXPECT_TRUE(successOfTransferringErrors);

    // stop coordinator and worker
    NES_INFO("GrpcNotifyErrorTest: Stop worker");
    bool retStopWrk = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("GGrpcNotifyErrorTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("GrpcNotifyErrorTest: Test finished");
}

}// namespace NES