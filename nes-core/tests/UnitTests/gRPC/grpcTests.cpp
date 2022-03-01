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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <NesBaseTest.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class grpcTests : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("grpcTests.log", NES::LogLevel::LOG_TRACE);
        NES_INFO("Setup grpc test class.");
    }
};

/**
* Test of Notification from Worker to Coordinator of a failed Query.
*/
TEST_F(grpcTests, testGrpcNotifyQueryFailure) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    auto defaultSource = DefaultSourceType::create();
    PhysicalSourcePtr srcConf = PhysicalSource::create("default_logical", "x1", defaultSource);
    wrkConf->physicalSources.add(srcConf);

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NES_INFO("GrpcNotifyQueryFailureTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("GrpcNotifyQueryFailureTest: Coordinator started successfully");

    NES_INFO("GrpcNotifyQueryFailureTest: Start worker");
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("GrpcNotifyQueryFailureTest: Worker started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = "test1.out";
    NES_INFO("GrpcNotifyQueryFailureTest: Submit query");
    string query = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    auto globalQueryPlan = crd->getGlobalQueryPlan();//necessary?
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    QueryId subQueryId = 1;
    uint64_t workerId = wrk->getWorkerId();
    uint64_t operatorId = 1;
    std::string errormsg = "Query failed.";
    bool successOfNotifyingQueryFailure = wrk->notifyQueryFailure(queryId, subQueryId, workerId, operatorId, errormsg);

    EXPECT_TRUE(successOfNotifyingQueryFailure);

    // stop coordinator and worker
    NES_INFO("GrpcNotifyQueryFailureTest: Stop worker");
    bool retStopWrk = wrk->stop(true);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("GrpcNotifyQueryFailureTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("GrpcNotifyQueryFailureTest: Test finished");
}


/**
* Test if errors are transferred from Worker to Coordinator.
*/
TEST_F(grpcTests, testGrpcSendErrorNotification) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    auto defaultSource = DefaultSourceType::create();
    PhysicalSourcePtr srcConf = PhysicalSource::create("default_logical", "x1", defaultSource);
    wrkConf->physicalSources.add(srcConf);

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NES_INFO("GrpcNotifyErrorTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("GGrpcNotifyErrorTest: Coordinator started successfully");

    NES_INFO("GrpcNotifyErrorTest: Start worker");
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("GrpcNotifyErrorTest: Worker started successfully");

    uint64_t workerId = wrk->getWorkerId();
    std::string errormsg = "Something went wrong.";
    bool successOfTransferringErrors = wrk->sendErrors(workerId, errormsg);
    EXPECT_TRUE(successOfTransferringErrors);

    // stop coordinator and worker
    NES_INFO("GrpcNotifyErrorTest: Stop worker");
    bool retStopWrk = wrk->stop(true);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("GGrpcNotifyErrorTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("GrpcNotifyErrorTest: Test finished");
}

}// namespace NES