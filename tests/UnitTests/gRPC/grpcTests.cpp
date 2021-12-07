/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class grpcTests : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("grpcTests.log", NES::LOG_TRACE);
        NES_INFO("Setup grpc test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { NES_INFO("Tear down grpcTest class."); }
};

/**
 * Test of Notification from Worker to Coordinator of a failed Query.
 */
TEST_F(grpcTests, testGrpcNotifyQueryFailure) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("QueryDeploymentTest: Worker started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = "test1.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    string query = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    auto globalQueryPlan = crd->getGlobalQueryPlan(); //necessary?
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    QueryId subQueryId = 1;
    uint64_t workerId = wrk->getWorkerId();
    uint64_t operatorId = 1;
    std::string errormsg = "Query failed.";
    bool successOfNotifyingQueryFailure = wrk->notifyQueryFailure(queryId, subQueryId, workerId, operatorId, errormsg);

    EXPECT_TRUE(successOfNotifyingQueryFailure);

    // stop coordinator and worker
    NES_INFO("QueryDeploymentTest: Stop worker");
    bool retStopWrk = wrk->stop(true);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

}// namespace NES