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

#include <gtest/gtest.h>
#include "../util/NesBaseTest.hpp"

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>

using namespace std;

namespace NES {

class MQTTSinkDeploymentTest : public Testing::NESBaseTest {
  public:
    CoordinatorConfigurationPtr coConf;
    WorkerConfigurationPtr wrkConf;

    static void SetUpTestCase() {
        NES::setupLogging("MQTTSinkDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MQTTSinkDeploymentTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        coConf = CoordinatorConfiguration::create();
        wrkConf = WorkerConfiguration::create();
        coConf->setRpcPort(*rpcCoordinatorPort);
        coConf->setRestPort(*restPort);
        wrkConf->setCoordinatorPort(*rpcCoordinatorPort);
    }

    void TearDown() override {
        Testing::NESBaseTest::TearDown();
        NES_INFO("Tear down MQTTSinkDeploymentTest class.");
    }
};

/**
 * Test deploying an MQTT sink and sending data via the deployed sink to an MQTT broker
 * DISABLED for now, because it requires a manually set up MQTT broker -> fails otherwise
 */

TEST_F(MQTTSinkDeploymentTest, DISABLED_testDeployOneWorker) {
    NES_INFO("MQTTSinkDeploymentTest: Start coordinator");
    // Here the default schema (default_logical) is already initialized (NesCoordinator calls 'SourceCatalog'
    // it is later used in TypeInferencePhase.cpp via 'sourceCatalog->getSchemaForLogicalStream(logicalSourceName);' to set
    // the new sources schema to the default_logical schema
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MQTTSinkDeploymentTest: Coordinator started successfully");

    NES_INFO("MQTTSinkDeploymentTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MQTTSinkDeploymentTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorker.out";
    remove(outputFilePath.c_str());

    NES_INFO("MQTTSinkDeploymentTest: Submit query");

    // arguments are given so that ThingsBoard accepts the messages sent by the MQTT client
    string query = R"(Query::from("default_logical").sink(MQTTSinkDescriptor::create("ws://127.0.0.1:9001",
            "/nesui", "rfRqLGZRChg8eS30PEeR", 5, MQTTSinkDescriptor::milliseconds, 500, MQTTSinkDescriptor::atLeastOnce, false));)";

    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    // Comment for better understanding: From here on at some point the DataSource.cpp 'runningRoutine()' function is called
    // this function, because "default_logical" is used, uses 'DefaultSource.cpp', which create a TupleBuffer with 10 id:value
    // pairs, each being 1,1
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("MQTTSinkDeploymentTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("MQTTSinkDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("MQTTSinkDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("MQTTSinkDeploymentTest: Test finished");
}

}// namespace NES
