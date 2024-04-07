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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Identifiers.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>

using namespace std;

namespace NES {
/**
 * Due to a LACK OF BROKER in the most common MQTT-CPP libraries, the broker must be set up manually.
 * An easy way is to use the Mosquitto broker. On Ubuntu: 'sudo apt install mosquitto'.
 * In order to start the broker with information for every received payload: 'mosquitto -v' (uses default port 1883).
 */

//FIXME test right now relies on setting up a broker manually.
// - find a way to fully automate test (e.time::WorkerContext workerContext(g. using redBoltz c++ MQTT library, which offers a broker
class MQTTSinkDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    CoordinatorConfigurationPtr coConf;
    WorkerConfigurationPtr wrkConf;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("MQTTSinkDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MQTTSinkDeploymentTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        coConf = CoordinatorConfiguration::createDefault();
        wrkConf = WorkerConfiguration::create();

        // Manually add SourceType since 'SourceCatalogue' cannot be found
        auto defaultSourceType1 = DefaultSourceType::create("default_logical", "physical_MQTTSinkDeployment");
        wrkConf->physicalSourceTypes.add(defaultSourceType1);

        coConf->rpcPort = (*rpcCoordinatorPort);
        coConf->restPort = *restPort;
        wrkConf->coordinatorPort = *rpcCoordinatorPort;
    }

    void TearDown() override {
        NES_INFO("Tear down MQTTSinkDeploymentTest class.");
        Testing::BaseIntegrationTest::TearDown();
    }
};

/**
 * Test deploying an MQTT sink and sending data via the deployed sink to an MQTT broker
 * DISABLED for now, because it requires a manually set up MQTT broker -> fails otherwise
 */

TEST_F(MQTTSinkDeploymentTest, DISABLED_testDeployOneWorker) {
    NES_INFO("MQTTSinkDeploymentTest: Start coordinator");
    // Here the default schema (default_logical) is already initialized (NesCoordinator calls 'SourceCatalog'
    // it is later used in TypeInferencePhase.cpp via 'sourceCatalog->getSchemaForLogicalSource(logicalSourceName);' to set
    // the new sources schema to the default_logical schema
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("MQTTSinkDeploymentTest: Coordinator started successfully");

    NES_INFO("MQTTSinkDeploymentTest: Start worker 1");
    wrkConf->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("MQTTSinkDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testDeployOneWorker.out";
    remove(outputFilePath.c_str());

    NES_INFO("MQTTSinkDeploymentTest: Submit query");

    // arguments are given so that ThingsBoard accepts the messages sent by the MQTT client
    // uses tcp on default port for local mosquitto broker.
    string query = R"(Query::from("default_logical").sink(MQTTSinkDescriptor::create("tcp://127.0.0.1:1883",
            "/nesui", "rfRqLGZRChg8eS30PEeR", 5, MQTTSinkDescriptor::TimeUnits::milliseconds, 500, MQTTSinkDescriptor::ServiceQualities::atLeastOnce, false));)";

    QueryId queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);

    // Comment for better understanding: From here on at some point the DataSource.cpp 'runningRoutine()' function is called
    // this function, because "default_logical" is used, uses 'DefaultSource.cpp', which create a TupleBuffer with 10 id:value
    // pairs, each being 1,1
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("MQTTSinkDeploymentTest: Remove query");
    requestHandlerService->validateAndQueueStopQueryRequest(queryId);
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
