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

#include <NesBaseTest.hpp>
#include <REST/ServerTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Services/QueryParsingService.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <API/Query.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Services/MonitoringService.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <cpprest/json.h>

namespace NES {
class MonitoringControllerTest : public Testing::NESBaseTest {
  public:
    Runtime::BufferManagerPtr bufferManager;
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MonitoringControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MonitoringControllerTest test class.");
    }

    void SetUp() override {
        Testing::NESBaseTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
        NES_INFO("Setup MonitoringControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MonitoringControllerTest test class."); }
};

TEST_F(MonitoringControllerTest, testStartMonitoringControllerUnsuccessfully) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");

    bool success = TestUtils::checkRESTServerCreationOrTimeout(coordinatorConfig->restPort.getValue(),5);
    if(!success){
        FAIL() << "Rest server failed to start";
    }

    WorkerRPCClientPtr workerClient = std::make_shared<WorkerRPCClient>();
    auto monitoringService = MonitoringService(workerClient, coordinator->getTopology(), coordinator->getQueryService(), coordinator->getQueryCatalogService());
    Monitoring::MonitoringPlanPtr monitoringPlan = Monitoring::MonitoringPlan::defaultPlan();

    //just for debugging
    web::json::value registered = monitoringService.registerMonitoringPlanToAllNodes(monitoringPlan);
    std::cout << "\n Content of registered Monitoring Plan: ";
    std::cout << registered.to_string();
    web::json::value resultJsonStart = monitoringService.startMonitoringStreams();
    std::cout << "\n Content of startMonitoringStreams value: ";
    std::cout << resultJsonStart.to_string();
    web::json::value resultJsonStop = monitoringService.stopMonitoringStreams();
    std::cout << "\n Content of stopMonitoringStreams value: ";
    std::cout << resultJsonStop.to_string();

    cpr::Response r =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});
    std::cout << "\n Content of Response r: ";
    std::cout << r.text;
    EXPECT_EQ(r.status_code, 404);

}

}//namespace NES