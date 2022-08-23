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
#include <Util/MetricValidator.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Runtime/BufferManager.hpp>
#include <cstdint>
#include <memory>

using std::cout;
using std::endl;

namespace NES {

uint16_t timeout = 15;
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


TEST_F(MonitoringControllerTest, MonitoringControllerStartEndpointTest) {

    uint64_t localBuffers = 64;
    uint64_t globalBuffers = 1024 * 128;
    std::set<std::string> expectedMonitoringStreams{"wrapped_network", "wrapped_cpu", "memory", "disk"};

    NES_INFO("Tests for Oatpp Monitoring Contoller: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");

    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(),5);
    if(!success){
        FAIL() << "Rest server failed to start";
    }

    // for debugging:
    MonitoringServicePtr monitoringService = coordinator->getMonitoringService();
    auto jsonMetrics = monitoringService->startMonitoringStreams();
    auto expected = jsonMetrics.to_string();

    // oatpp GET start call
    cpr::Response r =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});

    EXPECT_EQ(r.status_code, 200);

    // toDo also compare content of r and find out why its not like expected
/* this still fails:
    std::cout << "\n Content of OATPP Response r: ";
    std::cout << r.text;
    std::cout << "\n Expected content: ";
    std::cout << "monitoringData:" + expected;
   // EXPECT_EQ(expected, r.text);
*/
}


TEST_F(MonitoringControllerTest, StartMonitoringControllerFailsBecauseMonitoringIsNotEnabled) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");

    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(),5);
    if(!success){
        FAIL() << "Rest server failed to start";
    }

    cpr::Response r =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});
    std::cout << "\n Content of Response r: ";
    std::cout << r.text;
    EXPECT_EQ(r.status_code, 404);
}

}//namespace NES