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

#include <API/Query.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Monitoring/MetricCollectors/MetricCollectorType.hpp>
#include <Monitoring/MonitoringManager.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <NesBaseTest.hpp>
#include <REST/ServerTypes.hpp>
#include <Runtime/BufferManager.hpp>
#include <Services/MonitoringService.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/MetricValidator.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/json.h>
#include <cpr/cpr.h>
#include <cstdint>
#include <gtest/gtest.h>
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

TEST_F(MonitoringControllerTest, testStartMonitoring) {
    NES_INFO("Tests for Oatpp Monitoring Controller start monitoring: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    // oatpp GET start call
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});
    EXPECT_EQ(r.status_code, 200);
    //TODO:: compare content of response to expected values. To be added once json library found #2950
}

TEST_F(MonitoringControllerTest, testStartMonitoringFailsBecauseMonitoringIsNotEnabled) {
    NES_INFO("Tests for Oatpp Monitoring Controller start monitoring: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/start"});
    EXPECT_EQ(r.status_code, 404);
}

TEST_F(MonitoringControllerTest, testRequestAllMetrics) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testRequestAllMetrics: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/metrics"});
    EXPECT_EQ(r.status_code, 200);
    //TODO: also check if content of r contains valid information (right fields and valid queryIds) --> Need of json parser
}

TEST_F(MonitoringControllerTest, testGetMonitoringControllerDataFromOneNode) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testGetMonitoringControllerDataFromOneNode: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/metrics"},
                               cpr::Parameters{{"nodeId", std::to_string(1)}});
    EXPECT_EQ(r.status_code, 200);
    // TODO: also check if content of r contains valid information (right fields and valid queryIds) --> Need of json parser
}

TEST_F(MonitoringControllerTest, testGetMonitoringControllerStorage) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testGetMonitoringControllerStorage: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/storage"});
    EXPECT_EQ(r.status_code, 200);
    //TODO: compare content of response to expected values. To be added once json library found #2950
}

TEST_F(MonitoringControllerTest, testGetMonitoringControllerStreams) {
    NES_INFO("Tests for Oatpp Monitoring Controller - testGetMonitoringControllerStreams: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    coordinatorConfig->enableMonitoring = true;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("MonitoringControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    MonitoringServicePtr monitoringService = coordinator->getMonitoringService();
    auto expected = monitoringService->startMonitoringStreams();
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/monitoring/streams"});
    EXPECT_EQ(r.status_code, 200);
    //TODO: compare content of response to expected values. To be added once json library found #2950
}

}//namespace NES
