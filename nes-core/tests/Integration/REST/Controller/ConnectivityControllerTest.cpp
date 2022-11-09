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
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES {
class ConnectivityControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }
};

TEST_F(ConnectivityControllerTest, testGetRequest) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("ConnectivityControllerTest: Coordinator started successfully");

    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "REST Server failed to start";
    }
    cpr::Response r = cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/connectivity/check"});
    EXPECT_EQ(r.status_code, 200l);
}

}//namespace NES