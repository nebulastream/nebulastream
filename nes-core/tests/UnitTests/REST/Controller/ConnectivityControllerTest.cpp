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
#include <gtest/gtest.h>
#include <cpr/cpr.h>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <memory>
#include <REST/ServerTypes.hpp>

namespace NES {
class ConnectivityControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down TopologyControllerTest test class."); }

    NesCoordinatorPtr createAndStartCoordinator() const {
        NES_INFO("TestsForOatppEndpoints: Start coordinator");
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->restServerType = ServerType::Oatpp;
        auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("TestsForOatppEndpoints: Coordinator started successfully");
        return coordinator;
    }
};

TEST_F(ConnectivityControllerTest, testGetRequest) {
    auto coordinator = createAndStartCoordinator();

    cpr::Response r = cpr::Get(cpr::Url{"http://localhost/" + std::to_string(*restPort) + "check"});
    NES_DEBUG(r.status_code);
    NES_DEBUG(r.text);
}

}//namespace NES