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
#include <nlohmann/json.hpp>
#include <Phases/MigrationType.hpp>

namespace NES {
class MaintenanceControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MaintenanceControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MaintenanceControllerTest test class."); }

    void startRestServer() {
        NES_INFO("QueryControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinatorConfig->restServerType = ServerType::Oatpp;
        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("QueryControllerTest: Coordinator started successfully");
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestMissingNodeId) {
    startRestServer();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["someField"] = "someData";
    request["migrationType"] = "IN_MEMORY";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/maintenance/scheduleMaintenance"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    auto res = nlohmann::json::parse(response.text);
    EXPECT_EQ(res["message"], "Field 'id' must be provided");
}

TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestMissingMigrationType) {
    startRestServer();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["id"] = nodeId;
    request["someField"] = "Is mayonnaise an instrument?";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/maintenance/scheduleMaintenance"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 400l);
    auto res = nlohmann::json::parse(response.text);
    EXPECT_EQ(res["message"], "Field 'migrationType' must be provided");
}

TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestNoSuchMigrationType) {
    startRestServer();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["id"] = nodeId;
    request["migrationType"] = "Noodles";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/maintenance/scheduleMaintenance"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 404l);
    auto res = nlohmann::json::parse(response.text);
    std::string message = "MigrationType: 0"
         " not a valid type. Type must be either 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without "
          "Buffering)";
    EXPECT_EQ(res["message"], message);
}

TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestNoSuchNodeId) {
    startRestServer();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["id"] = 69;
    request["migrationType"] = Experimental::MigrationType::toString(Experimental::MigrationType::MIGRATION_WITH_BUFFERING);
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/maintenance/scheduleMaintenance"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 404l);
    auto res = nlohmann::json::parse(response.text);
    EXPECT_EQ(res["message"], "No Topology Node with ID " + std::to_string(69));
}


TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestAllFieldsProvided) {
    startRestServer();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["id"] = nodeId;
    request["migrationType"] = Experimental::MigrationType::toString(Experimental::MigrationType::MIGRATION_WITH_BUFFERING);
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/maintenance/scheduleMaintenance"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    auto res = nlohmann::json::parse(response.text);
    EXPECT_EQ(res["Info"], "Successfully submitted Maintenance Request");
    EXPECT_EQ(res["Node Id"], nodeId);
    EXPECT_EQ(res["Migration Type"], Experimental::MigrationType::toString(Experimental::MigrationType::MIGRATION_WITH_BUFFERING));
}

} // namespace NES