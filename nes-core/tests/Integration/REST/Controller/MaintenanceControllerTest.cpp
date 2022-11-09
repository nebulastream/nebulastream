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
#include <Phases/MigrationType.hpp>
#include <REST/ServerTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {
class MaintenanceControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MaintenanceControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MaintenanceControllerTest test class."); }

    void startCoordinator() {
        NES_INFO("QueryControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
        NES_INFO("QueryControllerTest: Coordinator started successfully");
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

// test behavior of POST request when required node Id isnt provided
TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestMissingNodeId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

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

// test behavior of POST request when request body doesn't contain 'migrationType'
TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestMissingMigrationType) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["id"] = nodeId;
    //arbitrary field
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

// test behavior of POST request when supplied 'migrationType' isn't supported/doesn't exist
TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestNoSuchMigrationType) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    uint64_t nodeId = coordinator->getNesWorker()->getTopologyNodeId();

    nlohmann::json request;
    request["id"] = nodeId;
    //non-existent migration type
    request["migrationType"] = "Noodles";
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/maintenance/scheduleMaintenance"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 404l);
    auto res = nlohmann::json::parse(response.text);
    std::string message =
        "MigrationType: 0"
        " not a valid type. Type must be either 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without "
        "Buffering)";
    EXPECT_EQ(res["message"], message);
}

// test behavior of POST request when supplied 'nodeId' doesn't exist
TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestNoSuchNodeId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request;
    //non-existent id
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

// test behavior of POST request when all required fields are provided and are valid
TEST_F(MaintenanceControllerTest, testPostMaintenanceRequestAllFieldsProvided) {
    startCoordinator();
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
    EXPECT_EQ(res["Migration Type"],
              Experimental::MigrationType::toString(Experimental::MigrationType::MIGRATION_WITH_BUFFERING));
}
}// namespace NES