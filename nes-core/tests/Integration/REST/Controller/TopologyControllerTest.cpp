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
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <REST/ServerTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {
class TopologyControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }

    /**
     * Starts a coordinator with the following configurations
     * rpcPort = rpcCoordinatorPort specified in NESBaseTest
     * restPort = restPort specified in NESBaseTest
     */
    void startCoordinator() {
        NES_INFO("SourceCatalogControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
};

TEST_F(TopologyControllerTest, testGetTopology) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology"});
    EXPECT_EQ(r.status_code, 200l);
    NES_DEBUG(r.text);
    nlohmann::json response = nlohmann::json::parse(r.text);
    NES_DEBUG(response.dump());
    EXPECT_EQ(r.status_code, 200l);
    for (auto edge : response["edges"]) {
        EXPECT_TRUE(edge.contains("source") && edge.contains("target"));
    }
    for (auto node : response["nodes"]) {
        EXPECT_TRUE(node.contains("id") && node.contains("ip_address") && node.contains("nodeType") && node.contains("location")
                    && node.contains("available_resources"));
    }
}

TEST_F(TopologyControllerTest, testAddParentMissingParentId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request{};
    request["childId"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], " Request body missing 'parentId'");
}

TEST_F(TopologyControllerTest, testAddParentMissingChildId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request{};
    request["parentId"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], " Request body missing 'childId'");
}

TEST_F(TopologyControllerTest, testAddParentNoSuchChild) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 7;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: Node with childId=7 not found.");
}

TEST_F(TopologyControllerTest, testAddParentNoSuchParent) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    nlohmann::json request{};
    request["parentId"] = 3;
    request["childId"] = 2;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: Node with parentId=3 not found.");
}

TEST_F(TopologyControllerTest, testAddParentSameChildAndParent) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request{};
    request["parentId"] = 7;
    request["childId"] = 7;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{30000});
    EXPECT_EQ(response.status_code, 400l);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: childId and parentId must be different.");
}

TEST_F(TopologyControllerTest, testAddParentAlreadyExists) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 2;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{30000});
    EXPECT_EQ(response.status_code, 500l);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
}

TEST_F(TopologyControllerTest, testRemoveParent) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 2;
    auto asyncResp = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/removeParent"},
                                      cpr::Header{{"Content-Type", "application/json"}},
                                      cpr::Body{request.dump()},
                                      cpr::ConnectTimeout{3000},
                                      cpr::Timeout{30000});
    asyncResp.wait();
    cpr::Response response = asyncResp.get();
    EXPECT_EQ(response.status_code, 200l);
    NES_DEBUG(response.text);
    nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["success"], true);
}

}// namespace NES
