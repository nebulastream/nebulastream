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
#include <BaseIntegrationTest.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

namespace NES {
class TopologyControllerTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }

    /**
     * Starts a coordinator with the following configurations
     * rpcPort = rpcCoordinatorPort specified in BaseIntegrationTest
     * restPort = restPort specified in BaseIntegrationTest
     */
    void startCoordinator() {
        NES_INFO("SourceCatalogControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
    uint64_t sleeptime = 1;
};

TEST_F(TopologyControllerTest, testGetTopology) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology"});
    nlohmann::json response;
    EXPECT_EQ(r.status_code, 200l);
    NES_DEBUG("{}", r.text);
    ASSERT_NO_THROW(response = nlohmann::json::parse(r.text));
    NES_DEBUG("{}", response.dump());
    EXPECT_EQ(r.status_code, 200l);
    for (auto edge : response["edges"]) {
        EXPECT_TRUE(edge.contains("source") && edge.contains("target"));
    }
    for (auto node : response["nodes"]) {
        EXPECT_TRUE(node.contains("id") && node.contains("ip_address") && node.contains("nodeType") && node.contains("location")
                    && node.contains("available_resources"));
    }
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChildMissingParentId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    nlohmann::json request{};
    request["childId"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    EXPECT_EQ(response.status_code, 400l);
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["message"], " Request body missing 'parentId'");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChildMissingChildId) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    nlohmann::json request{};
    request["parentId"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    EXPECT_EQ(response.status_code, 400l);
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["message"], " Request body missing 'childId'");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChildNoSuchChild) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 7;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    EXPECT_EQ(response.status_code, 400l);
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: Node with childId=7 not found.");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChildNoSuchParent) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1));

    nlohmann::json request{};
    request["parentId"] = 3;
    request["childId"] = 2;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    EXPECT_EQ(response.status_code, 400l);
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: Node with parentId=3 not found.");
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChildSameChildAndParent) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    nlohmann::json request{};
    request["parentId"] = 7;
    request["childId"] = 7;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{30000});
    nlohmann::json res;
    EXPECT_EQ(response.status_code, 400l);
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: childId and parentId must be different.");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChildAlreadyExists) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1));

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 2;
    request["bandwidth"] = 20;
    request["latency"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{30000});
    nlohmann::json res;
    EXPECT_EQ(response.status_code, 500l);
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testaddAsChild) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1));

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 2;
    auto asyncResp = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/removeAsChild"},
                                      cpr::Header{{"Content-Type", "application/json"}},
                                      cpr::Body{request.dump()},
                                      cpr::ConnectTimeout{3000},
                                      cpr::Timeout{30000});
    asyncResp.wait();
    cpr::Response response = asyncResp.get();
    EXPECT_EQ(response.status_code, 200l);
    NES_DEBUG("{}", response.text);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["success"], true);

    nlohmann::json addLinkRequest1{};
    addLinkRequest1["parentId"] = 1;
    addLinkRequest1["childId"] = 2;
    addLinkRequest1["bandwidth"] = 20;
    addLinkRequest1["latency"] = 1;
    auto response1 = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addAsChild"},
                               cpr::Header{{"Content-Type", "application/json"}},
                               cpr::Body{addLinkRequest1.dump()},
                               cpr::ConnectTimeout{3000},
                               cpr::Timeout{30000});
    EXPECT_EQ(response1.status_code, 200l);

    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testRemoveChild) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    ASSERT_TRUE(TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1));

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 2;
    auto asyncResp = cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/removeAsChild"},
                                      cpr::Header{{"Content-Type", "application/json"}},
                                      cpr::Body{request.dump()},
                                      cpr::ConnectTimeout{3000},
                                      cpr::Timeout{30000});
    asyncResp.wait();
    cpr::Response response = asyncResp.get();
    EXPECT_EQ(response.status_code, 200l);
    NES_DEBUG("{}", response.text);
    nlohmann::json res;
    ASSERT_NO_THROW(res = nlohmann::json::parse(response.text));
    NES_DEBUG("{}", res.dump());
    EXPECT_EQ(res["success"], true);
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port {}", coordinatorConfig->restPort.getValue());
    EXPECT_TRUE(stopCrd);
}

}// namespace NES
