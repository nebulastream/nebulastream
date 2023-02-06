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
#include <Util/TestUtils.hpp>

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
        EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    }

    NesCoordinatorPtr coordinator;
    CoordinatorConfigurationPtr coordinatorConfig;
    uint64_t sleeptime = 1;
};

TEST_F(TopologyControllerTest, testGetTopology) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }


    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology"});
    nlohmann::json response;
    try {
        response = nlohmann::json::parse(r.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(r.status_code, 200l);
    NES_DEBUG(r.text);
    NES_DEBUG(response.dump());
    EXPECT_EQ(r.status_code, 200l);
    for (auto edge : response["edges"]) {
        EXPECT_TRUE(edge.contains("source") && edge.contains("target"));
    }
    for (auto node : response["nodes"]) {
        EXPECT_TRUE(node.contains("id") && node.contains("ip_address") && node.contains("nodeType") && node.contains("location")
                    && node.contains("available_resources"));
    }
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testAddParentMissingParentId) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    nlohmann::json request{};
    request["childId"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 400l);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], " Request body missing 'parentId'");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testAddParentMissingChildId) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    nlohmann::json request{};
    request["parentId"] = 1;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 400l);
    //nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], " Request body missing 'childId'");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testAddParentNoSuchChild) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 7;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 400l);
    //nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: Node with childId=7 not found.");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testAddParentNoSuchParent) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    if (!TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1)) {
        bool stopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(stopWrk1);
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    nlohmann::json request{};
    request["parentId"] = 3;
    request["childId"] = 2;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{3000});
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(stopWrk1);
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 400l);
    //nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: Node with parentId=3 not found.");
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testAddParentSameChildAndParent) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    nlohmann::json request{};
    request["parentId"] = 7;
    request["childId"] = 7;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{30000});
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 400l);
    //nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["message"], "Could not add parent for node in topology: childId and parentId must be different.");
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testAddParentAlreadyExists) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    if (!TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1)) {
        bool stopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(stopWrk1);
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    nlohmann::json request{};
    request["parentId"] = 1;
    request["childId"] = 2;
    auto response = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                              cpr::Header{{"Content-Type", "application/json"}},
                              cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000},
                              cpr::Timeout{30000});
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(stopWrk1);
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 500l);
    //todo: add exception handling here?
    //nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

TEST_F(TopologyControllerTest, testRemoveParent) {
    startCoordinator();
    //ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    sleep(sleeptime);
    if (!TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5)) {
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    if (!TestUtils::waitForWorkers(coordinatorConfig->restPort.getValue(), 5, 1)) {
        bool stopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(stopWrk1);
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }

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
    nlohmann::json res;
    try {
        res = nlohmann::json::parse(response.text);
    } catch (std::exception& e) {
        NES_DEBUG("Exception while parsing json");
        bool stopWrk1 = wrk1->stop(true);
        EXPECT_TRUE(stopWrk1);
        bool stopCrd = coordinator->stopCoordinator(true);
        NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
        EXPECT_TRUE(stopCrd);
        FAIL();
    }
    EXPECT_EQ(response.status_code, 200l);
    NES_DEBUG(response.text);
    //nlohmann::json res = nlohmann::json::parse(response.text);
    NES_DEBUG(res.dump());
    EXPECT_EQ(res["success"], true);
    bool stopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(stopWrk1);
    bool stopCrd = coordinator->stopCoordinator(true);
    NES_DEBUG("shut down coordinator with rest port " << coordinatorConfig->restPort);
    EXPECT_TRUE(stopCrd);
}

}// namespace NES
