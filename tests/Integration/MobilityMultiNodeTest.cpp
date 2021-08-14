/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Mobility/LocationService.h>
#include <Util/Logger.hpp>
#include <cpprest/http_client.h>

namespace NES {

static int sleepTime = 100;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class MobilityNodesTest : public testing::Test {
  protected:
    NesCoordinatorPtr crd;
    std::string apiEndpoint;
    uint64_t port;

  public:
    static void SetUpTestCase() {
        NES::setupLogging("MobilityRESTEndpointTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MobilityRESTEndpointTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        apiEndpoint = "http://127.0.0.1:" + std::to_string(restPort) + "/v1/nes";

        CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
        coordinatorConfig->setRpcPort(rpcPort);
        coordinatorConfig->setRestPort(restPort);

        NES_INFO("MobilityRESTEndpointTest: Start coordinator");
        crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        port = crd->startCoordinator(/**blocking**/ false);

        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));

        EXPECT_NE(port, 0u);
        NES_INFO("MobilityRESTEndpointTest: Coordinator started successfully");
    }

    void TearDown() override {
        NES_INFO("MobilityRESTEndpointTest: Stop Coordinator");
        bool retStopCord = crd->stopCoordinator(true);
        EXPECT_TRUE(retStopCord);
        NES_INFO("MobilityRESTEndpointTest: Test finished");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MobilityRESTEndpointTest test class."); }
};

TEST_F(MobilityNodesTest, testAddLocationEnabledWorker) {
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("RESTEndpointTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setCoordinatorRestPort(restPort);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    workerConfig->setWorkerName("test_worker");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    wrk1->setWithRegisterLocation(true);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    EXPECT_TRUE(crd->getLocationService()->getLocationCatalog()->contains("test_worker"));

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
}

}
