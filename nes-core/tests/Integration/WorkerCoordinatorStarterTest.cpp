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

#include <iostream>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using std::cout;
using std::endl;
#define DEBUG_OUTPUT
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
// TODO use grpc async queue to fix this issue - I am currently increasing the rpc port by 30 on every test! this is very bad!
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class WorkerCoordinatorStarterTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("WorkerCoordinatorStarterTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup WorkerCoordinatorStarterTest test class.");
    }

    void SetUp() override { rpcPort = rpcPort + 30; }

    void TearDown() override { std::cout << "Tear down WorkerCoordinatorStarterTest class." << std::endl; }
};

TEST_F(WorkerCoordinatorStarterTest, startStopWorkerCoordinator) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started connected successfully" << endl;
    cout << "wakeup" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    cout << crd.use_count() << " use cnt coord" << endl;
    EXPECT_TRUE(retStopCord);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startStopWorkerCoordinator10times) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    for (uint64_t i = 0; i < 10; i++) {
        cout << "iteration = " << i << endl;
        NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        uint64_t port = crd->startCoordinator(/**blocking**/ false);
        EXPECT_NE(port, 0ULL);
        cout << "coordinator started successfully" << endl;

        cout << "start worker" << endl;
        workerConfig->setCoordinatorPort(port);
        workerConfig->setRpcPort(port + 10);
        workerConfig->setDataPort(port + 11);
        NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);
        cout << "worker started connected successfully" << endl;

        cout << "wakeup" << endl;

        cout << "stopping worker" << endl;
        bool retStopWrk = wrk->stop(false);
        EXPECT_TRUE(retStopWrk);

        cout << "stopping coordinator" << endl;
        bool retStopCord = crd->stopCoordinator(false);
        cout << crd.use_count() << " use cnt" << endl;
        cout << wrk.use_count() << " use cnt" << endl;
        crd.reset();
        wrk.reset();
        cout << crd.use_count() << " use cnt" << endl;
        cout << wrk.use_count() << " use cnt" << endl;
        EXPECT_TRUE(retStopCord);
    }
}
TEST_F(WorkerCoordinatorStarterTest, startStopCoordinatorWorker) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    cout << "worker started connected successfully" << endl;

    cout << "wakeup" << endl;

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startConnectStopWorkerCoordinator) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startConnectStopWithoutDisconnectWorkerCoordinator) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startConnectDisconnectStopWorkerCoordinator) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker got disconnected " << endl;

    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    rpcPort += 30;
}

TEST_F(WorkerCoordinatorStarterTest, startReconnectStopWorkerCoordinator) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ULL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool retConWrk = wrk->connect();
    EXPECT_TRUE(retConWrk);
    cout << "worker got connected " << endl;

    bool retDisWrk = wrk->disconnect();
    EXPECT_TRUE(retDisWrk);
    cout << "worker got disconnected " << endl;

    bool retConWrk2 = wrk->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker got connected " << endl;

    bool retDisWrk2 = wrk->disconnect();
    EXPECT_TRUE(retDisWrk2);
    cout << "worker got disconnected " << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES
