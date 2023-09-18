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

#include <iostream>

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Util/Logger/Logger.hpp>
#include <fstream>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

using namespace Configurations;

class MultiWorkerTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("MultiWorkerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup MultiWorkerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MultiWorkerTest class."); }
};

TEST_F(MultiWorkerTest, startStopWorkerCoordinatorSingle) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startStopWorkerCoordinator) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    cout << "worker1 started successfully" << endl;

    cout << "stopping worker" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stopping worker" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startStopCoordinatorWorker) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    cout << "worker1 started successfully" << endl;

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    cout << "stopping worker 1" << endl;
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    cout << "stopping worker 2" << endl;
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);
}

TEST_F(MultiWorkerTest, startConnectStopWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->coordinatorHealthCheckWaitTime = 1;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    wrkConf1->workerHealthCheckWaitTime = 1;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->workerHealthCheckWaitTime = 1;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);
    cout << "worker1 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 connected " << endl;

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWithConnectStopWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    cout << "worker1 started successfully" << endl;

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startConnectStopWithoutDisconnectWorkerCoordinator) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    cout << "start worker 1" << endl;
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);
    cout << "worker1 started successfully" << endl;

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);
    cout << "worker1 started successfully" << endl;

    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 started connected " << endl;

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 started connected " << endl;

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);
}

TEST_F(MultiWorkerTest, testMultipleWorker) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    cout << "start coordinator" << endl;
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    cout << "coordinator started successfully" << endl;

    uint64_t numWorkers = 3;
    std::vector<NesWorkerPtr> wPtrs;
    for (uint64_t i = 0; i < numWorkers; i++) {
        cout << "start worker" << i << endl;
        auto wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort = port;
        wPtrs.push_back(std::make_shared<NesWorker>(std::move(wrkConf)));
        bool retStart = wPtrs[i]->start(/**blocking**/ false, /**withConnect**/ false);
        EXPECT_TRUE(retStart);
    }

    //connect 10 worker
    for (uint64_t i = 0; i < numWorkers; i++) {
        cout << "connect worker" << i << endl;
        bool retConWrk = wPtrs[i]->connect();
        EXPECT_TRUE(retConWrk);
    }

    //disconnect 10 worker
    for (uint64_t i = 0; i < numWorkers; i++) {
        cout << "disconnect worker" << i << endl;
        bool retConWrk = wPtrs[i]->disconnect();
        EXPECT_TRUE(retConWrk);
    }

    //stop 10 worker
    for (uint64_t i = 0; i < numWorkers; i++) {
        cout << "stop worker" << i << endl;
        bool retConWrk = wPtrs[i]->stop(false);
        EXPECT_TRUE(retConWrk);
    }

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWorkersWithoutWorkerId) {
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with no configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());

    // start worker 2, with no configured workerId
    NES_DEBUG("Starting worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = port;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    // expected behavior: worker 2 gets assigned the next available workerId, 3
    EXPECT_EQ(wrk2->getWorkerId(), 3u);
    NES_DEBUG("Worker 2 started successfully with workerId {}", wrk2->getWorkerId());

    // stop worker 1
    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
    NES_DEBUG("Worker 1 stopped successfully");

    NES_DEBUG("Stopping worker 2");
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWorkerWithWorkerIdBelongingToActiveWorker) {
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with no configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());

    NES_DEBUG("Starting another worker with the workerId of an active worker");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = port;
    wrkConf2->workerId = 2u;
    NesWorkerPtr wrk2copy = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2copy = wrk2copy->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2copy);
    // expected behavior: this workerId belongs to an active worker, therefore the next available workerId will be assigned
    EXPECT_EQ(wrk2copy->getWorkerId(), 3u);
    NES_DEBUG("Worker 2 started successfully with workerId {}", wrk2copy->getWorkerId());

    // stop worker 1
    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
    NES_DEBUG("Worker 1 stopped successfully");

    // stop worker 2
    NES_DEBUG("Stopping worker 2");
    bool retStopWrk2 = wrk2copy->stop(false);
    EXPECT_TRUE(retStopWrk2);
    NES_DEBUG("Worker 2 stopped successfully");

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWorkerWithWorkerIdBelongingToInactiveWorker) {
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with no configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());

    // stop worker 1
    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
    NES_DEBUG("Worker 1 stopped successfully");

    // wait one second to make sure the workers failure is seen
    usleep(1000000);

    NES_DEBUG("Restarting worker 1");
    WorkerConfigurationPtr wrkConf1restart = WorkerConfiguration::create();
    wrkConf1restart->coordinatorPort = port;
    wrkConf1restart->workerId = 2u;
    NesWorkerPtr wrk1restart = std::make_shared<NesWorker>(std::move(wrkConf1restart));
    bool retRestart1 = wrk1restart->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retRestart1);
    bool connectedAfterRestart = wrk1restart->connect();
    // expected behavior: worker 1 restarts with the same workerId that was initially assigned, 2
    EXPECT_TRUE(connectedAfterRestart);
    EXPECT_EQ(wrk1restart->getWorkerId(), 2u);
    NES_DEBUG("Worker1 started successfully with workerId {}", wrk1restart->getWorkerId());

    // stop worker 1
    NES_DEBUG("Stopping worker 1");
    bool retStopWrk2 = wrk1restart->stop(false);
    EXPECT_TRUE(retStopWrk2);
    NES_DEBUG("Worker 1 stopped successfully");

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWorkerWithMisconfiguredWorkerId) {
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with a configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->workerId = 123;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());

    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, startWorkerWithCorrectNextWorkerId) {
    // make sure that even if the given workerId is the next available one, the counter is still increased
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with a configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    wrkConf->workerId = 2;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());

    // start worker 2, with no configured workerId
    NES_DEBUG("Starting worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = port;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    // expected behavior: worker 2 gets assigned the next available workerId, 3
    EXPECT_EQ(wrk2->getWorkerId(), 3u);
    NES_DEBUG("Worker 2 started successfully with workerId {}", wrk2->getWorkerId());

    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("Stopping worker 2");
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, checkPersistenceOfNewWorkerIdInYaml) {
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with no configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    std::string configPath = std::string(TEST_DATA_DIRECTORY) + "emptyWorker.yaml";
    wrkConf->configPath = configPath;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());
    std::ifstream configFile(configPath);
    std::stringstream ss;
    ss << configFile.rdbuf();
    std::string yamlContent = ss.str();
    EXPECT_NE(yamlContent.find("workerId: 2"), std::string::npos);

    // stop worker 1
    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
    NES_DEBUG("Worker 1 stopped successfully");

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(MultiWorkerTest, checkPersistenceOfOverwrittenWorkerIdInYaml) {
    // start the coordinator
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_DEBUG("Starting coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_DEBUG("Coordinator started successfully");

    // start worker 1, with no configured workerId
    NES_DEBUG("Starting worker 1");
    WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
    wrkConf->coordinatorPort = port;
    std::string configPath = std::string(TEST_DATA_DIRECTORY) + "emptyWorker.yaml";
    wrkConf->configPath = configPath;
    wrkConf->workerId = 3u;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    // expected behavior: worker 1 gets assigned the next available workerId, 2
    EXPECT_EQ(wrk1->getWorkerId(), 2u);
    NES_DEBUG("Worker 1 started successfully with workerId {}", wrk1->getWorkerId());
    std::ifstream configFile(configPath);
    std::stringstream ss;
    ss << configFile.rdbuf();
    std::string yamlContent = ss.str();
    EXPECT_NE(yamlContent.find("workerId: 2"), std::string::npos);

    // stop worker 1
    NES_DEBUG("Stopping worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
    NES_DEBUG("Worker 1 stopped successfully");

    NES_DEBUG("Stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES
