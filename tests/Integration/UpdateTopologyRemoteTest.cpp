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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;
uint64_t restPort = 8081;

class UpdateTopologyRemoteTest : public testing::Test {
  public:
    // set the default numberOfSlots to the number of processor
    const uint16_t processorCount = std::thread::hardware_concurrency();
    std::string ipAddress = "127.0.0.1";
    uint16_t coordinatorNumberOfSlots = 12;
    uint16_t workerNumberOfSlots = 6;

    static void SetUpTestCase() {
        NES::setupLogging("UpdateTopologyRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UpdateTopologyRemoteTest test class.");
    }

    void SetUp() override { rpcPort = rpcPort + 30; }

    static void TearDownTestCase() { std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl; }
};

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnId) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumberOfSlots(coordinatorNumberOfSlots);
    workerConfig->setNumberOfSlots(workerNumberOfSlots);
    workerConfig->setCoordinatorPort(rpcPort);

    coordinatorConfig->setNumberOfSlots(coordinatorNumberOfSlots);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker");
    uint64_t node1RpcPort = port + 10;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node1RpcPort);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");

    uint64_t node2RpcPort = port + 20;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node2RpcPort);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();

    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, port + 1));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node1RpcPort));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
    EXPECT_TRUE(rootNode->getGrpcPort() == port + 1);
    EXPECT_TRUE(rootNode->getChildren().size() == 2);
    EXPECT_TRUE(rootNode->getAvailableResources() == coordinatorNumberOfSlots);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    EXPECT_TRUE(node1->getGrpcPort() == node1RpcPort);
    EXPECT_TRUE(node1->getAvailableResources() == workerNumberOfSlots);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    EXPECT_TRUE(node2->getGrpcPort() == node2RpcPort);
    EXPECT_TRUE(node2->getAvailableResources() == workerNumberOfSlots);

    NES_INFO("ADD NEW PARENT");
    bool successAddPar = wrk->addParent(node2->getId());
    EXPECT_TRUE(successAddPar);
    EXPECT_TRUE(rootNode->getChildren().size() == 2);
    EXPECT_TRUE(node2->getChildren().size() == 1);
    EXPECT_TRUE(node2->getChildren()[0]->as<TopologyNode>()->getId() == node1->getId());

    NES_INFO("REMOVE NEW PARENT");
    bool successRemoveParent = wrk->removeParent(node2->getId());
    EXPECT_TRUE(successRemoveParent);
    EXPECT_TRUE(successAddPar);
    EXPECT_TRUE(rootNode->getChildren().size() == 2);
    EXPECT_TRUE(node2->getChildren().empty());

    NES_INFO("stopping worker");
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("stopping worker 2");
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnIdAndSelf) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    coordinatorConfig->setNumberOfSlots(coordinatorNumberOfSlots);
    workerConfig->setNumberOfSlots(workerNumberOfSlots);
    workerConfig->setCoordinatorPort(rpcPort);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker");
    uint64_t node1RpcPort = port + 10;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node1RpcPort);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");

    uint64_t node2RpcPort = port + 20;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node2RpcPort);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();

    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, port + 1));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node1RpcPort));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
    EXPECT_TRUE(rootNode->getGrpcPort() == port + 1);
    EXPECT_TRUE(rootNode->getChildren().size() == 2);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    EXPECT_TRUE(node1->getGrpcPort() == node1RpcPort);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    EXPECT_TRUE(node2->getGrpcPort() == node2RpcPort);

    NES_INFO("REMOVE NEW PARENT");
    bool successRemoveParent = wrk->removeParent(node1->getId());
    EXPECT_TRUE(!successRemoveParent);

    NES_INFO("stopping worker");
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("stopping worker 2");
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}// namespace NES
