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
#include <gtest/gtest.h>//

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

#include "../util/NesBaseTest.hpp"

using namespace std;
namespace NES {

class UpdateTopologyRemoteTest : public Testing::NESBaseTest {
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

    static void TearDownTestCase() { std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl; }
};

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnId) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    auto workerConfig1 = WorkerConfiguration::create();
    auto workerConfig2 = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    coordinatorConfig->setNumberOfSlots(coordinatorNumberOfSlots);

    workerConfig1->setNumberOfSlots(workerNumberOfSlots);
    workerConfig2->setNumberOfSlots(workerNumberOfSlots);

    coordinatorConfig->setNumberOfSlots(coordinatorNumberOfSlots);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    auto port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0ull);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker");
    auto node1RpcPort = getAvailablePort();
    auto node1DataPort = getAvailablePort();
    workerConfig1->setCoordinatorPort(*rpcCoordinatorPort);
    workerConfig1->setRpcPort(*node1RpcPort);
    workerConfig1->setDataPort(*node1DataPort);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");


    auto node2RpcPort = getAvailablePort();
    auto node2DataPort = getAvailablePort();
    workerConfig2->setCoordinatorPort(*rpcCoordinatorPort);
    workerConfig2->setRpcPort(*node2RpcPort);
    workerConfig2->setDataPort(*node2DataPort);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();

//    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, *rpcCoordinatorPort)); // this is the worker inside the coordinator
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, *node1RpcPort));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, *node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
//    EXPECT_TRUE(rootNode->getGrpcPort() == *rpcCoordinatorPort);
    EXPECT_TRUE(rootNode->getChildren().size() == 2);
    EXPECT_TRUE(rootNode->getAvailableResources() == coordinatorNumberOfSlots);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    EXPECT_TRUE(node1->getGrpcPort() == *node1RpcPort);
    EXPECT_TRUE(node1->getAvailableResources() == workerNumberOfSlots);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    EXPECT_TRUE(node2->getGrpcPort() == *node2RpcPort);
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
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    auto workerConfig1 = WorkerConfiguration::create();
    auto workerConfig2 = WorkerConfiguration::create();

    coordinatorConfig->setRpcPort(*rpcCoordinatorPort);
    coordinatorConfig->setRestPort(*restPort);
    coordinatorConfig->setNumberOfSlots(coordinatorNumberOfSlots);
    workerConfig1->setNumberOfSlots(workerNumberOfSlots);
    workerConfig2->setNumberOfSlots(workerNumberOfSlots);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker");
    auto node1RpcPort = getAvailablePort();
    auto node1DataPort = getAvailablePort();
    workerConfig1->setCoordinatorPort(*rpcCoordinatorPort);
    workerConfig1->setRpcPort(*node1RpcPort);
    workerConfig1->setDataPort(*node1DataPort);
    NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");

    auto node2RpcPort = getAvailablePort();
    auto node2DataPort = getAvailablePort();
    workerConfig2->setCoordinatorPort(*rpcCoordinatorPort);
    workerConfig2->setRpcPort(*node2RpcPort);
    workerConfig2->setDataPort(*node2DataPort);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();

//    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, *rpcCoordinatorPort)); // worker inside the coordinator
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, *node1RpcPort));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, *node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
//    EXPECT_TRUE(rootNode->getGrpcPort() == *rpcCoordinatorPort);
    EXPECT_TRUE(rootNode->getChildren().size() == 2);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    EXPECT_TRUE(node1->getGrpcPort() == *node1RpcPort);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    EXPECT_TRUE(node2->getGrpcPort() == *node2RpcPort);

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
