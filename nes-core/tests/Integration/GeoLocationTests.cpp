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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
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

class GeoLocationTests : public testing::Test {

  public:
    // set the default numberOfSlots to the number of processor
    const uint16_t processorCount = std::thread::hardware_concurrency();
    std::string ipAddress = "127.0.0.1";
    uint16_t coordinatorNumberOfSlots = 12;
    uint16_t workerNumberOfSlots = 6;
    std::string location = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";


    static void SetUpTestCase() {
        NES::setupLogging("UpdateTopologyRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UpdateTopologyRemoteTest test class.");
    }

    void SetUp() override { rpcPort = rpcPort + 30; }

    static void TearDownTestCase() { std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl; }
};

TEST_F(GeoLocationTests, createNodeWithLocation) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();

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
    NesWorkerPtr wrk = std::make_shared<NesWorker>(workerConfig);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();
    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 0);

    uint64_t node2RpcPort = port + 20;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node2RpcPort);
    workerConfig->setDataPort(port + 21);
    workerConfig->setLocationCoordinates(location);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 1);

    uint64_t node3RpcPort = port + 30;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node3RpcPort);
    workerConfig->setDataPort(port + 31);
    workerConfig->setLocationCoordinates(location3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(workerConfig);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    NES_INFO("worker started successfully");


    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 2);

    uint64_t node4RpcPort = port + 40;
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(node4RpcPort);
    workerConfig->setDataPort(port + 41);
    workerConfig->setLocationCoordinates(location4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(workerConfig);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_INFO("worker started successfully");


    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 3);
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, port + 1));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node1RpcPort));
    EXPECT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
    EXPECT_TRUE(rootNode->getGrpcPort() == port + 1);
    EXPECT_TRUE(rootNode->getChildren().size() == 4);
    EXPECT_TRUE(rootNode->getAvailableResources() == coordinatorNumberOfSlots);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    EXPECT_TRUE(node1->getGrpcPort() == node1RpcPort);
    EXPECT_TRUE(node1->getAvailableResources() == workerNumberOfSlots);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    EXPECT_TRUE(node2->getGrpcPort() == node2RpcPort);
    EXPECT_TRUE(node2->getAvailableResources() == workerNumberOfSlots);

    TopologyNodePtr node3 = rootNode->getChildren()[2]->as<TopologyNode>();
    EXPECT_TRUE(node3->getGrpcPort() == node3RpcPort);
    EXPECT_TRUE(node3->getAvailableResources() == workerNumberOfSlots);

    TopologyNodePtr node4 = rootNode->getChildren()[3]->as<TopologyNode>();
    EXPECT_TRUE(node4->getGrpcPort() == node4RpcPort);
    EXPECT_TRUE(node4->getAvailableResources() == workerNumberOfSlots);

    //checking coordinates
    EXPECT_EQ(node2->getCoordinates(), std::make_tuple(52.53736960143897, 13.299134894776092));
    EXPECT_EQ(topology->getClosestNodeTo(node4), node3);
    EXPECT_EQ(topology->getClosestNodeTo(node4->getCoordinates().value()), node4);
    topology->setPhysicalNodePosition(node2, std::make_tuple(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(topology->getClosestNodeTo(node4), node2);
    EXPECT_EQ(node2->getCoordinates(), std::make_tuple(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 3);
    NES_INFO("NEIGHBORS");
    auto inRange = topology->getNodesInRange(std::make_tuple(52.53736960143897, 13.299134894776092), 50.0);
    EXPECT_EQ(inRange.size(), (size_t) 3);
    auto inRangeAtWorker = wrk2->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 3);
    //moving node 3 to hamburg (more than 100km away
    topology->setPhysicalNodePosition(node3, std::make_tuple(53.559524264262194, 10.039384739854102));

    //node 3 should not have any nodes within a radius of 100km
    EXPECT_EQ(topology->getClosestNodeTo(node3, 100).has_value(), false);

    //because node 3 is in hamburg now, we will only get 2 nodes in a radius of 100km (node 2 itself and node 4)
    inRangeAtWorker = wrk2->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 2);

    //when looking within a radius of 500km we will find all nodes again
    inRangeAtWorker = wrk2->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 3);

    //location far away from all the other nodes should not have any closest node
    EXPECT_EQ(topology->getClosestNodeTo(std::make_tuple(-53.559524264262194, -10.039384739854102), 100).has_value(), false);

    NES_INFO("stopping worker");
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    NES_INFO("stopping worker 2");
    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);


    NES_INFO("stopping worker 2");
    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);

    NES_INFO("stopping worker 2");
    bool retStopWrk4 = wrk4->stop(false);
    EXPECT_TRUE(retStopWrk4);

    NES_INFO("stopping coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

}
}
