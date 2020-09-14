#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <ctime>
#include <gtest/gtest.h>

using namespace std;
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class UpdateTopologyRemoteTest : public testing::Test {
  public:
    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;

    uint16_t coordinatorNumberOfCpus = 128;
    uint16_t workerNumberOfCpus = 16;

    static void SetUpTestCase() {
        NES::setupLogging("UpdateTopologyRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup UpdateTopologyRemoteTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
    }

    static void TearDownTestCase() {
        std::cout << "Tear down UpdateTopologyRemoteTest test class." << std::endl;
    }
};

TEST_F(UpdateTopologyRemoteTest, addAndRemovePathWithOwnId) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort, coordinatorNumberOfCpus);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker");
    size_t node1RpcPort = port + 10;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, node1RpcPort, port + 11, workerNumberOfCpus, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");

    size_t node2RpcPort = port + 20;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, node2RpcPort, port + 21, workerNumberOfCpus, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();

    ASSERT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, port + 1));
    ASSERT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node1RpcPort));
    ASSERT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
    ASSERT_TRUE(rootNode->getGrpcPort() == port + 1);
    ASSERT_TRUE(rootNode->getChildren().size() == 2);
    ASSERT_TRUE(rootNode->getAvailableResources() == coordinatorNumberOfCpus);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    ASSERT_TRUE(node1->getGrpcPort() == node1RpcPort);
    ASSERT_TRUE(node1->getAvailableResources() == workerNumberOfCpus);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    ASSERT_TRUE(node2->getGrpcPort() == node2RpcPort);
    ASSERT_TRUE(node2->getAvailableResources() == workerNumberOfCpus);

    NES_INFO("ADD NEW PARENT");
    bool successAddPar = wrk->addParent(node2->getId());
    EXPECT_TRUE(successAddPar);
    ASSERT_TRUE(rootNode->getChildren().size() == 2);
    ASSERT_TRUE(node2->getChildren().size() == 1);
    ASSERT_TRUE(node2->getChildren()[0]->as<TopologyNode>()->getId() == node1->getId());

    NES_INFO("REMOVE NEW PARENT");
    bool successRemoveParent = wrk->removeParent(node2->getId());
    EXPECT_TRUE(successRemoveParent);
    EXPECT_TRUE(successAddPar);
    ASSERT_TRUE(rootNode->getChildren().size() == 2);
    ASSERT_TRUE(node2->getChildren().empty());

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
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort, coordinatorNumberOfCpus);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker");
    size_t node1RpcPort = port + 10;
    NesWorkerPtr wrk = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, node1RpcPort, port + 11, workerNumberOfCpus, NodeType::Sensor);
    bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart);
    NES_INFO("worker started successfully");

    size_t node2RpcPort = port + 20;
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), ipAddress, node2RpcPort, port + 21, workerNumberOfCpus, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("worker started successfully");

    TopologyPtr topology = crd->getTopology();

    ASSERT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, port + 1));
    ASSERT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node1RpcPort));
    ASSERT_TRUE(topology->nodeExistsWithIpAndPort(ipAddress, node2RpcPort));

    TopologyNodePtr rootNode = topology->getRoot();
    ASSERT_TRUE(rootNode->getGrpcPort() == port + 1);
    ASSERT_TRUE(rootNode->getChildren().size() == 2);
    TopologyNodePtr node1 = rootNode->getChildren()[0]->as<TopologyNode>();
    ASSERT_TRUE(node1->getGrpcPort() == node1RpcPort);
    TopologyNodePtr node2 = rootNode->getChildren()[1]->as<TopologyNode>();
    ASSERT_TRUE(node2->getGrpcPort() == node2RpcPort);

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
