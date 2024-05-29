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

#include <BaseIntegrationTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/Latency/SyntheticType.hpp>
#include <Util/Latency/Waypoint.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Index/NetworkCoordinateIndex.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProvider.hpp>

#include <Runtime/NodeEngine.hpp>
#include <Services/RequestHandlerService.hpp>

#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>
#include <gtest/gtest.h>

using std::map;
using std::string;
uint16_t timeout = 5;

namespace NES::Synthetic {

class NetworkCoordinateIntegrationTests : public Testing::BaseIntegrationTest, public testing::WithParamInterface<uint32_t> {
  public:
    std::string coordinate2 = "3, -5";
    std::string coordinate3 = "18, 24";
    std::string coordinate4 = "42, -1";
    std::chrono::duration<int64_t, std::milli> defaultTimeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);

    static void SetUpTestCase() {
        NES::Logger::setupLogging("NetworkCoordinateIntegrationTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NetworkCoordinateIntegrationTests test class.");
        std::vector<NES::Synthetic::DataTypes::Experimental::Waypoint> waypoints;
        waypoints.push_back({{10, -10}, 0});
        auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "singleNetworkCoordinate.csv";
        writeNCWaypointsToCSV(csvPath, waypoints);
    }

    /**
     * @brief wait until the topology contains the expected number of nodes so we can rely on these nodes being present for
     * the rest of the test
     * @param timeoutSeconds time to wait before aborting
     * @param nodes expected number of nodes
     * @param topology  the topology object to query
     * @return true if expected number of nodes was reached. false in case of timeout before number was reached
     */
    static bool waitForNodes(int timeoutSeconds, size_t nodes, TopologyPtr topology) {
        size_t numberOfNodes = 0;
        for (int i = 0; i < timeoutSeconds; ++i) {
            auto topoString = topology->toString();
            numberOfNodes = std::count(topoString.begin(), topoString.end(), '\n');
            numberOfNodes -= 1;
            if (numberOfNodes == nodes) {
                break;
            }
        }
        return numberOfNodes == nodes;
    }

    static void TearDownTestCase() {
        NES_INFO("Tear down NetworkCoordinateIntegrationTests class.");
        std::string singleNetworkCoordinatePath = std::filesystem::path(TEST_DATA_DIRECTORY) / "singleNetworkCoordinate.csv";
        remove(singleNetworkCoordinatePath.c_str());

    }

};


TEST_F(NetworkCoordinateIntegrationTests, testNetworkCoordinateFromCmd) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--networkCoordinates=10,11"};

    int argc = 1;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    ASSERT_EQ(workerConfigPtr->networkCoordinates.getValue(), NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(10, 11));
}

TEST_F(NetworkCoordinateIntegrationTests, testNCEnabledNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("starting the coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("starting the worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);

    NES_INFO("starting the worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->networkCoordinates.setValue(NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(coordinate2));
    wrkConf2->nodeSyntheticType.setValue(NES::Synthetic::Experimental::SyntheticType::NC_ENABLED);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);

    NES_INFO("starting the worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort = (port);
    wrkConf3->networkCoordinates.setValue(NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(coordinate3));
    wrkConf3->nodeSyntheticType.setValue(NES::Synthetic::Experimental::SyntheticType::NC_ENABLED);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart3);

    NES_INFO("starting the worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort = (port);
    wrkConf4->networkCoordinates.setValue(NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(coordinate4));
    wrkConf4->nodeSyntheticType.setValue(NES::Synthetic::Experimental::SyntheticType::NC_ENABLED);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart4);

    NES_INFO("worker1 started successfully");
    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    NES_INFO("worker 1 started and connected successfully");

    //TopologyPtr topology = crd->getTopology();
    TopologyPtr topology = crd->getTopology();

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    NES_INFO("worker 2 started and connected successfully");

    bool retConWrk3 = wrk3->connect();
    ASSERT_TRUE(retConWrk3);
    NES_INFO("worker 3 started and connected successfully");

    bool retConWrk4 = wrk4->connect();
    ASSERT_TRUE(retConWrk4);
    NES_INFO("worker 4 started and connected successfully");

    ASSERT_TRUE(waitForNodes(5, 5, topology));

    TopologyNodePtr node1 = topology->getCopyOfTopologyNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->getCopyOfTopologyNodeWithId(wrk2->getWorkerId());
    TopologyNodePtr node3 = topology->getCopyOfTopologyNodeWithId(wrk3->getWorkerId());
    TopologyNodePtr node4 = topology->getCopyOfTopologyNodeWithId(wrk4->getWorkerId());

    //checking coordinates
    ASSERT_EQ(topology->getNetworkCoordinateForNode(node2->getId()),
              NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(3, -5));
    topology->updateNetworkCoordinate(node2->getId(),
                                NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(6, -10));
    ASSERT_EQ(topology->getNetworkCoordinateForNode(node2->getId()),
              NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(6, -10));

    NES_INFO("NEIGHBORS");
    auto inRange =
        topology->getTopologyNodeIdsInNetworkRange(NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(0, 0),
                                     50);
    ASSERT_EQ(inRange.size(), (size_t) 3);

    topology->updateNetworkCoordinate(node3->getId(),
                                NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(-10, -25));

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);

    bool retStopWrk4 = wrk4->stop(false);
    ASSERT_TRUE(retStopWrk4);
}
//TODO: Check out the other tests to see if there are any instances where a worker is started without connect
TEST_F(NetworkCoordinateIntegrationTests, testGetNetworkCoordinateViaRPC) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;

    auto rpcPortWorker1 = getAvailablePort();

    NES_INFO("getNetworkCoordinateViaRPCTest: starting the coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("getNetworkCoordinateViaRPCTest: coordinator started successfully");

    NES_DEBUG("getNetworkCoordinateViaRPCTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->rpcPort=*rpcPortWorker1;
    workerConfig1->nodeSyntheticType.setValue(NES::Synthetic::Experimental::SyntheticType::NC_ENABLED);
    workerConfig1->latencyConfiguration.networkCoordinateProviderType.setValue(
        NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderType::CSV);
    workerConfig1->latencyConfiguration.networkCoordinateProviderConfig.setValue(std::filesystem::path(TEST_DATA_DIRECTORY)
                                                                            / "singleNetworkCoordinate.csv");

    auto singleNetworkCoordinateCSVPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "singleNetworkCoordinate.csv";

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));

    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    NES_INFO("getNetworkCoordinateViaRPCTest: Worker1 started successfully");

    WorkerRPCClientPtr client = WorkerRPCClient::create();

    auto rpcPortWorker2 = getAvailablePort();
    auto rpcPortWorker3 = getAvailablePort();

    auto coord1 = client->getNCWaypoint("127.0.0.1:" + std::to_string(*rpcPortWorker1));
    ASSERT_TRUE(coord1.getCoordinate().isValid());

    auto singleWaypoint = getNCWaypointsFromCSV(singleNetworkCoordinateCSVPath, 0);

    auto singleCoordinate = singleWaypoint.front().getCoordinate();

    ASSERT_EQ(coord1.getCoordinate(), singleCoordinate);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    //test getting coordinate of nc enabled node
    NES_INFO("start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    workerConfig2->rpcPort = *rpcPortWorker2;
    workerConfig2->networkCoordinates.setValue(NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(coordinate2));
    workerConfig2->nodeSyntheticType.setValue(NES::Synthetic::Experimental::SyntheticType::NC_ENABLED);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    NES_DEBUG("Starting worker 2 without connect.");
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart2);

    auto coord2 = client->getNCWaypoint("127.0.0.1:" + std::to_string(*rpcPortWorker2));
    ASSERT_TRUE(coord2.getCoordinate().isValid());
    ASSERT_EQ(coord2.getCoordinate(), NES::Synthetic::DataTypes::Experimental::NetworkCoordinate::fromString(coordinate2));

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);


    //test getting coordinate of node that does not have a coordinate
    NES_INFO("start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    workerConfig3->rpcPort = *rpcPortWorker3;
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart3);

    auto coord3 = client->getNCWaypoint("127.0.0.1:" + std::to_string(*rpcPortWorker3));
    ASSERT_FALSE(coord3.getCoordinate().isValid());

    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);

    //test getting coordinate of non existent node
    auto coord4 = client->getNCWaypoint("127.0.0.1:9999");
    ASSERT_FALSE(coord4.getCoordinate().isValid());

    NES_INFO("testGetNetworkCoordinateViaRPC: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("testGetNetworkCoordinateViaRPC: Test finished");
}


}

