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

#include <../util/NesBaseTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/GeographicalLocation.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

using namespace std;
namespace NES {
using namespace Configurations;

class GeoLocationTests : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoLocationTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GeoLocationTests test class.");
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";

    static void TearDownTestCase() { NES_INFO("Tear down GeoLocationTests class."); }
};

TEST_F(GeoLocationTests, testFieldNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
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

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(GeographicalLocation::fromString(location2));
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    cout << "start worker 3" << endl;
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort = (port);
    wrkConf3->locationCoordinates.setValue(GeographicalLocation::fromString(location3));
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

    cout << "start worker 4" << endl;
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort = (port);
    wrkConf4->locationCoordinates.setValue(GeographicalLocation::fromString(location4));
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart4);

    cout << "worker1 started successfully" << endl;
    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 started connected " << endl;

    TopologyPtr topology = crd->getTopology();
    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 0);

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 started connected " << endl;

    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 1);

    bool retConWrk3 = wrk3->connect();
    EXPECT_TRUE(retConWrk3);
    cout << "worker 3 started connected " << endl;

    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 2);

    bool retConWrk4 = wrk4->connect();
    EXPECT_TRUE(retConWrk4);
    cout << "worker 4 started connected " << endl;

    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 3);

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());
    TopologyNodePtr node3 = topology->findNodeWithId(wrk3->getWorkerId());
    TopologyNodePtr node4 = topology->findNodeWithId(wrk4->getWorkerId());

    //checking fixedCoordinates
    EXPECT_EQ(node2->getFixedCoordinates().value(), GeographicalLocation(52.53736960143897, 13.299134894776092));
    EXPECT_EQ(topology->getClosestNodeTo(node4), node3);
    EXPECT_EQ(topology->getClosestNodeTo(node4->getFixedCoordinates().value()).value(), node4);
    topology->setPhysicalNodeFixedCoordinates(node2, GeographicalLocation(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(topology->getClosestNodeTo(node4), node2);
    EXPECT_EQ(node2->getFixedCoordinates().value(), GeographicalLocation(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 3);
    NES_INFO("NEIGHBORS");
    auto inRange = topology->getNodesInRange(GeographicalLocation(52.53736960143897, 13.299134894776092), 50.0);
    EXPECT_EQ(inRange.size(), (size_t) 3);
    auto inRangeAtWorker = wrk2->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 3);
    //moving node 3 to hamburg (more than 100km away
    topology->setPhysicalNodeFixedCoordinates(node3, GeographicalLocation(53.559524264262194, 10.039384739854102));

    //node 3 should not have any nodes within a radius of 100km
    EXPECT_EQ(topology->getClosestNodeTo(node3, 100).has_value(), false);

    //because node 3 is in hamburg now, we will only get 2 nodes in a radius of 100km (node 3 itself and node 4)
    inRangeAtWorker = wrk2->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 2);
    EXPECT_EQ(inRangeAtWorker.at(1).first, wrk4->getWorkerId());
    EXPECT_EQ(inRangeAtWorker.at(1).second, wrk4->getCurrentOrPermanentGeoLoc());

    //when looking within a radius of 500km we will find all nodes again
    inRangeAtWorker = wrk2->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 3);
    //if we remove one of the other nodes, there should be one node less in the radius of 500 km
    topology->removePhysicalNode(topology->findNodeWithId(wrk3->getWorkerId()));
    inRangeAtWorker = wrk2->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 2);

    //location far away from all the other nodes should not have any closest node
    EXPECT_EQ(topology->getClosestNodeTo(GeographicalLocation(-53.559524264262194, -10.039384739854102), 100).has_value(), false);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);

    bool retStopWrk4 = wrk4->stop(false);
    EXPECT_TRUE(retStopWrk4);
}

TEST_F(GeoLocationTests, testMobileNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
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
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(GeographicalLocation::fromString(location2));
    wrkConf1->isMobile.setValue(true);
    wrkConf1->locationSourceType.setValue("csv");
    wrkConf1->locationSourceConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);

    cout << "start worker 2" << endl;
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(GeographicalLocation::fromString(location2));
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    cout << "worker1 started successfully" << endl;
    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    cout << "worker 1 started connected " << endl;

    TopologyPtr topology = crd->getTopology();
    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 0);

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    cout << "worker 2 started connected " << endl;

    EXPECT_EQ(topology->getSizeOfPointIndex(), (size_t) 1);

    EXPECT_EQ(wrk1->isMobileNode(), true);
    EXPECT_EQ(wrk2->isMobileNode(), false);

    EXPECT_EQ(wrk1->isFieldNode(), false);
    EXPECT_EQ(wrk2->isFieldNode(), true);

    EXPECT_EQ(wrk1->getCurrentOrPermanentGeoLoc(), GeographicalLocation(52.55227464714949, 13.351743136322877));
    EXPECT_EQ(wrk2->getCurrentOrPermanentGeoLoc(), GeographicalLocation::fromString(location2));

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());

    EXPECT_EQ(node1->isMobileNode(), true);
    EXPECT_EQ(node2->isMobileNode(), false);

    EXPECT_EQ(node1->isFieldNode(), false);
    EXPECT_EQ(node2->isFieldNode(), true);

    EXPECT_EQ(node1->getFixedCoordinates(), nullopt);
    EXPECT_EQ(node2->getFixedCoordinates(), GeographicalLocation::fromString(location2));

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);
}

TEST_F(GeoLocationTests, testLocationFromCmd) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--fixedLocationCoordinates=23.88,-3.4"};
    int argc = 1;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), GeographicalLocation(23.88, -3.4));
}

TEST_F(GeoLocationTests, testInvalidLocationFromCmd) {
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--fixedLocationCoordinates=230.88,-3.4"};
    int argc = 1;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    EXPECT_THROW(workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams), CoordinatesOutOfRangeException);
}

TEST_F(GeoLocationTests, DISABLED_testLocationFromConfig) {
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyFieldNode.yaml");
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), GeographicalLocation(45, -30));
}

}// namespace NES
