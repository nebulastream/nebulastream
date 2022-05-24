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
#include <Common/Location.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Spatial/LocationIndex.hpp>
#include <Spatial/NodeLocationWrapper.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <Spatial/LocationProviderCSV.hpp>
#include <Util/TimeMeasurement.hpp>
#include <GRPC/WorkerRPCClient.hpp>

using std::string;
using std::map;
namespace NES {
using namespace Configurations;

class LocationIntegrationTests : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationIntegrationTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationIntegrationTests test class.");
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";

    static void TearDownTestCase() { NES_INFO("Tear down LocationIntegrationTests class."); }
};

TEST_F(LocationIntegrationTests, testFieldNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort = (port);
    wrkConf3->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location3));
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort = (port);
    wrkConf4->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location4));
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart4);

    NES_INFO("worker1 started successfully");
    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    NES_INFO("worker 1 started connected ");

    TopologyPtr topology = crd->getTopology();
    NES::Spatial::Index::Experimental::LocationIndexPtr geoTopology = topology->getLocationIndex();
    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 0);

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    NES_INFO("worker 2 started connected ");

    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 1);

    bool retConWrk3 = wrk3->connect();
    EXPECT_TRUE(retConWrk3);
    NES_INFO("worker 3 started connected ");

    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 2);

    bool retConWrk4 = wrk4->connect();
    EXPECT_TRUE(retConWrk4);
    NES_INFO("worker 4 started connected ");

    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 3);

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());
    TopologyNodePtr node3 = topology->findNodeWithId(wrk3->getWorkerId());
    TopologyNodePtr node4 = topology->findNodeWithId(wrk4->getWorkerId());

    //checking coordinates
    EXPECT_EQ(*(node2->getCoordinates()), NES::Spatial::Index::Experimental::Location(52.53736960143897, 13.299134894776092));
    EXPECT_EQ(geoTopology->getClosestNodeTo(node4), node3);
    EXPECT_EQ(geoTopology->getClosestNodeTo(*(node4->getCoordinates())).value(), node4);
    geoTopology->updateFieldNodeCoordinates(node2,
                                            NES::Spatial::Index::Experimental::Location(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(geoTopology->getClosestNodeTo(node4), node2);
    EXPECT_EQ(*(node2->getCoordinates()), NES::Spatial::Index::Experimental::Location(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 3);
    NES_INFO("NEIGHBORS");
    auto inRange =
        geoTopology->getNodesInRange(NES::Spatial::Index::Experimental::Location(52.53736960143897, 13.299134894776092), 50.0);
    EXPECT_EQ(inRange.size(), (size_t) 3);
    auto inRangeAtWorker = wrk2->getLocationWrapper()->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 3);
    //moving node 3 to hamburg (more than 100km away
    geoTopology->updateFieldNodeCoordinates(node3,
                                            NES::Spatial::Index::Experimental::Location(53.559524264262194, 10.039384739854102));

    //node 3 should not have any nodes within a radius of 100km
    EXPECT_EQ(geoTopology->getClosestNodeTo(node3, 100).has_value(), false);

    //because node 3 is in hamburg now, we will only get 2 nodes in a radius of 100km (node 3 itself and node 4)
    inRangeAtWorker = wrk2->getLocationWrapper()->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 2);
    EXPECT_EQ(inRangeAtWorker.at(1).first, wrk4->getWorkerId());
    EXPECT_EQ(inRangeAtWorker.at(1).second, *(wrk4->getLocationWrapper()->getLocation()));

    //when looking within a radius of 500km we will find all nodes again
    inRangeAtWorker = wrk2->getLocationWrapper()->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 3);
    //if we remove one of the other nodes, there should be one node less in the radius of 500 km
    topology->removePhysicalNode(topology->findNodeWithId(wrk3->getWorkerId()));
    inRangeAtWorker = wrk2->getLocationWrapper()->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker.size(), (size_t) 2);

    //location far away from all the other nodes should not have any closest node
    EXPECT_EQ(
        geoTopology->getClosestNodeTo(NES::Spatial::Index::Experimental::Location(-53.559524264262194, -10.039384739854102), 100)
            .has_value(),
        false);

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

TEST_F(LocationIntegrationTests, testMobileNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    wrkConf1->isMobile.setValue(true);
    wrkConf1->locationSourceType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->locationSourceConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    NES_INFO("worker1 started successfully");
    bool retConWrk1 = wrk1->connect();
    EXPECT_TRUE(retConWrk1);
    NES_INFO("worker 1 started connected ");

    TopologyPtr topology = crd->getTopology();
    NES::Spatial::Index::Experimental::LocationIndexPtr geoTopology = topology->getLocationIndex();
    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 0);

    bool retConWrk2 = wrk2->connect();
    EXPECT_TRUE(retConWrk2);
    NES_INFO("worker 2 started connected ");

    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 1);

    EXPECT_EQ(wrk1->getLocationWrapper()->isMobileNode(), true);
    EXPECT_EQ(wrk2->getLocationWrapper()->isMobileNode(), false);

    EXPECT_EQ(wrk1->getLocationWrapper()->isFieldNode(), false);
    EXPECT_EQ(wrk2->getLocationWrapper()->isFieldNode(), true);

    EXPECT_EQ(*(wrk1->getLocationWrapper()->getLocation()),
              NES::Spatial::Index::Experimental::Location(52.55227464714949, 13.351743136322877));
    EXPECT_EQ(*(wrk2->getLocationWrapper()->getLocation()), NES::Spatial::Index::Experimental::Location::fromString(location2));

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());

    EXPECT_EQ(node1->isMobileNode(), true);
    EXPECT_EQ(node2->isMobileNode(), false);

    EXPECT_EQ(node1->isFieldNode(), false);
    EXPECT_EQ(node2->isFieldNode(), true);

    EXPECT_TRUE(node1->getCoordinates()->isValid());
    EXPECT_EQ(*(node2->getCoordinates()), NES::Spatial::Index::Experimental::Location::fromString(location2));

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);
}

TEST_F(LocationIntegrationTests, testLocationFromCmd) {

    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--fieldNodeLocationCoordinates=23.88,-3.4"};
    int argc = 1;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams);
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), NES::Spatial::Index::Experimental::Location(23.88, -3.4));
}

TEST_F(LocationIntegrationTests, testInvalidLocationFromCmd) {
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    std::string argv[] = {"--fieldNodeLocationCoordinates=230.88,-3.4"};
    int argc = 1;

    std::map<string, string> commandLineParams;

    for (int i = 0; i < argc; ++i) {
        commandLineParams.insert(
            std::pair<string, string>(string(argv[i]).substr(0, string(argv[i]).find('=')),
                                      string(argv[i]).substr(string(argv[i]).find('=') + 1, string(argv[i]).length() - 1)));
    }

    EXPECT_THROW(workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams),
                 NES::Spatial::Index::Experimental::CoordinatesOutOfRangeException);
}

TEST_F(LocationIntegrationTests, testMovingDevice) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    wrkConf1->isMobile.setValue(true);
    wrkConf1->locationSourceType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->locationSourceConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);


    double pos1lat = 52.55227464714949;
    double pos1lng = 13.351743136322877;

    double pos2lat = 52.574709862890394;
    double pos2lng = 13.419206057808077;

    double pos3lat = 52.61756571840606;
    double pos3lng = 13.505980882863446;

    double pos4lat = 52.67219559419452;
    double pos4lng = 13.591124924963108;

    TopologyPtr topology = crd->getTopology();
    NES::Spatial::Index::Experimental::LocationIndexPtr geoTopology = topology->getLocationIndex();
    Timestamp beforeQuery = getTimestamp();
    TopologyNodePtr wrk1Node = topology->findNodeWithId(wrk1->getWorkerId());
    NES::Spatial::Index::Experimental::LocationPtr currentLocation = wrk1Node->getCoordinates();
    Timestamp afterQuery = getTimestamp();

    auto sourceCsv = dynamic_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV*>(wrk1->getLocationWrapper()->getLocationProvider().get());
    auto startTime = sourceCsv->getStarttime();
    auto timefirstLoc = startTime;
    auto timesecloc = startTime + 100000000;
    auto timethirdloc = startTime + 200000000;
    auto timefourthloc = startTime + 300000000;
    auto loopTime = startTime + 400000000;

#ifdef S2DEF
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_GE(currentLocation->getLatitude(), pos1lat);
            EXPECT_GE(currentLocation->getLongitude(), pos1lng);
            EXPECT_LE(currentLocation->getLatitude(), pos2lat);
            EXPECT_LE(currentLocation->getLongitude(), pos2lng);
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentLocation->getLatitude(), pos2lat);
            EXPECT_GE(currentLocation->getLongitude(), pos2lng);
            EXPECT_LE(currentLocation->getLatitude(), pos3lat);
            EXPECT_LE(currentLocation->getLongitude(), pos3lng);
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentLocation->getLatitude(), pos3lat);
            EXPECT_GE(currentLocation->getLongitude(), pos3lng);
            EXPECT_LE(currentLocation->getLatitude(), pos4lat);
            EXPECT_LE(currentLocation->getLongitude(), pos4lng);
        } else if (beforeQuery > timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentLocation), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        beforeQuery = getTimestamp();
        currentLocation = wrk1Node->getCoordinates();
        afterQuery = getTimestamp();
    }

#else
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_EQ(*(currentLocation.first), NES::Spatial::Index::Experimental::Location(pos1lat, pos1lng));
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_EQ(*(currentLocation.first), NES::Spatial::Index::Experimental::Location(pos2lat, pos2lng));
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_EQ(*(currentLocation.first), NES::Spatial::Index::Experimental::Location(pos3lat, pos3lng));
        } else if (beforeQuery > timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentLocation.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        beforeQuery = getTimestamp();
        currentLocation = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }
#endif
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
}
TEST_F(LocationIntegrationTests, DISABLED_testLocationFromConfig) {
    WorkerConfigurationPtr workerConfigPtr = std::make_shared<WorkerConfiguration>();
    workerConfigPtr->overwriteConfigWithYAMLFileInput(std::string(TEST_DATA_DIRECTORY) + "emptyFieldNode.yaml");
    EXPECT_EQ(workerConfigPtr->locationCoordinates.getValue(), NES::Spatial::Index::Experimental::Location(45, -30));
}

TEST_F(LocationIntegrationTests, testGetLocationViaRPC) {

    WorkerRPCClientPtr client = std::make_shared<WorkerRPCClient>();
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;

    //test getting location of mobile node
    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->rpcPort = rpcPortWrk1;
    wrkConf1->isMobile.setValue(true);
    wrkConf1->locationSourceType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->locationSourceConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);

    auto loc1 = client->getLocation("127.0.0.1:" + std::to_string(rpcPortWrk1));
    EXPECT_TRUE(loc1->isValid());
    EXPECT_EQ(*loc1, NES::Spatial::Index::Experimental::Location(52.55227464714949, 13.351743136322877));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    //test getting location of field node
    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->rpcPort = rpcPortWrk2;
    wrkConf2->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    auto loc2 = client->getLocation("127.0.0.1:" + std::to_string(rpcPortWrk2));
    EXPECT_TRUE(loc2->isValid());
    EXPECT_EQ(*loc2, NES::Spatial::Index::Experimental::Location::fromString(location2));

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    //test getting location of node which does not have a location
    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

    auto loc3 = client->getLocation("127.0.0.1:" + std::to_string(rpcPortWrk3));
    EXPECT_FALSE(loc3->isValid());

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);

    //test getting location of non existent node
    auto loc4 = client->getLocation("127.0.0.1:9999");
    EXPECT_FALSE(loc4->isValid());
}
}// namespace NES
