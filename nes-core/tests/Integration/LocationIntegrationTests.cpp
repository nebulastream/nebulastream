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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <NesBaseTest.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Mobility/LocationProvider.hpp>
#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Spatial/Mobility/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#include <Spatial/Mobility/TrajectoryPredictor.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Experimental/S2Utilities.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>
#include <cpprest/json.h>
#include <gtest/gtest.h>

using std::map;
using std::string;
uint16_t timeout = 5;
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
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort = (port);
    wrkConf3->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location3));
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort = (port);
    wrkConf4->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location4));
    wrkConf4->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
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
    EXPECT_EQ((node2->getCoordinates()), NES::Spatial::Index::Experimental::Location(52.53736960143897, 13.299134894776092));
    EXPECT_EQ(geoTopology->getClosestNodeTo(node4), node3);
    EXPECT_EQ(geoTopology->getClosestNodeTo((node4->getCoordinates())).value(), node4);
    geoTopology->updateFieldNodeCoordinates(node2,
                                            NES::Spatial::Index::Experimental::Location(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(geoTopology->getClosestNodeTo(node4), node2);
    EXPECT_EQ((node2->getCoordinates()), NES::Spatial::Index::Experimental::Location(52.51094383152051, 13.463078966025266));
    EXPECT_EQ(geoTopology->getSizeOfPointIndex(), (size_t) 3);
    NES_INFO("NEIGHBORS");
    auto inRange =
        geoTopology->getNodesInRange(NES::Spatial::Index::Experimental::Location(52.53736960143897, 13.299134894776092), 50.0);
    EXPECT_EQ(inRange.size(), (size_t) 3);
    auto inRangeAtWorker = wrk2->getLocationProvider()->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker->size(), (size_t) 3);
    //moving node 3 to hamburg (more than 100km away
    geoTopology->updateFieldNodeCoordinates(node3,
                                            NES::Spatial::Index::Experimental::Location(53.559524264262194, 10.039384739854102));

    //node 3 should not have any nodes within a radius of 100km
    EXPECT_EQ(geoTopology->getClosestNodeTo(node3, 100).has_value(), false);

    //because node 3 is in hamburg now, we will only get 2 nodes in a radius of 100km (node 3 itself and node 4)
    inRangeAtWorker = wrk2->getLocationProvider()->getNodeIdsInRange(100.0);
    EXPECT_EQ(inRangeAtWorker->size(), (size_t) 2);
    EXPECT_TRUE(inRangeAtWorker->count(wrk4->getWorkerId()));
    EXPECT_EQ(inRangeAtWorker->find(wrk4->getWorkerId())->second, (*wrk4->getLocationProvider()->getLocation()));

    //when looking within a radius of 500km we will find all nodes again
    inRangeAtWorker = wrk2->getLocationProvider()->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker->size(), (size_t) 3);
    //if we remove one of the other nodes, there should be one node less in the radius of 500 km
    topology->removePhysicalNode(topology->findNodeWithId(wrk3->getWorkerId()));
    inRangeAtWorker = wrk2->getLocationProvider()->getNodeIdsInRange(500.0);
    EXPECT_EQ(inRangeAtWorker->size(), (size_t) 2);

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
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
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

    EXPECT_EQ(wrk1->getLocationProvider()->getNodeType(), NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    EXPECT_EQ(wrk2->getLocationProvider()->getNodeType(), NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);

    EXPECT_EQ(*(wrk2->getLocationProvider()->getLocation()), NES::Spatial::Index::Experimental::Location::fromString(location2));

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());

    EXPECT_EQ(node1->getSpatialNodeType(), NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    EXPECT_EQ(node2->getSpatialNodeType(), NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);

    EXPECT_TRUE(node1->getCoordinates().isValid());
    EXPECT_EQ(node2->getCoordinates(), NES::Spatial::Index::Experimental::Location::fromString(location2));

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
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv");
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
    NES::Spatial::Index::Experimental::Location currentLocation = wrk1Node->getCoordinates();
    Timestamp afterQuery = getTimestamp();

    auto sourceCsv =
        std::static_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV,
                                 NES::Spatial::Mobility::Experimental::LocationProvider>(wrk1->getLocationProvider());
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
            EXPECT_GE(currentLocation.getLatitude(), pos1lat);
            EXPECT_GE(currentLocation.getLongitude(), pos1lng);
            EXPECT_LE(currentLocation.getLatitude(), pos2lat);
            EXPECT_LE(currentLocation.getLongitude(), pos2lng);
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentLocation.getLatitude(), pos2lat);
            EXPECT_GE(currentLocation.getLongitude(), pos2lng);
            EXPECT_LE(currentLocation.getLatitude(), pos3lat);
            EXPECT_LE(currentLocation.getLongitude(), pos3lng);
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentLocation.getLatitude(), pos3lat);
            EXPECT_GE(currentLocation.getLongitude(), pos3lng);
            EXPECT_LE(currentLocation.getLatitude(), pos4lat);
            EXPECT_LE(currentLocation.getLongitude(), pos4lng);
        } else if (beforeQuery > timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ((currentLocation), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
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

TEST_F(LocationIntegrationTests, testGetLocationViaRPC) {

    WorkerRPCClientPtr client = std::make_shared<WorkerRPCClient>();
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;

    //test getting location of mobile node
    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->rpcPort = rpcPortWrk1;
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart1);

    auto loc1 = client->getLocation("127.0.0.1:" + std::to_string(rpcPortWrk1));
    EXPECT_TRUE(loc1.isValid());
    EXPECT_EQ(loc1, NES::Spatial::Index::Experimental::Location(52.55227464714949, 13.351743136322877));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    //test getting location of field node
    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->rpcPort = rpcPortWrk2;
    wrkConf2->locationCoordinates.setValue(NES::Spatial::Index::Experimental::Location::fromString(location2));
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart2);

    auto loc2 = client->getLocation("127.0.0.1:" + std::to_string(rpcPortWrk2));
    EXPECT_TRUE(loc2.isValid());
    EXPECT_EQ(loc2, NES::Spatial::Index::Experimental::Location::fromString(location2));

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
    EXPECT_FALSE(loc3.isValid());

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);

    //test getting location of non existent node
    auto loc4 = client->getLocation("127.0.0.1:9999");
    EXPECT_FALSE(loc4.isValid());
}

TEST_F(LocationIntegrationTests, testReconnecting) {
    NES::Logger::getInstance()->setLogLevel(LogLevel::LOG_DEBUG);
    size_t coverage = 5000;
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    auto locIndex = topology->getLocationIndex();

    TopologyNodePtr node = topology->getRoot();
    std::vector<NES::Spatial::Index::Experimental::Location> locVec = {
        {52.53024925374664, 13.440408001670573},  {52.44959193751221, 12.994693532702838},
        {52.58394737653231, 13.404557656002641},  {52.48534029037908, 12.984138457171484},
        {52.37433823627218, 13.558651957244951},  {52.51533875315059, 13.241771507925069},
        {52.55973107205912, 13.015653271890772},  {52.63119966549814, 13.441159505328082},
        {52.52554704888443, 13.140415389311752},  {52.482596286130494, 13.292443465145574},
        {52.54298642356826, 13.73191525503437},   {52.42678133005856, 13.253118169911525},
        {52.49621174869779, 13.660943763979146},  {52.45590365225229, 13.683553731893118},
        {52.62859441558, 13.135969230535936},     {52.49564618880393, 13.333672868668472},
        {52.58790396655713, 13.283405589901832},  {52.43730546215479, 13.288472865017477},
        {52.452625895558846, 13.609715377620118}, {52.604381034747234, 13.236153100778251},
        {52.52406858008703, 13.202905224067974},  {52.48532771063918, 13.248322218507269},
        {52.50023010173765, 13.35516100143647},   {52.5655774963026, 13.416236069617133},
        {52.56839177666675, 13.311990021109548},  {52.42881523569258, 13.539510531504995},
        {52.55745803205775, 13.521177091034348},  {52.378590211721814, 13.39387224077735},
        {52.45968932886132, 13.466172426273232},  {52.60131778672673, 13.6759151640276},
        {52.59382248148305, 13.17751716953493},   {52.51690603363213, 13.627430091500505},
        {52.40035318355461, 13.386405495784041},  {52.49369404130713, 13.503477002208028},
        {52.52102316662499, 13.231109595273479},  {52.6264057419334, 13.239482930461145},
        {52.45997462557177, 13.038370380285766},  {52.405581430754694, 12.994506535621692},
        {52.5165220102255, 13.287867202522792},   {52.61937748717004, 13.607622490869543},
        {52.620153404197254, 13.236774758123099}, {52.53095039302521, 13.150218024942914},
        {52.60042748492653, 13.591960614892749},  {52.44688258081577, 13.091132219453291},
        {52.44810624782493, 13.189186365976528},  {52.631904019035325, 13.099599387131189},
        {52.51607843891218, 13.361003233097668},  {52.63920358795863, 13.365640690678045},
        {52.51050545031392, 13.687455299147123},  {52.42516226249599, 13.597154340475155},
        {52.585620728658185, 13.177440252255762}, {52.54251642039891, 13.270687079693818},
        {52.62589583837628, 13.58922212327232},   {52.63840628658707, 13.336777486335386},
        {52.382935034604074, 13.54689828854007},  {52.46173261319607, 13.637993027984113},
        {52.45558349451082, 13.774558360650097},  {52.50660545385822, 13.171564805090318},
        {52.38586011054127, 13.772290920473052},  {52.4010561708298, 13.426889487526187}};

    S2PointIndex<uint64_t> nodeIndex;
    size_t idCount = 10000;
    for (auto elem : locVec) {
        TopologyNodePtr currNode = TopologyNode::create(idCount, "127.0.0.1", 1, 0, 0);
        currNode->setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
        currNode->setFixedCoordinates(elem);
        topology->addNewTopologyNodeAsChild(node, currNode);
        locIndex->initializeFieldNodeCoordinates(currNode, (currNode->getCoordinates()));
        nodeIndex.Add(NES::Spatial::Util::S2Utilities::locationToS2Point(currNode->getCoordinates()), currNode->getId());
        idCount++;
    }

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf1->parentId.setValue(10006);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.pathPredictionUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.sendLocationUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "testLocationsSlow2.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    auto waypoints =
        std::dynamic_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(wrk1->getLocationProvider())
            ->getWaypoints();
    auto reconnectSchedule = wrk1->getTrajectoryPredictor()->getReconnectSchedule();
    while (!reconnectSchedule->getLastIndexUpdatePosition()) {
        NES_DEBUG("reconnect schedule does not yet contain index update position")
        reconnectSchedule = wrk1->getTrajectoryPredictor()->getReconnectSchedule();
    }

    size_t waypointCounter = 1;
    std::vector<bool> waypointCovered(waypoints.size(), false);
    S2Polyline lastPredictedPath;
    Timestamp lastPredictedPathRetrievalTime;
    uint64_t parentId = 0;
    Timestamp allowedTimeDiff = 150000000;//0.15 seconds
    std::optional<Timestamp> firstPrediction;
    auto allowedReconnectPositionPredictionError = S2Earth::MetersToAngle(50);
    std::pair<NES::Spatial::Index::Experimental::LocationPtr, Timestamp> lastReconnectPositionAndTime =
        std::pair(std::make_shared<NES::Spatial::Index::Experimental::Location>(), 0);
    std::shared_ptr<NES::Spatial::Mobility::Experimental::ReconnectPoint> predictedReconnect;
    std::shared_ptr<NES::Spatial::Mobility::Experimental::ReconnectPoint> oldPredictedReconnect;
    bool stabilizedSchedule = false;
    int reconnectCounter = 0;
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPrediction> checkVectorForCoordinatorPrediction;
    std::optional<std::tuple<uint64_t, NES::Spatial::Index::Experimental::Location, Timestamp>>
        delayedCoordinatorPredictionsToCheck;
    ReconnectSchedule currentSchedule;
    ReconnectSchedule lastSchedule;

    //keep looping until the final waypoint is reached
    for (auto workerLocation = wrk1->getLocationProvider()->getCurrentLocation();
         *workerLocation.first != *(waypoints.back().first);
         workerLocation = wrk1->getLocationProvider()->getCurrentLocation()) {
        //test local node index
        NES::Spatial::Index::Experimental::LocationPtr indexUpdatePosition =
            wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getLastIndexUpdatePosition();
        if (indexUpdatePosition) {
            S2ClosestPointQuery<uint64_t> query(&nodeIndex);
            query.mutable_options()->set_max_distance(
                S2Earth::MetersToAngle(mobilityConfiguration1->nodeInfoDownloadRadius.getValue()));
            S2ClosestPointQuery<int>::PointTarget target(
                NES::Spatial::Util::S2Utilities::locationToS2Point(*indexUpdatePosition));
            auto closestNodeList = query.FindClosestPoints(&target);
            EXPECT_GT(closestNodeList.size(), 1);
            if (closestNodeList.size(), wrk1->getTrajectoryPredictor()->getSizeOfSpatialIndex()) {
                for (auto result : closestNodeList) {
                    NES::Spatial::Index::Experimental::Location loc;
                    loc = wrk1->getTrajectoryPredictor()->getNodeLocationById(result.data());
                    if (!loc.isValid()) {
                        auto newDownloadPos =
                            wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getLastIndexUpdatePosition();
                        if (newDownloadPos) {
                            NES_DEBUG("new downloaded position is not null, checking if it changed and breaking out of loop")
                            EXPECT_NE(*indexUpdatePosition, *(newDownloadPos));
                        } else {
                            NES_DEBUG("new downloaded node index is null, breaking out of loop")
                        }
                        break;
                    }
                    EXPECT_TRUE(S2::ApproxEquals(NES::Spatial::Util::S2Utilities::locationToS2Point(loc), result.point()));
                }
            } else {
                auto newDownloadPos = wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getLastIndexUpdatePosition();
                EXPECT_NE(*indexUpdatePosition, *(newDownloadPos));
                break;
            }
        }

        //testing path prediction
        //find out which one is the upcoming waypoint
        auto nextWaypoint = waypoints[waypointCounter];
        while (workerLocation.second > nextWaypoint.second) {
            //expecting this to be true works with the current input data
            //for paths where waypoints lead to less sharp turns, we also need to consider the option, that the predicted path did not change after passing a waypoint
            EXPECT_TRUE(waypointCovered[waypointCounter]);
            nextWaypoint = waypoints[++waypointCounter];
            waypointCovered[waypointCounter] = false;
            stabilizedSchedule = false;
        }

        if (!waypointCovered[waypointCounter]) {
            auto pathStart = wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getPathStart();
            auto pathEnd = wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getPathEnd();
            lastPredictedPathRetrievalTime = getTimestamp();
            if (pathStart && pathEnd) {
                if (pathStart->isValid() && pathEnd->isValid()) {
                    auto startPoint = NES::Spatial::Util::S2Utilities::locationToS2Point(*pathStart);
                    auto endPoint = NES::Spatial::Util::S2Utilities::locationToS2Point(*pathEnd);
                    lastPredictedPath = S2Polyline(std::vector({startPoint, endPoint}));
                    auto pathCurrentPosToWayPoint =
                        S2Polyline(std::vector({NES::Spatial::Util::S2Utilities::locationToS2Point(*workerLocation.first),
                                                NES::Spatial::Util::S2Utilities::locationToS2Point(*nextWaypoint.first)}));
                    waypointCovered[waypointCounter] =
                        lastPredictedPath.NearlyCovers(pathCurrentPosToWayPoint, S2Earth::MetersToAngle(1));
                }
            }
        }

        auto pathStartNew = wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getPathStart();
        auto pathEndNew = wrk1->getTrajectoryPredictor()->getReconnectSchedule()->getPathEnd();
        if (pathStartNew && pathEndNew) {
            if (pathStartNew->isValid() && pathEndNew->isValid()) {
                auto startPointNew = NES::Spatial::Util::S2Utilities::locationToS2Point(*pathStartNew);
                auto endPointNew = NES::Spatial::Util::S2Utilities::locationToS2Point(*pathEndNew);
                auto pathNew = S2Polyline(std::vector({startPointNew, endPointNew}));

                //if we once covered the waypoint, we expect the path not to change until the waypoint is reached
                if (waypointCovered[waypointCounter]) {
                    NES_TRACE("upcoming waypoint is covered, checking if path stayed stable")
                    EXPECT_TRUE(lastPredictedPath.Equals(pathNew));
                }

                if (workerLocation.second > lastPredictedPathRetrievalTime
                        + mobilityConfiguration1->pathPredictionUpdateInterval.getValue() * 1000000) {
                    NES_TRACE("update interval passed, check stabilizing and node covering");

                    //if the path prediction stabilizedSchedule, we expect it to cover the next waypoint
                    EXPECT_EQ(lastPredictedPath.Equals(pathNew), waypointCovered[waypointCounter]);

                    if (waypointCovered[waypointCounter]) {
                        auto newPredictedReconnect = wrk1->getTrajectoryPredictor()->getNextPredictedReconnect();
                        auto updatedLastReconnect = wrk1->getTrajectoryPredictor()->getLastReconnectLocationAndTime();
                        auto newSchedule = wrk1->getTrajectoryPredictor()->getReconnectSchedule();
                        //todo: test other functions here
                        //the path covered the waypoint, but the new schedule is not necessarily computed yet, therefore we need to keep querying for the prediction
                        EXPECT_TRUE(lastReconnectPositionAndTime.first);
                        EXPECT_TRUE(updatedLastReconnect.first);
                        if (newPredictedReconnect
                            && ((lastReconnectPositionAndTime.first->isValid()
                                 && *updatedLastReconnect.first == *lastReconnectPositionAndTime.first)
                                || !lastReconnectPositionAndTime.first->isValid())) {
                            if ((/*we are still connected to first node*/ !oldPredictedReconnect
                                 || /*we are reconnected to a new node*/ (
                                     newPredictedReconnect->predictedReconnectLocation
                                         != oldPredictedReconnect->predictedReconnectLocation
                                     &&
                                     //prevent omitting reconnect check
                                     oldPredictedReconnect->reconnectPrediction.expectedNewParentId
                                         == updatedLastReconnect.second))) {
                                NES_TRACE("path stabilized after reconnect")
                                NES_TRACE(
                                    "new predicted parent = " << newPredictedReconnect->reconnectPrediction.expectedNewParentId);
                                predictedReconnect = newPredictedReconnect;
                                firstPrediction = predictedReconnect->reconnectPrediction.expectedTime;
                            }
                            if (predictedReconnect
                                && predictedReconnect->predictedReconnectLocation
                                    == newPredictedReconnect->predictedReconnectLocation
                                && predictedReconnect->reconnectPrediction.expectedTime
                                    != newPredictedReconnect->reconnectPrediction.expectedTime) {
                                NES_DEBUG("updating ETA")
                                predictedReconnect = newPredictedReconnect;
                            }
                        }
                    }
                }
            }
        }

        //testing scheduling of reconnects
        auto updatedLastReconnect = wrk1->getTrajectoryPredictor()->getLastReconnectLocationAndTime();
        EXPECT_TRUE(updatedLastReconnect.first);
        //if there has been a reconnect, check the accuracy of the prediction against the actual reconnect place and time
        if (get<0>(updatedLastReconnect)->isValid()) {
            if (!get<0>(lastReconnectPositionAndTime)->isValid()
                || get<0>(updatedLastReconnect) != get<0>(lastReconnectPositionAndTime)) {
                NES_DEBUG("worker reconnected")
                if (predictedReconnect) {
                    auto predictedPoint =
                        NES::Spatial::Util::S2Utilities::locationToS2Point(predictedReconnect->predictedReconnectLocation);
                    auto actualPoint = NES::Spatial::Util::S2Utilities::locationToS2Point(*(updatedLastReconnect.first));
                    EXPECT_TRUE(S2::ApproxEquals(predictedPoint, actualPoint, allowedReconnectPositionPredictionError));
                    EXPECT_NE(predictedReconnect->reconnectPrediction.expectedTime, 0);
                    EXPECT_NE(get<1>(updatedLastReconnect), 0);
                    NES_DEBUG("timediff " << predictedReconnect->reconnectPrediction.expectedTime
                                  - (long long) get<1>(updatedLastReconnect));
                    NES_DEBUG("expected parent id " << predictedReconnect->reconnectPrediction.expectedNewParentId);
                    EXPECT_LT(abs((long long) predictedReconnect->reconnectPrediction.expectedTime
                                  - (long long) get<1>(updatedLastReconnect)),
                              allowedTimeDiff);
                    EXPECT_LT(abs((long long) firstPrediction.value() - (long long) get<1>(updatedLastReconnect)),
                              allowedTimeDiff);
                    firstPrediction = std::nullopt;
                    reconnectCounter++;

                    //check if the predicted position was already sent to the coordinator before. If not, check if it is present now
                    bool predictedAtCoord = false;
                    for (auto prediction = checkVectorForCoordinatorPrediction.begin();
                         prediction != checkVectorForCoordinatorPrediction.end();
                         ++prediction) {
                        NES_DEBUG("comparing prediction to node with id " << prediction->expectedNewParentId)
                        predictedAtCoord =
                            prediction->expectedNewParentId == predictedReconnect->reconnectPrediction.expectedNewParentId;
                        if (predictedAtCoord) {
                            checkVectorForCoordinatorPrediction.erase(checkVectorForCoordinatorPrediction.begin(), prediction);
                            break;
                        }
                    }
                    if (!predictedAtCoord) {
                        auto currentPredictionAtCoordinator =
                            crd->getTopology()->getLocationIndex()->getScheduledReconnect(wrk1->getWorkerId());
                        EXPECT_EQ(predictedReconnect->reconnectPrediction.expectedNewParentId,
                                  currentPredictionAtCoordinator.value().expectedNewParentId);
                    }
                    predictedReconnect.reset();
                    oldPredictedReconnect = predictedReconnect;
                } else {
                    NES_DEBUG("no prediction!");
                }
                lastReconnectPositionAndTime = updatedLastReconnect;
            }
        }

        //testing record of scheduled reconnects on coordinator side
        auto currentPredictionAtCoordinator = crd->getTopology()->getLocationIndex()->getScheduledReconnect(wrk1->getWorkerId());
        if (currentPredictionAtCoordinator
            && (checkVectorForCoordinatorPrediction.empty()
                || checkVectorForCoordinatorPrediction.back().expectedNewParentId
                   != currentPredictionAtCoordinator.value().expectedNewParentId)) {
            NES_DEBUG("adding new prediction from coordinator")
            checkVectorForCoordinatorPrediction.push_back(currentPredictionAtCoordinator.value());
        }
    }

    //check if we caught all reconnects
    EXPECT_EQ(reconnectCounter, 6);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
}

TEST_F(LocationIntegrationTests, testReconnectingParentOutOfCoverage) {
    NES::Logger::getInstance()->setLogLevel(LogLevel::LOG_DEBUG);
    size_t coverage = 5000;
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    auto locIndex = topology->getLocationIndex();

    TopologyNodePtr node = topology->getRoot();
    std::vector<NES::Spatial::Index::Experimental::Location> locVec = {
        {52.53024925374664, 13.440408001670573},  {52.44959193751221, 12.994693532702838},
        {52.58394737653231, 13.404557656002641},  {52.48534029037908, 12.984138457171484},
        {52.37433823627218, 13.558651957244951},  {52.51533875315059, 13.241771507925069},
        {52.55973107205912, 13.015653271890772},  {52.63119966549814, 13.441159505328082},
        {52.52554704888443, 13.140415389311752},  {52.482596286130494, 13.292443465145574},
        {52.54298642356826, 13.73191525503437},   {52.42678133005856, 13.253118169911525},
        {52.49621174869779, 13.660943763979146},  {52.45590365225229, 13.683553731893118},
        {52.62859441558, 13.135969230535936},     {52.49564618880393, 13.333672868668472},
        {52.58790396655713, 13.283405589901832},  {52.43730546215479, 13.288472865017477},
        {52.452625895558846, 13.609715377620118}, {52.604381034747234, 13.236153100778251},
        {52.52406858008703, 13.202905224067974},  {52.48532771063918, 13.248322218507269},
        {52.50023010173765, 13.35516100143647},   {52.5655774963026, 13.416236069617133},
        {52.56839177666675, 13.311990021109548},  {52.42881523569258, 13.539510531504995},
        {52.55745803205775, 13.521177091034348},  {52.378590211721814, 13.39387224077735},
        {52.45968932886132, 13.466172426273232},  {52.60131778672673, 13.6759151640276},
        {52.59382248148305, 13.17751716953493},   {52.51690603363213, 13.627430091500505},
        {52.40035318355461, 13.386405495784041},  {52.49369404130713, 13.503477002208028},
        {52.52102316662499, 13.231109595273479},  {52.6264057419334, 13.239482930461145},
        {52.45997462557177, 13.038370380285766},  {52.405581430754694, 12.994506535621692},
        {52.5165220102255, 13.287867202522792},   {52.61937748717004, 13.607622490869543},
        {52.620153404197254, 13.236774758123099}, {52.53095039302521, 13.150218024942914},
        {52.60042748492653, 13.591960614892749},  {52.44688258081577, 13.091132219453291},
        {52.44810624782493, 13.189186365976528},  {52.631904019035325, 13.099599387131189},
        {52.51607843891218, 13.361003233097668},  {52.63920358795863, 13.365640690678045},
        {52.51050545031392, 13.687455299147123},  {52.42516226249599, 13.597154340475155},
        {52.585620728658185, 13.177440252255762}, {52.54251642039891, 13.270687079693818},
        {52.62589583837628, 13.58922212327232},   {52.63840628658707, 13.336777486335386},
        {52.382935034604074, 13.54689828854007},  {52.46173261319607, 13.637993027984113},
        {52.45558349451082, 13.774558360650097},  {52.50660545385822, 13.171564805090318},
        {52.38586011054127, 13.772290920473052},  {52.4010561708298, 13.426889487526187}};

    S2PointIndex<uint64_t> nodeIndex;
    size_t idCount = 10000;
    for (auto elem : locVec) {
        TopologyNodePtr currNode = TopologyNode::create(idCount, "127.0.0.1", 1, 0, 0);
        currNode->setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
        currNode->setFixedCoordinates(elem);
        topology->addNewTopologyNodeAsChild(node, currNode);
        locIndex->initializeFieldNodeCoordinates(currNode, (currNode->getCoordinates()));
        nodeIndex.Add(NES::Spatial::Util::S2Utilities::locationToS2Point(currNode->getCoordinates()), currNode->getId());
        idCount++;
    }

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf1->parentId.setValue(10045);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.pathPredictionUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.sendLocationUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "testLocationsSlow2.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);

    auto waypoints =
        std::dynamic_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(wrk1->getLocationProvider())
            ->getWaypoints();
    auto reconnectSchedule = wrk1->getTrajectoryPredictor()->getReconnectSchedule();
    while (!reconnectSchedule->getLastIndexUpdatePosition()) {
        NES_DEBUG("reconnect schedule does not yet contain index update position")
        reconnectSchedule = wrk1->getTrajectoryPredictor()->getReconnectSchedule();
    }

    uint64_t parentId = 0;
    std::vector<uint64_t> reconnectSequence({10045, 10006, 10008, 10051, 10046, 10000, 10033, 10031});
    parentId =
        std::dynamic_pointer_cast<TopologyNode>(topology->findNodeWithId(wrk1->getWorkerId())->getParents().front())->getId();
    while (parentId != reconnectSequence.back()) {
        if (parentId != reconnectSequence.front()) {
            reconnectSequence.erase(reconnectSequence.begin());
            EXPECT_EQ(parentId, reconnectSequence.front());
        }
        parentId =
            std::dynamic_pointer_cast<TopologyNode>(topology->findNodeWithId(wrk1->getWorkerId())->getParents().front())->getId();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);
}

TEST_F(LocationIntegrationTests, DISABLED_testSequenceWithoutBuffering) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_without_buffering_out.csv";

    std::stringstream fileInStream;
    std::ifstream checkFile(std::string(TEST_DATA_DIRECTORY) + std::string("sequence_middle_check.csv"));
    std::string compareString;
    if (checkFile.is_open()) {
        if (checkFile.good()) {
            std::ostringstream oss;
            oss << checkFile.rdbuf();
            compareString = oss.str();
        }
    }
    remove(testFile.c_str());

    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    //todo: maybe remove these 2
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);
    wrkConf1->numWorkerThreads.setValue(4);

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    //stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long_test.csv");
    stype->setNumberOfBuffersToProduce(9999);
    stype->setNumberOfTuplesToProducePerBuffer(1);
    stype->setGatheringInterval(1);
    auto sourceExdra = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sourceExdra);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"seq\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    size_t recv_tuples = 0;
    while (recv_tuples < 5000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: " << recv_tuples)
        sleep(1);
    }

    //wrk1->getNodeEngine()->bufferAllData();

    sleep(1);
    std::ifstream inFile(testFile);
    auto last_recv = std::count(std::istreambuf_iterator<char>(inFile),
                                std::istreambuf_iterator<char>(), '\n');

    for (int i = 0; i < 5; ++i) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv while buffering: " << recv_tuples)
        //EXPECT_EQ(last_recv, recv_tuples);
        sleep(1);
    }
    //wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 10000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: " << recv_tuples)
        sleep(1);
    }

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = compareString;
    //todo: this fails sporadically, why?
    EXPECT_TRUE(TestUtils::checkOrderedOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, DISABLED_testSequenceWithBufferingIsOrdered) {
    //NES::Logger::getInstance()->setLogLevel(LogLevel::LOG_TRACE);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";

    std::stringstream fileInStream;
    std::ifstream checkFile(std::string(TEST_DATA_DIRECTORY) + std::string("sequence_middle_check.csv"));
    std::string compareString;
    if (checkFile.is_open()) {
        if (checkFile.good()) {
            std::ostringstream oss;
            oss << checkFile.rdbuf();
            compareString = oss.str();
        }
    }
    remove(testFile.c_str());

    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    //todo: maybe remove these 2
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(9999);
    stype->setNumberOfTuplesToProducePerBuffer(1);
    stype->setGatheringInterval(1);
    auto sourceExdra = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sourceExdra);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"seq\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    size_t recv_tuples = 0;
    while (recv_tuples < 5000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: " << recv_tuples)
        sleep(1);
    }

    wrk1->getNodeEngine()->bufferAllData();

    sleep(1);
    std::ifstream inFile(testFile);
    auto last_recv = std::count(std::istreambuf_iterator<char>(inFile),
                                std::istreambuf_iterator<char>(), '\n');

    for (int i = 0; i < 5; ++i) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv while buffering: " << recv_tuples)
        EXPECT_EQ(last_recv, recv_tuples);
        sleep(1);
    }
    wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 10000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: " << recv_tuples)
        sleep(1);
    }

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = compareString;
    EXPECT_TRUE(TestUtils::checkOrderedOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testSequenceWithBuffering) {
    //NES::Logger::getInstance()->setLogLevel(LogLevel::LOG_TRACE);
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";

    std::stringstream fileInStream;
    std::ifstream checkFile(std::string(TEST_DATA_DIRECTORY) + std::string("sequence_middle_check.csv"));
    std::string compareString;
    if (checkFile.is_open()) {
        if (checkFile.good()) {
            std::ostringstream oss;
            oss << checkFile.rdbuf();
            compareString = oss.str();
        }
    }
    remove(testFile.c_str());

    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    //todo: maybe remove these 2
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(9999);
    stype->setNumberOfTuplesToProducePerBuffer(1);
    stype->setGatheringInterval(1);
    auto sourceExdra = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sourceExdra);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"seq\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    size_t recv_tuples = 0;
    while (recv_tuples < 5000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
            std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: " << recv_tuples)
        sleep(1);
    }

    wrk1->getNodeEngine()->bufferAllData();

    sleep(1);
    std::ifstream inFile(testFile);
    auto last_recv = std::count(std::istreambuf_iterator<char>(inFile),
                             std::istreambuf_iterator<char>(), '\n');

    for (int i = 0; i < 5; ++i) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv while buffering: " << recv_tuples)
        EXPECT_EQ(last_recv, recv_tuples);
        sleep(1);
    }
    wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 10000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: " << recv_tuples)
        sleep(1);
    }

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = compareString;
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testSequenceWithBufferingMultiThread) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";

    std::stringstream fileInStream;
    std::ifstream checkFile(std::string(TEST_DATA_DIRECTORY) + std::string("sequence_middle_check.csv"));
    std::string compareString;
    if (checkFile.is_open()) {
        if (checkFile.good()) {
            std::ostringstream oss;
            oss << checkFile.rdbuf();
            compareString = oss.str();
        }
    }
    remove(testFile.c_str());

    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    //todo: maybe remove these 2
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->numWorkerThreads.setValue(4);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    //stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long_test.csv");
    stype->setNumberOfBuffersToProduce(9999);
    stype->setNumberOfTuplesToProducePerBuffer(1);
    stype->setGatheringInterval(1);
    auto sourceExdra = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sourceExdra);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"seq\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    size_t recv_tuples = 0;
    while (recv_tuples < 5000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: " << recv_tuples)
        sleep(1);
    }

    wrk1->getNodeEngine()->bufferAllData();

    sleep(1);
    std::ifstream inFile(testFile);
    auto last_recv = std::count(std::istreambuf_iterator<char>(inFile),
                                std::istreambuf_iterator<char>(), '\n');

    for (int i = 0; i < 5; ++i) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv while buffering: " << recv_tuples)
        EXPECT_EQ(last_recv, recv_tuples);
        sleep(1);
    }
    wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 10000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: " << recv_tuples)
        sleep(1);
    }

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = compareString;
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testFlushingEmptyBuffer) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "empty_buffer_out.csv";

    std::stringstream fileInStream;
    std::ifstream checkFile(std::string(TEST_DATA_DIRECTORY) + std::string("sequence_middle_check.csv"));
    std::string compareString;
    if (checkFile.is_open()) {
        if (checkFile.good()) {
            std::ostringstream oss;
            oss << checkFile.rdbuf();
            compareString = oss.str();
        }
    }
    remove(testFile.c_str());

    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    /*
    //todo: maybe remove these 2
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);
     */

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(9999);
    stype->setNumberOfTuplesToProducePerBuffer(1);
    stype->setGatheringInterval(10000);
    stype->setGatheringMode(GatheringMode::INTERVAL_MODE);
    auto sourceExdra = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sourceExdra);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"seq\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    size_t recv_tuples = 0;
    while (recv_tuples < 3) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: " << recv_tuples)
        sleep(1);
    }

    wrk1->getNodeEngine()->bufferAllData();

    std::ifstream inFile(testFile);
    recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                             std::istreambuf_iterator<char>(), '\n');
    NES_DEBUG("recv while buffering: " << recv_tuples)
    sleep(1);

    wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 5) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: " << recv_tuples)
        sleep(1);
    }

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testReconfigWithoutRunningQuery) {
    NES_INFO(" start coordinator");
    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));

    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    //todo: maybe remove these 2
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 1));

    wrk1->getNodeEngine()->bufferAllData();

    wrk1->getNodeEngine()->stopBufferingAllData();

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testSequenceWithReconnecting) {
    NES_INFO(" start coordinator");
    //NES::Logger::getInstance()->setLogLevel(NES::LogLevel::LOG_TRACE);
    std::string testFile = getTestResourceFolder() / "exdra_out.csv";

    std::stringstream fileInStream;
    std::ifstream checkFile(std::string(TEST_DATA_DIRECTORY) + std::string("sequence_middle_check.csv"));
    std::string compareString;
    if (checkFile.is_open()) {
        if (checkFile.good()) {
            std::ostringstream oss;
            oss << checkFile.rdbuf();
            compareString = oss.str();
        }
    }
    remove(testFile.c_str());

    NES_INFO("rest port = " << *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 0));
    auto locIndex = topology->getLocationIndex();

    TopologyNodePtr node = topology->getRoot();
    std::vector<NES::Spatial::Index::Experimental::Location> locVec = {
        {52.53024925374664, 13.440408001670573},
        {52.44959193751221, 12.994693532702838},
        {52.58394737653231, 13.404557656002641},
        {52.48534029037908, 12.984138457171484},
        {52.37433823627218, 13.558651957244951},
        {52.51533875315059, 13.241771507925069},
        {52.55973107205912, 13.015653271890772},
        {52.63119966549814, 13.441159505328082},
        {52.52554704888443, 13.140415389311752},
        {52.482596286130494, 13.292443465145574},
        {52.54298642356826, 13.73191525503437},
        {52.42678133005856, 13.253118169911525},
        {52.49621174869779, 13.660943763979146},
        {52.45590365225229, 13.683553731893118},
        {52.62859441558, 13.135969230535936},
        {52.49564618880393, 13.333672868668472},
        {52.58790396655713, 13.283405589901832},
        {52.43730546215479, 13.288472865017477},
        {52.452625895558846, 13.609715377620118},
        {52.604381034747234, 13.236153100778251},
        {52.52406858008703, 13.202905224067974},
        {52.48532771063918, 13.248322218507269},
        {52.50023010173765, 13.35516100143647},
        {52.5655774963026, 13.416236069617133},
        {52.56839177666675, 13.311990021109548},
        {52.42881523569258, 13.539510531504995},
        {52.55745803205775, 13.521177091034348},
        {52.378590211721814, 13.39387224077735},
        {52.45968932886132, 13.466172426273232},
        {52.60131778672673, 13.6759151640276},
        {52.59382248148305, 13.17751716953493},
        {52.51690603363213, 13.627430091500505},
        {52.40035318355461, 13.386405495784041},
        {52.49369404130713, 13.503477002208028},
        {52.52102316662499, 13.231109595273479},
        {52.6264057419334, 13.239482930461145},
        {52.45997462557177, 13.038370380285766},
        {52.405581430754694, 12.994506535621692},
        {52.5165220102255, 13.287867202522792},
        {52.61937748717004, 13.607622490869543},
        {52.620153404197254, 13.236774758123099},
        {52.53095039302521, 13.150218024942914},
        {52.60042748492653, 13.591960614892749},
        {52.44688258081577, 13.091132219453291},
        {52.44810624782493, 13.189186365976528},
        {52.631904019035325, 13.099599387131189},
        {52.51607843891218, 13.361003233097668},
        {52.63920358795863, 13.365640690678045},
        {52.51050545031392, 13.687455299147123},
        {52.42516226249599, 13.597154340475155},
        {52.585620728658185, 13.177440252255762},
        {52.54251642039891, 13.270687079693818},
        {52.62589583837628, 13.58922212327232},
        {52.63840628658707, 13.336777486335386},
        {52.382935034604074, 13.54689828854007},
        {52.46173261319607, 13.637993027984113},
        {52.45558349451082, 13.774558360650097},
        {52.50660545385822, 13.171564805090318},
        {52.38586011054127, 13.772290920473052},
        {52.4010561708298, 13.426889487526187}
    };

    //size_t idCount = 10000;
    for (auto elem : locVec) {
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue(elem);
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        EXPECT_TRUE(retStart);

        //idCount++;
    }
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 60));
    string singleLocStart = "52.55227464714949, 13.351743136322877";
    auto startParentId =
        topology->getLocationIndex()->getClosestNodeTo(NES::Spatial::Index::Experimental::Location::fromString(singleLocStart)).value()->getId();
    std::stringstream schema;
    schema << "{\"logicalSourceName\" : \"seq\",\"schema\" "
              ":\"Schema::create()->addField(createField(\\\"value\\\",UINT64));\"}";
    schema << endl;
    NES_INFO("schema submit=" << schema.str());
    EXPECT_TRUE(TestUtils::addLogicalSource(schema.str(), std::to_string(*restPort)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    //todo: maybe remove these 2
    /*
    wrkConf1->dataPort.setValue(0);
    wrkConf1->rpcPort.setValue(0);
     */

    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(9999);
    stype->setNumberOfTuplesToProducePerBuffer(1);
    stype->setGatheringInterval(1);
    auto sourceExdra = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sourceExdra);

    //error starts after adding this block
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf1->parentId.setValue(10006);
    wrkConf1->parentId.setValue(startParentId);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.pathPredictionUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.sendLocationUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "testLocationsSlow2.csv");
    //wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    EXPECT_TRUE(TestUtils::waitForWorkers(*restPort, timeout, 61));

    std::stringstream ss;
    ss << "{\"userQuery\" : ";
    ss << R"("Query::from(\"seq\").sink(FileSinkDescriptor::create(\")";
    ss << testFile;
    ss << R"(\", \"CSV_FORMAT\", \"APPEND\")";
    ss << R"());","placement" : "BottomUp"})";
    ss << endl;
    NES_INFO("string submit=" << ss.str());
    string body = ss.str();

    web::json::value json_return = TestUtils::startQueryViaRest(ss.str(), std::to_string(*restPort));

    NES_INFO("try to acc return");
    QueryId queryId = json_return.at("queryId").as_integer();
    NES_INFO("Query ID: " << queryId);
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    //todo: check if buffering really happens, make more finegrained test, to see if buffering really works properly
    //std::system(std::string("/snap/bin/chromium file:///home/x/visualizeNes/mapNodesWithReconnects.html?restPort=" +std::to_string(*restPort)).c_str());
    size_t recv_tuples = 0;
    while (recv_tuples < 10000) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile),
                                 std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("received: " << recv_tuples)
        sleep(1);
    }

    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(queryId, 1, std::to_string(*restPort)));

    string expectedContent = compareString;
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, testFile));

    int response = remove(testFile.c_str());
    EXPECT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES
