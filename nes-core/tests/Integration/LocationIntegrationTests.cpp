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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Services/QueryService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProvider.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Spatial/Mobility/WorkerMobilityHandler.hpp>
#include <Topology/Topology.hpp>
#include <Util/Experimental/S2Utilities.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>
#include <gtest/gtest.h>
#include <iostream>

using std::map;
using std::string;
uint16_t timeout = 5;
namespace NES::Spatial {

class LocationIntegrationTests : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationIntegrationTests.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationIntegrationTests test class.");

        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints;
        waypoints.push_back({{52.55227464714949, 13.351743136322877}, 0});
        waypoints.push_back({{2.574709862890394, 13.419206057808077}, 1000000000});
        waypoints.push_back({{2.61756571840606, 13.505980882863446}, 2000000000});
        waypoints.push_back({{2.67219559419452, 13.591124924963108}, 3000000000});
        auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
        remove(csvPath.c_str());
        writeWaypointsToCsv(csvPath, waypoints);

        std::string singleLocationPath = std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv";
        remove(singleLocationPath.c_str());
        writeWaypointsToCsv(singleLocationPath, {{{52.55227464714949, 13.351743136322877}, 0}});

#ifdef S2DEF
        auto interpolatedCsv = std::string(TEST_DATA_DIRECTORY) + "path1.csv";
        remove(interpolatedCsv.c_str());
        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypointsToInterpolate;
        waypointsToInterpolate.push_back({{52.58210307572243, 12.987507417206261}, 0});
        waypointsToInterpolate.push_back({{52.5225665088927, 13.198478869225813}, 1000000000});
        waypointsToInterpolate.push_back({{52.5824815034542, 13.280594641984383}, 2000000000});
        waypointsToInterpolate.push_back({{52.5251960754162, 13.400310793986574}, 3000000000});
        waypointsToInterpolate.push_back({{52.51309876750171, 13.57837236374691}, 5000000000});
        auto interpolatedPath = interpolatePath(waypointsToInterpolate, 1000);
        writeWaypointsToCsv(interpolatedCsv, interpolatedPath);
#endif

        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        std::ofstream inputSequenceStream(inputSequence);
        for (int i = 1; i < 100000; ++i) {
            inputSequenceStream << std::to_string(i) << std::endl;
        }
        inputSequenceStream.close();
        ASSERT_FALSE(inputSequenceStream.fail());
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    std::chrono::duration<int64_t, std::milli> defaultTimeoutInSec = std::chrono::seconds(TestUtils::defaultTimeout);

    struct getGeolocationParameters {
        getGeolocationParameters(TopologyNodePtr node, TopologyManagerServicePtr service) : node(node), service(service) {}
        TopologyNodePtr node;
        TopologyManagerServicePtr service;
    };

    //wrapper function so allow the util function to call the member function of LocationProvider
    static std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
    getLocationFromTopologyNode(const std::shared_ptr<void>& structParams) {
        auto casted = std::static_pointer_cast<getGeolocationParameters>(structParams);
        return casted->service->getGeoLocationForNode(casted->node->getId());
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

#ifdef S2DEF
    /**
     * @brief check if two location objects latitudes and longitudes do not differ more than the specified error
     * @param location1
     * @param location2
     * @param error the tolerated difference in latitute or longitude specified in degrees
     * @return true if location1 and location2 do not differ more then the specified degrees in latitiude of logintude
     */
    static bool isClose(NES::Spatial::DataTypes::Experimental::GeoLocation location1,
                        NES::Spatial::DataTypes::Experimental::GeoLocation location2,
                        double error) {
        if (std::abs(location1.getLatitude() - location2.getLatitude()) > error) {
            return false;
        }
        if (std::abs(location1.getLongitude() - location2.getLongitude()) > error) {
            return false;
        }
        return true;
    }

    static std::vector<NES::Spatial::DataTypes::Experimental::Waypoint>
    interpolatePath(std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints, Timestamp amount) {
        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> interpolatedPath;
        Timestamp step = waypoints.back().getTimestamp().value() / amount;
        for (auto waypointsItr = waypoints.cbegin(); waypointsItr != waypoints.cend(); ++waypointsItr) {
            auto next = waypointsItr + 1;
            if (next == waypoints.cend()) {
                interpolatedPath.emplace_back(*waypointsItr);
                break;
            }
            auto prevLocation = waypointsItr->getLocation();
            auto prevTime = waypointsItr->getTimestamp().value();
            auto nextLocation = next->getLocation();
            auto nextTime = next->getTimestamp().value();
            Timestamp timerange = nextTime - prevTime;
            S2Point prevPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(prevLocation);
            S2Point nextPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(nextLocation);
            S2Polyline path({prevPoint, nextPoint});
            for (Timestamp i = 0; i < timerange; i += step) {
                double frac = (double) i / (double) timerange;
                S2LatLng interpolated(path.Interpolate(frac));
                interpolatedPath.emplace_back(NES::Spatial::DataTypes::Experimental::GeoLocation(interpolated.lat().degrees(),
                                                                                                 interpolated.lng().degrees()),
                                              prevTime + i);
            }
        }
        return interpolatedPath;
    }
#endif

    static void TearDownTestCase() {
        NES_INFO("Tear down LocationIntegrationTests class.");
        std::string singleLocationPath = std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv";
        remove(singleLocationPath.c_str());
        std::string testLocationsPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
        remove(testLocationsPath.c_str());
        auto interpolatedCsv = std::string(TEST_DATA_DIRECTORY) + "path1.csv";
        remove(interpolatedCsv.c_str());
        auto inputSequence = std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv";
        remove(inputSequence.c_str());
    }
};

TEST_F(LocationIntegrationTests, testFieldNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->coordinatorPort = (port);
    wrkConf3->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location3));
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart3);

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->coordinatorPort = (port);
    wrkConf4->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location4));
    wrkConf4->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart4);

    NES_INFO("worker1 started successfully");
    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    NES_INFO("worker 1 started connected ");

    TopologyPtr topology = crd->getTopology();
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    NES_INFO("worker 2 started connected ");

    bool retConWrk3 = wrk3->connect();
    ASSERT_TRUE(retConWrk3);
    NES_INFO("worker 3 started connected ");

    bool retConWrk4 = wrk4->connect();
    ASSERT_TRUE(retConWrk4);
    NES_INFO("worker 4 started connected ");

    ASSERT_TRUE(waitForNodes(5, 5, topology));

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());
    TopologyNodePtr node3 = topology->findNodeWithId(wrk3->getWorkerId());
    TopologyNodePtr node4 = topology->findNodeWithId(wrk4->getWorkerId());

    //checking coordinates
    ASSERT_EQ(topologyManagerService->getGeoLocationForNode(node2->getId()),
              NES::Spatial::DataTypes::Experimental::GeoLocation(52.53736960143897, 13.299134894776092));
    topologyManagerService->updateGeoLocation(
        node2->getId(),
        NES::Spatial::DataTypes::Experimental::GeoLocation(52.51094383152051, 13.463078966025266));
    ASSERT_EQ(topologyManagerService->getGeoLocationForNode(node2->getId()),
              NES::Spatial::DataTypes::Experimental::GeoLocation(52.51094383152051, 13.463078966025266));

#ifdef S2DEF
    NES_INFO("NEIGHBORS");
    auto inRange = topologyManagerService->getNodesIdsInRange(
        NES::Spatial::DataTypes::Experimental::GeoLocation(52.53736960143897, 13.299134894776092),
        50.0);
    ASSERT_EQ(inRange.size(), (size_t) 3);
#endif
    topologyManagerService->updateGeoLocation(
        node3->getId(),
        NES::Spatial::DataTypes::Experimental::GeoLocation(53.559524264262194, 10.039384739854102));

    topology->removePhysicalNode(topology->findNodeWithId(wrk3->getWorkerId()));

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

TEST_F(LocationIntegrationTests, testMobileNodes) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);

    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    wrkConf2->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);

    NES_INFO("worker1 started successfully");
    bool retConWrk1 = wrk1->connect();
    ASSERT_TRUE(retConWrk1);
    NES_INFO("worker 1 started connected ");

    TopologyPtr topology = crd->getTopology();
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    bool retConWrk2 = wrk2->connect();
    ASSERT_TRUE(retConWrk2);
    NES_INFO("worker 2 started connected ");

    ASSERT_TRUE(waitForNodes(5, 3, topology));

    ASSERT_EQ(wrk1->getLocationProvider()->getSpatialType(), NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    ASSERT_EQ(wrk2->getLocationProvider()->getSpatialType(), NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);

    ASSERT_EQ(wrk2->getLocationProvider()->getCurrentWaypoint().getLocation(),
              NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));

    TopologyNodePtr node1 = topology->findNodeWithId(wrk1->getWorkerId());
    TopologyNodePtr node2 = topology->findNodeWithId(wrk2->getWorkerId());

    ASSERT_EQ(node1->getSpatialNodeType(), NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    ASSERT_EQ(node2->getSpatialNodeType(), NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);

    auto node1Location = topologyManagerService->getGeoLocationForNode(node1->getId());
    ASSERT_TRUE(node1Location.has_value());
    ASSERT_TRUE(node1Location.value().isValid());
    auto node2Location = topologyManagerService->getGeoLocationForNode(node2->getId());
    ASSERT_EQ(node2Location, NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);
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
    ASSERT_EQ(workerConfigPtr->locationCoordinates.getValue(), NES::Spatial::DataTypes::Experimental::GeoLocation(23.88, -3.4));
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

    ASSERT_THROW(workerConfigPtr->overwriteConfigWithCommandLineInput(commandLineParams),
                 NES::Spatial::Exception::CoordinatesOutOfRangeException);
}

TEST_F(LocationIntegrationTests, testMovingDevice) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (*rpcCoordinatorPort);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(csvPath);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    auto sourceCsv =
        std::static_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV,
                                 NES::Spatial::Mobility::Experimental::LocationProvider>(wrk1->getLocationProvider());
    auto startTime = sourceCsv->getStartTime();
    TopologyPtr topology = crd->getTopology();
    auto wrk1id = wrk1->getWorkerId();
    ASSERT_TRUE(waitForNodes(5, 2, topology));
    TopologyNodePtr wrk1Node = topology->findNodeWithId(wrk1->getWorkerId());
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, startTime);

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) * 3;

    bool receivedLocation = false;
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentGeoLocation = topologyManagerService->getGeoLocationForNode(wrk1id);

        //after we receivd a location from the location index, there should always be one present for all remaining iterations
        if (receivedLocation) {
            EXPECT_TRUE(currentGeoLocation.has_value());
        }
        if (currentGeoLocation.has_value()) {
            receivedLocation = true;
            if (actualWayPoints.empty() || currentGeoLocation.value() != actualWayPoints.back().getLocation()) {
                actualWayPoints.emplace_back(currentGeoLocation.value());
            }
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedIt != expectedWayPoints.cend() && expectedIt->getLocation() != actualIt->getLocation()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedIt, expectedWayPoints.cend());
    }

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);
}

TEST_F(LocationIntegrationTests, testMovementAfterStandStill) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(csvPath);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    auto locationProvider =
        std::static_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV,
                                 NES::Spatial::Mobility::Experimental::LocationProvider>(wrk1->getLocationProvider());
    auto startTime = locationProvider->getStartTime();
    TopologyPtr topology = crd->getTopology();
    auto wrk1id = wrk1->getWorkerId();
    ASSERT_TRUE(waitForNodes(5, 2, topology));
    TopologyNodePtr wrk1Node = topology->findNodeWithId(wrk1->getWorkerId());
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, startTime);

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) * 3;

    bool receivedLocation = false;
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentGeoLocation = topologyManagerService->getGeoLocationForNode(wrk1id);

        //after we receivd a location from the location index, there should always be one present for all remaining iterations
        if (receivedLocation) {
            ASSERT_TRUE(currentGeoLocation.has_value());
        }
        if (currentGeoLocation.has_value()) {
            receivedLocation = true;
            if (actualWayPoints.empty() || currentGeoLocation.value() != actualWayPoints.back().getLocation()) {
                actualWayPoints.emplace_back(currentGeoLocation.value());
            }
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedIt != expectedWayPoints.cend() && expectedIt->getLocation() != actualIt->getLocation()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        ASSERT_NE(expectedIt, expectedWayPoints.cend());
    }

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);
}

TEST_F(LocationIntegrationTests, testMovingDeviceSimulatedStartTimeInFuture) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    //we set a location which should get ignored, because we make this node mobile. so it should not show up as a field node
    wrkConf1->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(csvPath);
    Timestamp offset = 400000000;
    auto currTime = getTimestamp();
    Timestamp simulatedStartTime = currTime + offset;
    wrkConf1->mobilityConfiguration.locationProviderSimulatedStartTime.setValue(simulatedStartTime);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    auto locationProvider =
        std::static_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV,
                                 NES::Spatial::Mobility::Experimental::LocationProvider>(wrk1->getLocationProvider());
    auto startTime = locationProvider->getStartTime();
    TopologyPtr topology = crd->getTopology();
    auto wrk1id = wrk1->getWorkerId();
    ASSERT_TRUE(waitForNodes(5, 2, topology));
    TopologyNodePtr wrk1Node = topology->findNodeWithId(wrk1->getWorkerId());
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, startTime);

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) * 3;

    bool receivedLocation = false;
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentGeoLocation = topologyManagerService->getGeoLocationForNode(wrk1id);

        //after we receivd a location from the location index, there should always be one present for all remaining iterations
        if (receivedLocation) {
            ASSERT_TRUE(currentGeoLocation.has_value());
        }
        if (currentGeoLocation.has_value()) {
            receivedLocation = true;
            if (actualWayPoints.empty() || currentGeoLocation.value() != actualWayPoints.back().getLocation()) {
                actualWayPoints.emplace_back(currentGeoLocation.value());
            }
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedIt != expectedWayPoints.cend() && expectedIt->getLocation() != actualIt->getLocation()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedIt, expectedWayPoints.cend());
    }

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);
}

TEST_F(LocationIntegrationTests, testMovingDeviceSimulatedStartTimeInPast) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(csvPath);
    Timestamp offset = -100000000;
    auto currTime = getTimestamp();
    Timestamp simulatedStartTime = currTime + offset;
    wrkConf1->mobilityConfiguration.locationProviderSimulatedStartTime.setValue(simulatedStartTime);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    auto locationProvider =
        std::static_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV,
                                 NES::Spatial::Mobility::Experimental::LocationProvider>(wrk1->getLocationProvider());
    auto startTime = locationProvider->getStartTime();
    TopologyPtr topology = crd->getTopology();
    auto wrk1id = wrk1->getWorkerId();
    ASSERT_TRUE(waitForNodes(5, 2, topology));
    TopologyNodePtr wrk1Node = topology->findNodeWithId(wrk1->getWorkerId());
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();
    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, startTime);

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) * 3;

    bool receivedLocation = false;
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentGeoLocation = topologyManagerService->getGeoLocationForNode(wrk1id);

        //after we receivd a location from the location index, there should always be one present for all remaining iterations
        if (receivedLocation) {
            ASSERT_TRUE(currentGeoLocation.has_value());
        }
        if (currentGeoLocation.has_value()) {
            receivedLocation = true;
            if (actualWayPoints.empty() || currentGeoLocation.value() != actualWayPoints.back().getLocation()) {
                actualWayPoints.emplace_back(currentGeoLocation.value());
            }
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedIt != expectedWayPoints.cend() && expectedIt->getLocation() != actualIt->getLocation()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedIt, expectedWayPoints.cend());
    }

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);
}

TEST_F(LocationIntegrationTests, testGetLocationViaRPC) {

    WorkerRPCClientPtr client = WorkerRPCClient::create();
    auto rpcPortWrk1 = getAvailablePort();
    auto rpcPortWrk2 = getAvailablePort();
    auto rpcPortWrk3 = getAvailablePort();

    //test getting location of mobile node
    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->rpcPort = *rpcPortWrk1;
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart1);

    auto loc1 = client->getWaypoint("127.0.0.1:" + std::to_string(*rpcPortWrk1));
    ASSERT_TRUE(loc1.getLocation().isValid());
    auto singleLoc = getWaypointsFromCsv(wrkConf1->mobilityConfiguration.locationProviderConfig, 0).front().getLocation();
    ASSERT_EQ(loc1.getLocation(), singleLoc);

    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);

    //test getting location of field node
    NES_INFO("start worker 2");
    WorkerConfigurationPtr wrkConf2 = WorkerConfiguration::create();
    wrkConf2->rpcPort = *rpcPortWrk2;
    wrkConf2->locationCoordinates.setValue(NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));
    wrkConf2->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart2);

    auto loc2 = client->getWaypoint("127.0.0.1:" + std::to_string(*rpcPortWrk2));
    ASSERT_TRUE(loc2.getLocation().isValid());
    ASSERT_EQ(loc2.getLocation(), NES::Spatial::DataTypes::Experimental::GeoLocation::fromString(location2));

    bool retStopWrk2 = wrk2->stop(false);
    ASSERT_TRUE(retStopWrk2);

    //test getting location of node which does not have a location
    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = *rpcPortWrk3;
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart3);

    auto loc3 = client->getWaypoint("127.0.0.1:" + std::to_string(*rpcPortWrk3));
    ASSERT_FALSE(loc3.getLocation().isValid());

    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);

    //test getting location of non existent node
    auto loc4 = client->getWaypoint("127.0.0.1:9999");
    ASSERT_FALSE(loc4.getLocation().isValid());
}

#ifdef S2DEF
TEST_F(LocationIntegrationTests, testReconnectingParentOutOfCoverage) {
    size_t coverage = 5000;
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    TopologyNodePtr node = topology->getRoot();

    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> locVec;
    //fixed location node on path
    locVec.emplace_back(52.57828047889124, 12.988243103027346);
    locVec.emplace_back(52.53968787768964, 13.109092712402346);
    locVec.emplace_back(52.544487724835534, 13.207969665527346);
    locVec.emplace_back(52.55930133381748, 13.3154296875);
    locVec.emplace_back(52.5221535423678, 13.411216735839846);
    locVec.emplace_back(52.50523880235127, 13.540649414062502);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;

    S2PointIndex<uint64_t> nodeIndex;
    size_t idCount = 10000;
    for (auto elem : locVec) {
        TopologyNodePtr currNode = TopologyNode::create(idCount, "127.0.0.1", 1, 0, 0, properties);
        topology->addNewTopologyNodeAsChild(node, currNode);
        topologyManagerService->addGeoLocation(currNode->getId(), NES::Spatial::DataTypes::Experimental::GeoLocation(elem));
        nodeIndex.Add(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(elem), currNode->getId());
        idCount++;
    }

    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> offPathVec(
        {{52.5357946437949, 13.025665283203127},   {52.50656427051772, 13.168487548828127},
         {52.627534956324375, 13.303070068359377}, {52.60836876528842, 13.355255126953125},
         {52.52744509312945, 13.341522216796877},  {52.49988031135493, 13.432159423828125},
         {52.485673523569304, 13.270111083984377}, {52.61670292287682, 13.209686279296875},
         {52.61086917912814, 13.08746337890625},   {52.590028034393036, 13.452758789062502},
         {52.56667417928053, 13.559875488281252},  {52.44888169881338, 13.493957519531252},
         {52.42712661816063, 13.345642089843752},  {52.43047425278167, 13.189086914062502},
         {52.46393661692312, 13.345642089843752},  {52.462264102421585, 13.073730468750002},
         {52.46560906788363, 12.970733642578125},  {52.51742353671031, 12.886962890625002},
         {52.654187004059686, 12.991333007812502}, {52.66167996673036, 13.139648437500002},
         {52.68831121111206, 13.24127197265625},   {52.66667456166765, 13.374481201171877},
         {52.65002369203887, 13.465118408203127},  {52.64835825622956, 13.568115234375002},
         {52.40284866297361, 13.454132080078127},  {52.3927987027365, 13.253631591796877},
         {52.533289945284096, 13.627166748046877}, {52.45390057676374, 13.592834472656252}});

    idCount = 20001;
    for (auto elem : offPathVec) {
        TopologyNodePtr currNode = TopologyNode::create(idCount, "127.0.0.1", 1, 0, 0, properties);
        topology->addNewTopologyNodeAsChild(node, currNode);
        topologyManagerService->addGeoLocation(currNode->getId(), NES::Spatial::DataTypes::Experimental::GeoLocation(elem));
        nodeIndex.Add(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(elem), currNode->getId());
        idCount++;
    }

    uint64_t initialParentId = 20000;
    auto outOfCoverageLocation = NES::Spatial::DataTypes::Experimental::GeoLocation(52.7091055763087, 13.128662109375002);
    TopologyNodePtr currNode = TopologyNode::create(initialParentId, "127.0.0.1", 1, 0, 0, properties);
    topology->addNewTopologyNodeAsChild(node, currNode);
    topologyManagerService->addGeoLocation(currNode->getId(),
                                           NES::Spatial::DataTypes::Experimental::GeoLocation(outOfCoverageLocation));
    nodeIndex.Add(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(outOfCoverageLocation), currNode->getId());

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->parentId.setValue(initialParentId);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "path1.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, locVec.size() + offPathVec.size() + 3, topology));

    uint64_t parentId = 0;
    std::vector<uint64_t> reconnectSequence({initialParentId, 10000, 10001, 10002, 10003, 10004, 10005});
    parentId =
        std::dynamic_pointer_cast<TopologyNode>(topology->findNodeWithId(wrk1->getWorkerId())->getParents().front())->getId();
    uint64_t oldParentId = 0;
    std::vector<uint64_t> actualSequence;
    auto startTimestamp = std::chrono::system_clock::now();
    while (reconnectSequence.size() > 1 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        if (parentId != reconnectSequence.front()) {
            reconnectSequence.erase(reconnectSequence.begin());
        }
        parentId =
            std::dynamic_pointer_cast<TopologyNode>(topology->findNodeWithId(wrk1->getWorkerId())->getParents().front())->getId();
        NES_DEBUG("parent id = {}", parentId);
        if (parentId != oldParentId) {
            actualSequence.push_back(parentId);
            oldParentId = parentId;
        }
        for (auto id : actualSequence) {
            NES_DEBUG("{}", id);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    EXPECT_EQ(parentId, 10005);
    ASSERT_EQ(reconnectSequence.size(), 1);

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);
}

// even if no parent was specified in the worker configuration, the worker should reconnect to the closest parent on startup
TEST_F(LocationIntegrationTests, testConnectingToClosestNodeNoParentInConfig) {
    size_t coverage = 5000;
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    std::shared_ptr<TopologyManagerService> topologyManagerService = crd->getTopologyManagerService();

    TopologyNodePtr node = topology->getRoot();

    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> locVec;
    //fixed location node on path
    locVec.emplace_back(52.57828047889124, 12.988243103027346);
    locVec.emplace_back(52.53968787768964, 13.109092712402346);
    locVec.emplace_back(52.544487724835534, 13.207969665527346);
    locVec.emplace_back(52.55930133381748, 13.3154296875);
    locVec.emplace_back(52.5221535423678, 13.411216735839846);
    locVec.emplace_back(52.50523880235127, 13.540649414062502);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;

    S2PointIndex<uint64_t> nodeIndex;
    size_t idCount = 10000;
    for (auto elem : locVec) {
        TopologyNodePtr currNode = TopologyNode::create(idCount, "127.0.0.1", 1, 0, 0, properties);
        topology->addNewTopologyNodeAsChild(node, currNode);
        topologyManagerService->addGeoLocation(currNode->getId(), NES::Spatial::DataTypes::Experimental::GeoLocation(elem));
        nodeIndex.Add(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(elem), currNode->getId());
        idCount++;
    }

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfiguration1 =
        Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfiguration::create();
    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "path1.csv");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, locVec.size() + 2, topology));

    uint64_t parentId = 0;
    std::vector<uint64_t> reconnectSequence({10000, 10001, 10002, 10003, 10004, 10005});
    parentId =
        std::dynamic_pointer_cast<TopologyNode>(topology->findNodeWithId(wrk1->getWorkerId())->getParents().front())->getId();

    //wait until the node has changed its parent from the coordinator node (id 1) to a fixed location node in range
    auto startTimestamp = std::chrono::system_clock::now();
    while (parentId == 1 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        parentId =
            std::dynamic_pointer_cast<TopologyNode>(topology->findNodeWithId(wrk1->getWorkerId())->getParents().front())->getId();
        NES_DEBUG("parent id = {}", parentId);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    //check that the worker is connected to one of the nodes on the path
    EXPECT_NE(std::find(reconnectSequence.begin(), reconnectSequence.end(), parentId), reconnectSequence.end());

    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
    bool retStopWrk1 = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk1);
}
#endif

TEST_F(LocationIntegrationTests, testSequenceWithBuffering) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";
    remove(testFile.c_str());

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (int i = 1; i <= 10000; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);

    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully");

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(10);
    stype->setGatheringInterval(1);
    auto sequenceSource = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sequenceSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        "BottomUp",
        FaultToleranceType::NONE,
        LineageType::NONE);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    auto startTimestamp = std::chrono::system_clock::now();
    size_t recv_tuples = 0;
    while (recv_tuples < 5000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: {}", recv_tuples)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    wrk1->getNodeEngine()->bufferAllData();

    sleep(1);
    std::ifstream inFile(testFile);
    auto last_recv = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');

    for (int i = 0; i < 5; ++i) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv while buffering: {}", recv_tuples)
        ASSERT_EQ(last_recv, recv_tuples);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 10000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: {}", recv_tuples)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(recv_tuples, 10001);
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testSequenceWithBufferingMultiThread) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_buffering_out.csv";

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (int i = 1; i <= 10000; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    wrkConf1->numWorkerThreads.setValue(4);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(10);
    stype->setGatheringInterval(1);
    auto sequenceSource = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sequenceSource);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        "BottomUp",
        FaultToleranceType::NONE,
        LineageType::NONE);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);

    size_t recv_tuples = 0;
    auto startTimestamp = std::chrono::system_clock::now();
    while (recv_tuples < 5000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv before buffering: {}", recv_tuples)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    wrk1->getNodeEngine()->bufferAllData();

    sleep(1);
    std::ifstream inFile(testFile);
    auto last_recv = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');

    for (int i = 0; i < 5; ++i) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv while buffering: {}", recv_tuples)
        ASSERT_EQ(last_recv, recv_tuples);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    wrk1->getNodeEngine()->stopBufferingAllData();

    while (recv_tuples < 10000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("recv after buffering: {}", recv_tuples)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_EQ(recv_tuples, 10001);
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

TEST_F(LocationIntegrationTests, testReconfigWithoutRunningQuery) {
    NES_INFO(" start coordinator");
    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    crd->getSourceCatalog()->addLogicalSource("seq", Schema::create()->addField(createField("value", BasicType::UINT64)));

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 2, topology));

    wrk1->getNodeEngine()->bufferAllData();

    wrk1->getNodeEngine()->stopBufferingAllData();

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}

#ifdef S2DEF
TEST_F(LocationIntegrationTests, testSequenceWithReconnecting) {
    NES_INFO(" start coordinator");
    std::string testFile = getTestResourceFolder() / "sequence_with_reconnecting_out.csv";

    std::string compareString;
    std::ostringstream oss;
    oss << "seq$value:INTEGER(64 bits)" << std::endl;
    for (int i = 1; i <= 10000; ++i) {
        oss << std::to_string(i) << std::endl;
        compareString = oss.str();
    }

    NES_INFO("rest port = {}", *restPort);

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort.setValue(*rpcCoordinatorPort);
    coordinatorConfig->restPort.setValue(*restPort);
    NES_INFO("start coordinator")
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    ASSERT_NE(port, 0UL);
    NES_INFO("coordinator started successfully")

    TopologyPtr topology = crd->getTopology();
    ASSERT_TRUE(waitForNodes(5, 1, topology));

    TopologyNodePtr node = topology->getRoot();
    std::vector<NES::Spatial::DataTypes::Experimental::GeoLocation> locVec = {
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

    std::vector<NesWorkerPtr> fieldNodes;
    for (auto elem : locVec) {
        WorkerConfigurationPtr wrkConf = WorkerConfiguration::create();
        wrkConf->coordinatorPort.setValue(*rpcCoordinatorPort);
        wrkConf->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::FIXED_LOCATION);
        wrkConf->locationCoordinates.setValue(elem);
        NesWorkerPtr wrk = std::make_shared<NesWorker>(std::move(wrkConf));
        fieldNodes.push_back(wrk);
        bool retStart = wrk->start(/**blocking**/ false, /**withConnect**/ true);
        ASSERT_TRUE(retStart);
    }
    ASSERT_TRUE(waitForNodes(5, 61, topology));
    string singleLocStart = "52.55227464714949, 13.351743136322877";
    crd->getSourceCatalog()->addLogicalSource("seq", "Schema::create()->addField(createField(\"value\", BasicType::UINT64));");

    NES_INFO("start worker 1");
    WorkerConfigurationPtr wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort.setValue(*rpcCoordinatorPort);

    auto stype = CSVSourceType::create();
    stype->setFilePath(std::string(TEST_DATA_DIRECTORY) + "sequence_long.csv");
    stype->setNumberOfBuffersToProduce(1000);
    stype->setNumberOfTuplesToProducePerBuffer(10);
    stype->setGatheringInterval(1);
    auto sequenceSource = PhysicalSource::create("seq", "test_stream", stype);
    wrkConf1->physicalSources.add(sequenceSource);

    wrkConf1->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf1->mobilityConfiguration.nodeInfoDownloadRadius.setValue(20000);
    wrkConf1->mobilityConfiguration.nodeIndexUpdateThreshold.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(10);
    wrkConf1->mobilityConfiguration.locationBufferSaveRate.setValue(1);
    wrkConf1->mobilityConfiguration.pathPredictionLength.setValue(40000);
    wrkConf1->mobilityConfiguration.defaultCoverageRadius.setValue(5000);
    wrkConf1->mobilityConfiguration.mobilityHandlerUpdateInterval.setValue(1000);
    wrkConf1->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf1->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "path1.csv");

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    ASSERT_TRUE(retStart1);
    ASSERT_TRUE(waitForNodes(5, 62, topology));

    QueryId queryId = crd->getQueryService()->validateAndQueueAddQueryRequest(
        R"(Query::from("seq").sink(FileSinkDescriptor::create(")" + testFile + R"(", "CSV_FORMAT", "APPEND"));)",
        "BottomUp",
        FaultToleranceType::NONE,
        LineageType::NONE);

    NES_INFO("Query ID: {}", queryId);
    ASSERT_NE(queryId, INVALID_QUERY_ID);
    size_t recv_tuples = 0;
    auto startTimestamp = std::chrono::system_clock::now();
    while (recv_tuples < 10000 && std::chrono::system_clock::now() < startTimestamp + defaultTimeoutInSec) {
        std::ifstream inFile(testFile);
        recv_tuples = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
        NES_DEBUG("received: {}", recv_tuples)
        sleep(1);
    }

    ASSERT_EQ(recv_tuples, 10001);
    ASSERT_TRUE(TestUtils::checkOutputOrTimeout(compareString, testFile));

    int response = remove(testFile.c_str());
    ASSERT_TRUE(response == 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk1->stop(false);
    ASSERT_TRUE(retStopWrk);

    for (const auto& w : fieldNodes) {
        bool stop = w->stop(false);
        ASSERT_TRUE(stop);
    }

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    ASSERT_TRUE(retStopCord);
}
#endif
}// namespace NES::Spatial
