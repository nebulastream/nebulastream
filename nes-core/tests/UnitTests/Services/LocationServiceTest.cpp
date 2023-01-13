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

#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <NesBaseTest.hpp>
#include <Services/LocationService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#ifdef S2DEF
namespace NES {

std::string ip = "127.0.0.1";

class LocationServiceTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Set up LocationServiceTest test class.")
    }
    static void TearDownTestCase(){NES_INFO("Tear down LocationServiceTest test class")}

    nlohmann::json convertNodeLocationInfoToJson(uint64_t id, NES::Spatial::DataTypes::Experimental::GeoLocation loc) {
        nlohmann::json nodeInfo;
        nodeInfo["id"] = id;
        nlohmann::json::array_t locJson;
        if (loc.isValid()) {
            locJson.push_back(loc.getLatitude());
            locJson.push_back((loc.getLongitude()));
        }

        nodeInfo["location"] = locJson;
        NES_DEBUG(nodeInfo.dump());
        return nodeInfo;
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    NES::LocationServicePtr locationService;
    NES::TopologyManagerServicePtr topologyManagerService;
};

TEST_F(LocationServiceTest, testRequestSingleNodeLocation) {
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;

    nlohmann::json cmpJson;
    nlohmann::json cmpLoc;
    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node1Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk1, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;
    auto node2Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk2, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk3, 0, 0, properties);

    topologyManagerService->updateGeoLocation(node2Id, {13.4, -23});

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

//todo #3390: should this have an effect for a mobile node?
    topologyManagerService->updateGeoLocation(node3Id, {13.4, -23});

    // test querying for node which does not exist in the system
    EXPECT_EQ(locationService->requestNodeLocationDataAsJson(1234), nullptr);

    //test getting location of node which does not have a location
    cmpJson = locationService->requestNodeLocationDataAsJson(1);
    EXPECT_EQ(cmpJson["id"], node1Id);
    EXPECT_TRUE(cmpJson["location"].empty());

    //test getting location of field node
    cmpJson = locationService->requestNodeLocationDataAsJson(2);
    auto entry = cmpJson.get<std::map<std::string, nlohmann::json>>();
    EXPECT_EQ(entry.at("id").get<uint64_t>(), node2Id);
    cmpLoc[0] = 13.4;
    cmpLoc[1] = -23;
    EXPECT_EQ(cmpJson.at("location"), cmpLoc);

    //test getting location of a mobile node
    cmpJson = nullptr;
    cmpJson = locationService->requestNodeLocationDataAsJson(3);
    EXPECT_EQ(cmpJson["id"], node3Id);
    cmpLoc[0] = 52.55227464714949;
    cmpLoc[1] = 13.351743136322877;
    //todo #3390: this currently returns the fixed location that was set, although this is a mobile node
    //EXPECT_EQ(cmpJson["location"], cmpLoc);

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);
}

//todo #3390: set mobile node locations via rpc calls so locations can acutally be returned
TEST_F(LocationServiceTest, DISABLED_testRequestAllMobileNodeLocations) {
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;
    uint64_t rpcPortWrk4 = 6003;

    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node1Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk1, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;
    auto node2Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk2, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk3, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node4Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk4, 0, 0, properties);

    topologyManagerService->updateGeoLocation(node2Id, {13.4, -23});

    auto response0 = locationService->requestLocationDataFromAllMobileNodesAsJson();

    EXPECT_EQ(response0.get<std::vector<nlohmann::json>>().size(), 1);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

    auto response1 = locationService->requestLocationDataFromAllMobileNodesAsJson();
    auto getLocResp1 = response1.get<std::vector<nlohmann::json>>();
    EXPECT_TRUE(getLocResp1.size() == 1);

    nlohmann::json cmpLoc;
    cmpLoc[0] = 13.4;
    cmpLoc[1] = -23;
    auto entry = getLocResp1[0].get<std::map<std::string, nlohmann::json>>();
    EXPECT_TRUE(entry.size() == 2);
    EXPECT_TRUE(entry.find("id") != entry.end());
    EXPECT_EQ(entry.at("id"), node2Id);
    EXPECT_TRUE(entry.find("location") != entry.end());
    EXPECT_EQ(entry.at("location"), cmpLoc);

    cmpLoc[0] = 52.55227464714949;
    cmpLoc[1] = 13.351743136322877;

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->rpcPort = rpcPortWrk4;
    wrkConf4->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf4->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf4->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation2.csv");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart4);

    auto response2 = locationService->requestLocationDataFromAllMobileNodesAsJson();
    auto getLocResp2 = response2.get<std::vector<nlohmann::json>>();
    EXPECT_EQ(getLocResp2.size(), 3);

    for (auto e : getLocResp2) {
        entry = e.get<std::map<std::string, nlohmann::json>>();
        EXPECT_TRUE(entry.size() == 3);
        EXPECT_TRUE(entry.find("id") != entry.end());
        NES_DEBUG("checking element with id " << entry.at("id"));
        EXPECT_TRUE(entry.at("id") == node3Id || entry.at("id") == node4Id);
        if (entry.at("id") == node3Id) {
//todo #3390: this currently returns the fixed location
            /*
            cmpLoc[0] = 52.55227464714949;
            cmpLoc[1] = 13.351743136322877;
             */
            continue;
        }
        if (entry.at("id") == node4Id) {
            cmpLoc[0] = 53.55227464714949;
            cmpLoc[1] = -13.351743136322877;
        }
        EXPECT_TRUE(entry.find("location") != entry.end());
        EXPECT_EQ(entry.at("location"), cmpLoc);
    }

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);

    bool retStopWrk4 = wrk4->stop(false);
    EXPECT_TRUE(retStopWrk4);
}

//TODO #3391: currently retrieving scheduled reconnects is not implemented
TEST_F(LocationServiceTest, DISABLED_testRequestEmptyReconnectSchedule) {
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk3 = 6002;
    nlohmann::json cmpLoc;
    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk1, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker("127.0.0.1", rpcPortWrk3, 0, 0, properties);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);

    auto response1 = locationService->requestReconnectScheduleAsJson(node3Id);
    auto entry = response1.get<std::map<std::string, nlohmann::json>>();
    EXPECT_EQ(entry.size(), 4);
    EXPECT_NE(entry.find("pathStart"), entry.end());
    EXPECT_EQ(entry.at("pathStart"), nullptr);
    EXPECT_NE(entry.find("pathEnd"), entry.end());
    EXPECT_EQ(entry.at("pathEnd"), nullptr);
    EXPECT_NE(entry.find("indexUpdatePosition"), entry.end());
    EXPECT_EQ(entry.at("indexUpdatePosition"), nullptr);
    EXPECT_NE(entry.find("reconnectPoints"), entry.end());
    EXPECT_EQ(entry.at("reconnectPoints").size(), 0);

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);
}

TEST_F(LocationServiceTest, testConvertingToJson) {
    double lat = 10.5;
    double lng = -3.3;
    auto loc1 = NES::Spatial::DataTypes::Experimental::GeoLocation(lat, lng);
    auto validLocJson = convertNodeLocationInfoToJson(1, loc1);

    EXPECT_EQ(validLocJson["id"], 1);
    EXPECT_EQ(validLocJson["location"][0], lat);
    EXPECT_EQ(validLocJson["location"][1], lng);

    auto invalidLoc = NES::Spatial::DataTypes::Experimental::GeoLocation();
    auto invalidLocJson = convertNodeLocationInfoToJson(2, invalidLoc);
    NES_DEBUG("Invalid location json: " << invalidLocJson.dump())
    EXPECT_EQ(invalidLocJson["id"], 2);

    EXPECT_TRUE(invalidLocJson["location"].empty());

    NES::Spatial::DataTypes::Experimental::GeoLocation locNullPtr;
    auto nullLocJson = convertNodeLocationInfoToJson(3, locNullPtr);
    EXPECT_EQ(nullLocJson["id"], 3);
    EXPECT_TRUE(nullLocJson["location"].empty());
}
}// namespace NES
#endif