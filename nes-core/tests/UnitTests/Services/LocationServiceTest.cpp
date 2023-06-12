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
#include <Topology/Topology.hpp>
#include <Util/Experimental/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cmath>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#ifdef S2DEF
namespace NES {

std::string ip = "127.0.0.1";

using allMobileResponse = std::map<std::string, std::vector<std::map<std::string, nlohmann::json>>>;

class LocationServiceTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO2("Set up LocationServiceTest test class.")
        std::string singleLocationPath = std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv";
        remove(singleLocationPath.c_str());
        writeWaypointsToCsv(singleLocationPath, {{{52.55227464714949, 13.351743136322877}, 0}});
    }
    static void TearDownTestCase(){NES_INFO2("Tear down LocationServiceTest test class")}

    nlohmann::json convertNodeLocationInfoToJson(uint64_t id, NES::Spatial::DataTypes::Experimental::GeoLocation loc) {
        nlohmann::json nodeInfo;
        nodeInfo["id"] = id;
        nlohmann::json::array_t locJson;
        if (loc.isValid()) {
            locJson.push_back(loc.getLatitude());
            locJson.push_back((loc.getLongitude()));
        }

        nodeInfo["location"] = locJson;
        NES_DEBUG2("{}", nodeInfo.dump());
        return nodeInfo;
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    NES::LocationServicePtr locationService;
    NES::TopologyManagerServicePtr topologyManagerService;
};

TEST_F(LocationServiceTest, testRequestSingleNodeLocation) {
    auto rpcPortWrk1 = getAvailablePort();
    auto rpcPortWrk2 = getAvailablePort();
    auto rpcPortWrk3 = getAvailablePort();

    nlohmann::json cmpJson;
    nlohmann::json cmpLoc;
    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node1Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk1, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;
    auto node2Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk2, 0, 0, properties);
    topologyManagerService->addGeoLocation(node2Id, {13.4, -23});

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk3, 0, 0, properties);
    topologyManagerService->updateGeoLocation(node3Id, {52.55227464714949, 13.351743136322877});

    // test querying for node which does not exist in the system
    ASSERT_EQ(locationService->requestNodeLocationDataAsJson(1234), nullptr);

    //test getting location of node which does not have a location
    cmpJson = locationService->requestNodeLocationDataAsJson(node1Id);
    EXPECT_EQ(cmpJson["id"], node1Id);
    ASSERT_TRUE(cmpJson["location"].empty());

    //test getting location of field node
    cmpJson = locationService->requestNodeLocationDataAsJson(node2Id);
    auto entry = cmpJson.get<std::map<std::string, nlohmann::json>>();
    EXPECT_EQ(entry.at("id").get<uint64_t>(), node2Id);
    cmpLoc["latitude"] = 13.4;
    cmpLoc["longitude"] = -23;
    ASSERT_EQ(cmpJson.at("location"), cmpLoc);

    //test getting location of a mobile node
    cmpJson = nullptr;
    cmpJson = locationService->requestNodeLocationDataAsJson(node3Id);
    ASSERT_EQ(cmpJson["id"], node3Id);
    cmpLoc["latitude"] = 52.55227464714949;
    cmpLoc["longitude"] = 13.351743136322877;
}

TEST_F(LocationServiceTest, testRequestAllMobileNodeLocations) {
    auto rpcPortWrk1 = getAvailablePort();
    auto rpcPortWrk2 = getAvailablePort();
    auto rpcPortWrk3 = getAvailablePort();
    auto rpcPortWrk4 = getAvailablePort();

    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    auto response = locationService->requestLocationAndParentDataFromAllMobileNodes()
        .get<allMobileResponse>();

    ASSERT_EQ(response["nodes"].size(), 0);
    ASSERT_EQ(response["edges"].size(), 0);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node1Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk1, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;
    auto node2Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk2, 0, 0, properties);
    topologyManagerService->addGeoLocation(node2Id, {13.4, -23});

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk3, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node4Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk4, 0, 0, properties);

    response = locationService->requestLocationAndParentDataFromAllMobileNodes()
        .get<allMobileResponse>();

    ASSERT_EQ(response["nodes"].size(), 0);
    ASSERT_EQ(response["edges"].size(), 0);

    topologyManagerService->updateGeoLocation(node3Id, {52.55227464714949, 13.351743136322877});

    auto response1 = locationService->requestLocationAndParentDataFromAllMobileNodes();
    auto getLocResp1 = response1.get<allMobileResponse>();
    ASSERT_TRUE(getLocResp1.size() == 2);

    nlohmann::json cmpLoc;
    cmpLoc["latitude"] = 52.55227464714949;
    cmpLoc["longitude"] = 13.351743136322877;
    auto nodes = getLocResp1["nodes"];
    ASSERT_EQ(nodes.size(), 1);

    auto entry = nodes[0];
    EXPECT_EQ(entry.size(), 2);
    EXPECT_TRUE(entry.find("id") != entry.end());
    EXPECT_EQ(entry.at("id"), node3Id);
    EXPECT_TRUE(entry.find("location") != entry.end());
    ASSERT_EQ(entry.at("location"), cmpLoc);

    auto edges = getLocResp1["edges"];
    ASSERT_EQ(edges.size(), 1);

    const auto& edge = edges[0];
    EXPECT_EQ(edge.size(), 2);
    EXPECT_NE(edge.find("source"), edge.end());
    EXPECT_EQ(edge.at("source"), node3Id);
    EXPECT_NE(edge.find("target"), edge.end());
    EXPECT_EQ(edge.at("target"), 1);

    topologyManagerService->updateGeoLocation(node4Id, {53.55227464714949, -13.351743136322877});

    auto response2 = locationService->requestLocationAndParentDataFromAllMobileNodes();
    auto getLocResp2 = response2.get<allMobileResponse>();
    ASSERT_EQ(getLocResp2.size(), 2);
    nodes = getLocResp2["nodes"];
    edges = getLocResp2["edges"];
    ASSERT_EQ(nodes.size(), 2);
    ASSERT_EQ(edges.size(), 2);

    for (const auto& node : nodes) {
        EXPECT_EQ(node.size(), 2);
        EXPECT_TRUE(node.find("id") != node.end());
        NES_DEBUG2("checking element with id {}", node.at("id"));
        EXPECT_TRUE(node.at("id") == node3Id || node.at("id") == node4Id);
        if (node.at("id") == node3Id) {
            cmpLoc["latitude"] = 52.55227464714949;
            cmpLoc["longitude"] = 13.351743136322877;
        }
        if (node.at("id") == node4Id) {
            cmpLoc["latitude"] = 53.55227464714949;
            cmpLoc["longitude"] = -13.351743136322877;
        }
        EXPECT_TRUE(node.find("location") != node.end());
        EXPECT_EQ(node.at("location"), cmpLoc);
    }

    std::vector sources = {node3Id, node4Id};
    for (const auto& topologyEdge : edges) {
        ASSERT_EQ(topologyEdge.at("target"), 1);
        auto edgeSource = topologyEdge.at("source");
        auto sourcesIterator = std::find(sources.begin(), sources.end(), edgeSource);
        ASSERT_NE(sourcesIterator, sources.end());
        sources.erase(sourcesIterator);
    }
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
    topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", rpcPortWrk1, 0, 0, properties);

    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", rpcPortWrk3, 0, 0, properties);

    NES_INFO2("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    ASSERT_TRUE(retStart3);

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
    ASSERT_EQ(entry.at("reconnectPoints").size(), 0);

    bool retStopWrk3 = wrk3->stop(false);
    ASSERT_TRUE(retStopWrk3);
}

TEST_F(LocationServiceTest, testConvertingToJson) {
    double lat = 10.5;
    double lng = -3.3;
    auto loc1 = NES::Spatial::DataTypes::Experimental::GeoLocation(lat, lng);
    auto validLocJson = convertNodeLocationInfoToJson(1, loc1);

    EXPECT_EQ(validLocJson["id"], 1);
    EXPECT_EQ(validLocJson["location"][0], lat);
    ASSERT_EQ(validLocJson["location"][1], lng);

    auto invalidLoc = NES::Spatial::DataTypes::Experimental::GeoLocation();
    auto invalidLocJson = convertNodeLocationInfoToJson(2, invalidLoc);
    NES_DEBUG2("Invalid location json: {}", invalidLocJson.dump())
    EXPECT_EQ(invalidLocJson["id"], 2);
    ASSERT_TRUE(invalidLocJson["location"].empty());

    NES::Spatial::DataTypes::Experimental::GeoLocation locNullPtr;
    auto nullLocJson = convertNodeLocationInfoToJson(3, locNullPtr);
    EXPECT_EQ(nullLocJson["id"], 3);
    ASSERT_TRUE(nullLocJson["location"].empty());
}
}// namespace NES
#endif