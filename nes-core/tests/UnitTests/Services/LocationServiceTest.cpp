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
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
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

class LocationServiceTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Set up LocationServiceTest test class.")
        std::string singleLocationPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "singleLocation.csv";
        remove(singleLocationPath.c_str());
        writeWaypointsToCsv(singleLocationPath, {{{52.55227464714949, 13.351743136322877}, 0}});
    }
    static void TearDownTestCase(){NES_INFO("Tear down LocationServiceTest test class")}

    NES::LocationServicePtr locationService;
    NES::TopologyManagerServicePtr topologyManagerService;
};

TEST_F(LocationServiceTest, testRequestSingleNodeLocation) {
    auto rpcPortWrk1 = getAvailablePort();
    auto rpcPortWrk2 = getAvailablePort();
    auto rpcPortWrk3 = getAvailablePort();

    nlohmann::json nodeLocationInfoJson;
    nlohmann::json locationJson;

    //create topology and services
    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    //create node without location
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node1Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk1, 0, 0, properties);

    //create fixed location node
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;
    auto node2Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk2, 0, 0, properties);
    topologyManagerService->addGeoLocation(node2Id, {13.4, -23});

    //create mobile node
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk3, 0, 0, properties);
    topologyManagerService->updateGeoLocation(node3Id, {52.55227464714949, 13.351743136322877});

    // test querying for node which does not exist in the system
    ASSERT_EQ(locationService->requestNodeLocationDataAsJson(1234), nullptr);

    //test getting location of node which does not have a location
    nodeLocationInfoJson = locationService->requestNodeLocationDataAsJson(node1Id);
    EXPECT_EQ(nodeLocationInfoJson["id"], node1Id);
    ASSERT_TRUE(nodeLocationInfoJson["location"].empty());

    //test getting location of field node
    nodeLocationInfoJson = locationService->requestNodeLocationDataAsJson(node2Id);
    auto entry = nodeLocationInfoJson.get<std::map<std::string, nlohmann::json>>();
    EXPECT_EQ(entry.at("id").get<uint64_t>(), node2Id);
    locationJson = nodeLocationInfoJson.at("location");
    EXPECT_EQ(locationJson["latitude"], 13.4);
    EXPECT_EQ(locationJson["longitude"], -23);

    //test getting location of a mobile node
    nodeLocationInfoJson = locationService->requestNodeLocationDataAsJson(node3Id);
    ASSERT_EQ(nodeLocationInfoJson["id"], node3Id);
    locationJson = nodeLocationInfoJson.at("location");
    EXPECT_EQ(locationJson["latitude"], 52.55227464714949);
    EXPECT_EQ(locationJson["longitude"], 13.351743136322877);
}

TEST_F(LocationServiceTest, testRequestAllMobileNodeLocations) {
    auto rpcPortWrk1 = getAvailablePort();
    auto rpcPortWrk2 = getAvailablePort();
    auto rpcPortWrk3 = getAvailablePort();
    auto rpcPortWrk4 = getAvailablePort();

    nlohmann::json locationJson;

    //create topology and services
    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    locationService = std::make_shared<NES::LocationService>(topology, locationIndex);
    topologyManagerService = std::make_shared<NES::TopologyManagerService>(topology, locationIndex);

    //get mobile node info
    auto response = locationService->requestLocationAndParentDataFromAllMobileNodes().get<allMobileResponse>();

    //check that the json does not contain any nodes or edges
    ASSERT_EQ(response["nodes"].size(), 0);
    ASSERT_EQ(response["edges"].size(), 0);

    //create node without location
    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto node1Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk1, 0, 0, properties);

    //create fixed location node
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;
    auto node2Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk2, 0, 0, properties);
    topologyManagerService->addGeoLocation(node2Id, {13.4, -23});

    //create mobile node
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node3Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk3, 0, 0, properties);

    //create mobile node
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::MOBILE_NODE;
    auto node4Id = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, "127.0.0.1", *rpcPortWrk4, 0, 0, properties);

    //get mobile node info
    response = locationService->requestLocationAndParentDataFromAllMobileNodes().get<allMobileResponse>();

    //while no location is known, the mobile nodes should not appear in the list
    ASSERT_EQ(response["nodes"].size(), 0);
    ASSERT_EQ(response["edges"].size(), 0);

    //update the location of node 3
    topologyManagerService->updateGeoLocation(node3Id, {52.55227464714949, 13.351743136322877});

    //get mobile node locations
    auto response1 = locationService->requestLocationAndParentDataFromAllMobileNodes();
    auto getLocResp1 = response1.get<allMobileResponse>();

    //verify that the object contains 2 entries (list of nodes and list of edges)
    ASSERT_TRUE(getLocResp1.size() == 2);

    auto nodes = getLocResp1["nodes"];
    ASSERT_EQ(nodes.size(), 1);

    //verify correctness of the node information
    auto entry = nodes[0];
    EXPECT_EQ(entry.size(), 2);
    EXPECT_TRUE(entry.find("id") != entry.end());
    EXPECT_EQ(entry.at("id"), node3Id);
    EXPECT_TRUE(entry.find("location") != entry.end());
    locationJson = entry.at("location");
    EXPECT_EQ(locationJson["latitude"], 52.55227464714949);
    EXPECT_EQ(locationJson["longitude"], 13.351743136322877);

    //respone should contain only one edge
    auto edges = getLocResp1["edges"];
    ASSERT_EQ(edges.size(), 1);

    //verify correctness of the provided edge
    const auto& edge = edges[0];
    EXPECT_EQ(edge.size(), 2);
    EXPECT_NE(edge.find("source"), edge.end());
    EXPECT_EQ(edge.at("source"), node3Id);
    EXPECT_NE(edge.find("target"), edge.end());
    EXPECT_EQ(edge.at("target"), 1);

    //update the location of node 4
    topologyManagerService->updateGeoLocation(node4Id, {53.55227464714949, -13.351743136322877});

    //verify that the object contains 2 entries (list of nodes and list of edges)
    auto response2 = locationService->requestLocationAndParentDataFromAllMobileNodes();
    auto getLocResp2 = response2.get<allMobileResponse>();
    ASSERT_EQ(getLocResp2.size(), 2);

    //verify correct size of node list and edge list
    nodes = getLocResp2["nodes"];
    edges = getLocResp2["edges"];
    ASSERT_EQ(nodes.size(), 2);
    ASSERT_EQ(edges.size(), 2);

    //check correctness of locations
    for (const auto& node : nodes) {
        EXPECT_EQ(node.size(), 2);
        EXPECT_TRUE(node.contains("location"));
        EXPECT_TRUE(node.contains("id"));
        const auto nodeLocation = node.at("location");
        if (node.at("id") == node3Id) {
            EXPECT_EQ(nodeLocation.at("latitude"), 52.55227464714949);
            EXPECT_EQ(nodeLocation["longitude"], 13.351743136322877);
        } else if (node.at("id") == node4Id) {
            EXPECT_EQ(nodeLocation["latitude"], 53.55227464714949);
            EXPECT_EQ(nodeLocation["longitude"], -13.351743136322877);
        } else {
            FAIL();
        }
    }

    //check correctness of edges
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

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Experimental::SpatialType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::filesystem::path(TEST_DATA_DIRECTORY)
                                                                    / "singleLocation.csv");
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
}// namespace NES
#endif