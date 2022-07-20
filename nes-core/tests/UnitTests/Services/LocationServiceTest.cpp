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

#include <Common/Location.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <NesBaseTest.hpp>
#include <Services/LocationService.hpp>
#include <Spatial/LocationIndex.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Logger/Logger.hpp>
#include <cmath>
#include <gtest/gtest.h>

namespace NES {

std::string ip = "127.0.0.1";
namespace Spatial::Index::Experimental {
using LocationServicePtr = std::shared_ptr<LocationService>;
}

class LocationServiceTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Set up LocationServiceTest test class.")
    }
    static void TearDownTestCase(){NES_INFO("Tear down LocationServiceTest test class")}

    web::json::value convertNodeLocationInfoToJson(uint64_t id, NES::Spatial::Index::Experimental::LocationPtr loc) {
        web::json::value nodeInfo;
        nodeInfo["id"] = web::json::value(id);
        web::json::value locJson;
        if (loc) {
            locJson[0] = loc->getLatitude();
            locJson[1] = loc->getLongitude();
        } else {
            locJson = web::json::value::null();
        }
        nodeInfo["location"] = web::json::value(locJson);
        return nodeInfo;
    }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    NES::Spatial::Index::Experimental::LocationServicePtr service;
};

TEST_F(LocationServiceTest, testRequestSingleNodeLocation) {
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;

    web::json::value cmpJson;
    web::json::value cmpLoc;
    TopologyPtr topology = Topology::create();
    service = std::make_shared<NES::Spatial::Index::Experimental::LocationService>(topology);

    NES::Spatial::Index::Experimental::LocationIndexPtr locIndex = topology->getLocationIndex();
    TopologyNodePtr node1 = TopologyNode::create(1, "127.0.0.1", rpcPortWrk1, 0, 0);
    TopologyNodePtr node2 = TopologyNode::create(2, "127.0.0.1", rpcPortWrk2, 0, 0);
    //setting coordinates for field node which should not show up in the response when querying for mobile nodes
    node2->setFixedCoordinates(13.4, -23);
    node2->setSpatialType(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
#ifdef S2DEF
    TopologyNodePtr node3 = TopologyNode::create(3, "127.0.0.1", rpcPortWrk3, 0, 0);
    node3->setSpatialType(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
#endif

    topology->setAsRoot(node1);
    topology->addNewTopologyNodeAsChild(node1, node2);

    locIndex->initializeFieldNodeCoordinates(node2, (node2->getCoordinates()));

#ifdef S2DEF
    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->spatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);
    topology->addNewTopologyNodeAsChild(node1, node3);
    locIndex->addMobileNode(node3);
#endif

    // test querying for node which does not exist in the system
    EXPECT_EQ(service->requestNodeLocationDataAsJson(1234), web::json::value::null());

    //test getting location of node which does not have a location
    cmpJson = service->requestNodeLocationDataAsJson(1);
    EXPECT_EQ(cmpJson["id"], 1);
    EXPECT_EQ(cmpJson["location"], web::json::value::null());

    //test getting location of field node
    cmpJson = service->requestNodeLocationDataAsJson(2);
    EXPECT_EQ(cmpJson["id"], 2);
    EXPECT_EQ(cmpJson["location"][0], 13.4);
    EXPECT_EQ(cmpJson["location"][1].as_integer(), -23);

    //test getting location of a mobile node
#ifdef S2DEF
    cmpJson = service->requestNodeLocationDataAsJson(3);
    EXPECT_EQ(cmpJson["id"], 3);
    EXPECT_EQ(cmpJson["location"][0], 52.55227464714949);
    EXPECT_EQ(cmpJson["location"][1], 13.351743136322877);

    bool retStopWrk3 = wrk3->stop(false);
    EXPECT_TRUE(retStopWrk3);
#endif
}

#ifdef S2DEF
TEST_F(LocationServiceTest, testRequestAllMobileNodeLocations) {
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;
    uint64_t rpcPortWrk4 = 6003;
    web::json::value cmpLoc;
    TopologyPtr topology = Topology::create();
    service = std::make_shared<NES::Spatial::Index::Experimental::LocationService>(topology);
    NES::Spatial::Index::Experimental::LocationIndexPtr locIndex = topology->getLocationIndex();
    TopologyNodePtr node1 = TopologyNode::create(1, "127.0.0.1", rpcPortWrk1, 0, 0);
    TopologyNodePtr node2 = TopologyNode::create(2, "127.0.0.1", rpcPortWrk2, 0, 0);
    //setting coordinates for field node which should not show up in the response when querying for mobile nodes
    node2->setSpatialType(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
    node2->setFixedCoordinates(13.4, -23);
    TopologyNodePtr node3 = TopologyNode::create(3, "127.0.0.1", rpcPortWrk3, 0, 0);
    node3->setSpatialType(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    TopologyNodePtr node4 = TopologyNode::create(4, "127.0.0.1", rpcPortWrk4, 0, 0);
    node4->setSpatialType(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    topology->setAsRoot(node1);
    topology->addNewTopologyNodeAsChild(node1, node2);

    locIndex->initializeFieldNodeCoordinates(node2, node2->getCoordinates());

    auto response0 = service->requestLocationDataFromAllMobileNodesAsJson();

    //no mobile nodes added yet. List should be empty
    NES_DEBUG("Response: " << response0.serialize());
    EXPECT_EQ(response0.as_array().size(), 0);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->spatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);
    topology->addNewTopologyNodeAsChild(node1, node3);
    locIndex->addMobileNode(node3);

    auto response1 = service->requestLocationDataFromAllMobileNodesAsJson();
    auto getLocResp1 = response1.as_array();
    NES_DEBUG("Response: " << response1.serialize());
    EXPECT_TRUE(getLocResp1.size() == 1);

    cmpLoc[0] = 52.55227464714949;
    cmpLoc[1] = 13.351743136322877;
    auto entry = getLocResp1[0].as_object();
    EXPECT_TRUE(entry.size() == 2);
    EXPECT_TRUE(entry.find("id") != entry.end());
    EXPECT_EQ(entry.at("id"), 3);
    EXPECT_TRUE(entry.find("location") != entry.end());
    EXPECT_EQ(entry.at("location"), cmpLoc);

    NES_INFO("start worker 4");
    WorkerConfigurationPtr wrkConf4 = WorkerConfiguration::create();
    wrkConf4->rpcPort = rpcPortWrk4;
    wrkConf4->spatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf4->mobilityConfiguration.locationProviderType.setValue(NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf4->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation2.csv");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart4);
    topology->addNewTopologyNodeAsChild(node1, node4);
    locIndex->addMobileNode(node4);

    auto response2 = service->requestLocationDataFromAllMobileNodesAsJson();
    auto getLocResp2 = response2.as_array();
    NES_DEBUG("Response: " << response2.serialize());
    EXPECT_TRUE(getLocResp2.size() == 2);

    for (auto e : getLocResp2) {
        entry = e.as_object();
        EXPECT_TRUE(entry.size() == 2);
        EXPECT_TRUE(entry.find("id") != entry.end());
        NES_DEBUG("checking element with id " << entry.at("id"));
        EXPECT_TRUE(entry.at("id") == 3 || entry.at("id") == 4);
        if (entry.at("id") == 3) {
            cmpLoc[0] = 52.55227464714949;
            cmpLoc[1] = 13.351743136322877;
        }
        if (entry.at("id") == 4) {
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
#endif

TEST_F(LocationServiceTest, testConvertingToJson) {
    double lat = 10.5;
    double lng = -3.3;
    auto loc1 = std::make_shared<NES::Spatial::Index::Experimental::Location>(lat, lng);
    auto validLocJson = convertNodeLocationInfoToJson(1, loc1);

    EXPECT_EQ(validLocJson["id"], 1);
    EXPECT_EQ(validLocJson["location"][0], lat);
    EXPECT_EQ(validLocJson["location"][1], lng);

    auto invalidLoc = std::make_shared<NES::Spatial::Index::Experimental::Location>();
    auto invalidLocJson = convertNodeLocationInfoToJson(2, invalidLoc);
    NES_DEBUG("Invalid location json: " << invalidLocJson.serialize())
    EXPECT_EQ(invalidLocJson["id"], 2);
    EXPECT_TRUE(std::isnan(invalidLocJson["location"][0].as_double()));
    EXPECT_TRUE(std::isnan(invalidLocJson["location"][1].as_double()));

    std::shared_ptr<NES::Spatial::Index::Experimental::Location> locNullPtr;
    auto nullLocJson = convertNodeLocationInfoToJson(3, locNullPtr);
    EXPECT_EQ(nullLocJson["id"], 3);
    EXPECT_EQ(nullLocJson["location"], web::json::value::null());
}
}// namespace NES
