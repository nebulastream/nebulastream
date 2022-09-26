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

#include <NesBaseTest.hpp>
#include <gtest/gtest.h>

#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <REST/Controller/LocationController.hpp>
#include <REST/RestEngine.hpp>
#include <Services/LocationService.hpp>
#include <Spatial/Index/LocationIndex.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Experimental/NodeType.hpp>

using namespace std;
namespace NES {

//TODO: to be deleted with #3001
class LocationControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LocationControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down LocationControllerTest test class."); }

    std::string location2 = "52.53736960143897, 13.299134894776092";
    std::string location3 = "52.52025049345923, 13.327886280405611";
    std::string location4 = "52.49846981391786, 13.514464421192917";
    LocationControllerPtr controller;
};

TEST_F(LocationControllerTest, testBadGETRequests) {
    TopologyPtr topology = Topology::create();
    NES::Spatial::Index::Experimental::LocationServicePtr service =
        std::make_shared<NES::Spatial::Index::Experimental::LocationService>(topology);
    controller = std::make_shared<NES::LocationController>(service);

    //test request without nodeId parameter
    web::http::http_request msg1(web::http::methods::GET);
    msg1.set_request_uri(web::http::uri("location"));

    web::http::http_response httpResponse1;
    web::json::value response1;
    controller->handleGet(std::vector<utility::string_t>{"location"}, msg1);

    msg1.get_response()
        .then([&httpResponse1](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse1 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse1.extract_json()
        .then([&response1](const pplx::task<web::json::value>& task) {
            response1 = task.get();
        })
        .wait();
    auto getLocResp1 = response1.as_object();
    NES_DEBUG("Response: " << response1.serialize());
    EXPECT_TRUE(getLocResp1.size() == 2);
    EXPECT_TRUE(getLocResp1.find("message") != getLocResp1.end());
    EXPECT_EQ(getLocResp1.at("message").as_string(), "Parameter nodeId must be provided");
    EXPECT_TRUE(getLocResp1.find("code") != getLocResp1.end());
    EXPECT_EQ(getLocResp1.at("code"), 400);

    //test request for inexistent node
    web::http::http_request msg2(web::http::methods::GET);
    msg2.set_request_uri(web::http::uri("location?nodeId=1234"));

    web::http::http_response httpResponse2;
    web::json::value response2;
    controller->handleGet(std::vector<utility::string_t>{"location"}, msg2);

    msg2.get_response()
        .then([&httpResponse2](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse2 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse2.extract_json()
        .then([&response2](const pplx::task<web::json::value>& task) {
            response2 = task.get();
        })
        .wait();
    auto getLocResp2 = response2.as_object();
    NES_DEBUG("Response: " << response2.serialize());
    EXPECT_TRUE(getLocResp2.size() == 2);
    EXPECT_TRUE(getLocResp2.find("message") != getLocResp2.end());
    EXPECT_EQ(getLocResp2.at("message").as_string(), "No node with this Id");
    EXPECT_TRUE(getLocResp2.find("code") != getLocResp2.end());
    EXPECT_EQ(getLocResp2.at("code"), 404);

    //test request without for non numerical id
    web::http::http_request msg3(web::http::methods::GET);
    msg3.set_request_uri(web::http::uri("location?nodeId=abc"));

    web::http::http_response httpResponse3;
    web::json::value response3;
    controller->handleGet(std::vector<utility::string_t>{"location"}, msg3);

    msg3.get_response()
        .then([&httpResponse3](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse3 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse3.extract_json()
        .then([&response3](const pplx::task<web::json::value>& task) {
            response3 = task.get();
        })
        .wait();
    auto getLocResp3 = response3.as_object();
    NES_DEBUG("Response: " << response3.serialize());
    EXPECT_TRUE(getLocResp3.size() == 2);
    EXPECT_TRUE(getLocResp3.find("message") != getLocResp3.end());
    EXPECT_EQ(getLocResp3.at("message").as_string(), "Parameter nodeId must be an unsigned integer");
    EXPECT_TRUE(getLocResp3.find("code") != getLocResp3.end());
    EXPECT_EQ(getLocResp3.at("code"), 400);

    //test request without nodeId parameter
    web::http::http_request msg4(web::http::methods::GET);
    msg4.set_request_uri(web::http::uri("location?nodeId=18446744073709551616"));

    web::http::http_response httpResponse4;
    web::json::value response4;
    controller->handleGet(std::vector<utility::string_t>{"location"}, msg4);

    msg4.get_response()
        .then([&httpResponse4](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse4 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse4.extract_json()
        .then([&response4](const pplx::task<web::json::value>& task) {
            response4 = task.get();
        })
        .wait();
    auto getLocResp4 = response4.as_object();
    NES_DEBUG("Response: " << response4.serialize());
    EXPECT_TRUE(getLocResp4.size() == 2);
    EXPECT_TRUE(getLocResp4.find("message") != getLocResp4.end());
    EXPECT_EQ(getLocResp4.at("message").as_string(), "Parameter nodeId must be in 64bit unsigned int value range");
    EXPECT_TRUE(getLocResp4.find("code") != getLocResp4.end());
    EXPECT_EQ(getLocResp4.at("code"), 400);
}

TEST_F(LocationControllerTest, testGETSingleLocation) {
    TopologyPtr topology = Topology::create();
    NES::Spatial::Index::Experimental::LocationServicePtr service =
        std::make_shared<NES::Spatial::Index::Experimental::LocationService>(topology);
    controller = std::make_shared<LocationController>(service);
    TopologyNodePtr node = TopologyNode::create(3, "127.0.0.1", 0, 0, 0);
    TopologyNodePtr node2 = TopologyNode::create(4, "127.0.0.1", 1, 0, 0);
    node2->setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
    node2->setFixedCoordinates(13.4, -23);
    topology->setAsRoot(node);
    topology->addNewTopologyNodeAsChild(node, node2);

    //test getting location of node without location
    web::http::http_request msg1(web::http::methods::GET);
    msg1.set_request_uri(web::http::uri("location?nodeId=3"));

    web::http::http_response httpResponse1;
    web::json::value response1;
    controller->handleGet(std::vector<utility::string_t>{"location"}, msg1);

    msg1.get_response()
        .then([&httpResponse1](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse1 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse1.extract_json()
        .then([&response1](const pplx::task<web::json::value>& task) {
            response1 = task.get();
        })
        .wait();
    auto getLocResp1 = response1.as_object();
    NES_DEBUG("Response: " << response1.serialize());
    EXPECT_TRUE(getLocResp1.size() == 2);
    EXPECT_TRUE(getLocResp1.find("id") != getLocResp1.end());
    EXPECT_EQ(getLocResp1.at("id"), 3);
    EXPECT_TRUE(getLocResp1.find("location") != getLocResp1.end());
    EXPECT_EQ(getLocResp1.at("location"), web::json::value::null());

    //test getting location of node with location
    web::http::http_request msg2(web::http::methods::GET);
    msg2.set_request_uri(web::http::uri("location?nodeId=4"));

    web::http::http_response httpResponse2;
    web::json::value response2;
    controller->handleGet(std::vector<utility::string_t>{"location"}, msg2);

    msg2.get_response()
        .then([&httpResponse2](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse2 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse2.extract_json()
        .then([&response2](const pplx::task<web::json::value>& task) {
            response2 = task.get();
        })
        .wait();
    auto getLocResp2 = response2.as_object();
    web::json::value location;
    location[0] = 13.4;
    location[1] = -23;
    NES_DEBUG("Response: " << response2.serialize());
    EXPECT_TRUE(getLocResp2.size() == 2);
    EXPECT_TRUE(getLocResp2.find("id") != getLocResp2.end());
    EXPECT_EQ(getLocResp2.at("id"), 4);
    EXPECT_TRUE(getLocResp2.find("location") != getLocResp2.end());
    EXPECT_EQ(getLocResp2.at("location"), location);
}

#ifdef S2DEF
TEST_F(LocationControllerTest, testGETAllMobileLocations) {
    uint64_t rpcPortWrk1 = 6000;
    uint64_t rpcPortWrk2 = 6001;
    uint64_t rpcPortWrk3 = 6002;
    uint64_t rpcPortWrk4 = 6003;
    web::json::value cmpLoc;
    TopologyPtr topology = Topology::create();
    NES::Spatial::Index::Experimental::LocationServicePtr service =
        std::make_shared<NES::Spatial::Index::Experimental::LocationService>(topology);
    controller = std::make_shared<LocationController>(service);
    NES::Spatial::Index::Experimental::LocationIndexPtr locIndex = topology->getLocationIndex();
    TopologyNodePtr node1 = TopologyNode::create(1, "127.0.0.1", rpcPortWrk1, 0, 0);
    TopologyNodePtr node2 = TopologyNode::create(2, "127.0.0.1", rpcPortWrk2, 0, 0);
    //setting coordinates for field node which should not show up in the response when querying for mobile nodes
    node2->setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION);
    node2->setFixedCoordinates(13.4, -23);
    TopologyNodePtr node3 = TopologyNode::create(3, "127.0.0.1", rpcPortWrk3, 0, 0);
    node3->setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    TopologyNodePtr node4 = TopologyNode::create(4, "127.0.0.1", rpcPortWrk4, 0, 0);
    node4->setSpatialNodeType(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    topology->setAsRoot(node1);
    topology->addNewTopologyNodeAsChild(node1, node2);

    locIndex->initializeFieldNodeCoordinates(node2, (node2->getCoordinates()));

    //no mobile nodes added yet. List should be empty
    web::http::http_request msg0(web::http::methods::GET);
    msg0.set_request_uri(web::http::uri("location/allMobile"));

    web::http::http_response httpResponse0;
    web::json::value response0;
    controller->handleGet(std::vector<utility::string_t>{"location", "allMobile"}, msg0);

    msg0.get_response()
        .then([&httpResponse0](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse0 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse0.extract_json()
        .then([&response0](const pplx::task<web::json::value>& task) {
            response0 = task.get();
        })
        .wait();
    auto getLocResp0 = response0.as_array();
    NES_DEBUG("Response: " << response0.serialize());
    EXPECT_TRUE(getLocResp0.size() == 0);

    NES_INFO("start worker 3");
    WorkerConfigurationPtr wrkConf3 = WorkerConfiguration::create();
    wrkConf3->rpcPort = rpcPortWrk3;
    wrkConf3->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf3->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf3->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation.csv");
    wrkConf3->mobilityConfiguration.pushDeviceLocationUpdates.setValue(false);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(wrkConf3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart3);
    topology->addNewTopologyNodeAsChild(node1, node3);
    locIndex->addMobileNode(node3);

    web::http::http_request msg1(web::http::methods::GET);
    msg1.set_request_uri(web::http::uri("location/allMobile"));

    web::http::http_response httpResponse1;
    web::json::value response1;
    controller->handleGet(std::vector<utility::string_t>{"location", "allMobile"}, msg1);

    msg1.get_response()
        .then([&httpResponse1](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse1 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse1.extract_json()
        .then([&response1](const pplx::task<web::json::value>& task) {
            response1 = task.get();
        })
        .wait();
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
    wrkConf4->nodeSpatialType.setValue(NES::Spatial::Index::Experimental::NodeType::MOBILE_NODE);
    wrkConf4->mobilityConfiguration.locationProviderType.setValue(
        NES::Spatial::Mobility::Experimental::LocationProviderType::CSV);
    wrkConf4->mobilityConfiguration.locationProviderConfig.setValue(std::string(TEST_DATA_DIRECTORY) + "singleLocation2.csv");
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(wrkConf4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ false);
    EXPECT_TRUE(retStart4);
    topology->addNewTopologyNodeAsChild(node1, node4);
    locIndex->addMobileNode(node4);

    web::http::http_request msg2(web::http::methods::GET);
    msg2.set_request_uri(web::http::uri("location/allMobile"));

    web::http::http_response httpResponse2;
    web::json::value response2;
    controller->handleGet(std::vector<utility::string_t>{"location", "allMobile"}, msg2);

    msg2.get_response()
        .then([&httpResponse2](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse2 = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse2.extract_json()
        .then([&response2](const pplx::task<web::json::value>& task) {
            response2 = task.get();
        })
        .wait();
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
}// namespace NES