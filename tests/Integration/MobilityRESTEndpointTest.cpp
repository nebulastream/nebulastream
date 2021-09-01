/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <chrono>
#include <thread>
#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Mobility/LocationService.h>
#include <Util/Logger.hpp>
#include <cpprest/http_client.h>

namespace NES {

static int sleepTime = 100;

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

void EXPECT_EQ_GEOPOINTS(GeoPoint p1, const GeoPointPtr& p2){
    EXPECT_EQ(p1.getLatitude(), p2->getLatitude());
    EXPECT_EQ(p1.getLongitude(), p2->getLongitude());
}

class MobilityRESTEndpointTest : public testing::Test {
  protected:
    NesCoordinatorPtr crd;
    std::string apiEndpoint;

  public:
    static void SetUpTestCase() {
        NES::setupLogging("MobilityRESTEndpointTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup MobilityRESTEndpointTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
        apiEndpoint = "http://127.0.0.1:" + std::to_string(restPort) + "/v1/nes";

        CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
        coordinatorConfig->setRpcPort(rpcPort);
        coordinatorConfig->setRestPort(restPort);

        NES_INFO("MobilityRESTEndpointTest: Start coordinator");
        crd = std::make_shared<NesCoordinator>(coordinatorConfig);
        uint64_t port = crd->startCoordinator(/**blocking**/ false);

        std::this_thread::sleep_for(std::chrono::milliseconds(sleepTime));

        EXPECT_NE(port, 0u);
        NES_INFO("MobilityRESTEndpointTest: Coordinator started successfully");

        crd->getLocationService()->addSink("test_sink", 1000);
        crd->getLocationService()->updateNodeLocation("test_sink", std::make_shared<GeoPoint>(1, 2));
        crd->getLocationService()->addSource("test_source");
        crd->getLocationService()->updateNodeLocation("test_source", std::make_shared<GeoPoint>(3, 4));
    }

    void TearDown() override {
        NES_INFO("MobilityRESTEndpointTest: Stop Coordinator");
        bool retStopCord = crd->stopCoordinator(true);
        EXPECT_TRUE(retStopCord);
        NES_INFO("MobilityRESTEndpointTest: Test finished");
    }

    static void TearDownTestCase() { NES_INFO("Tear down MobilityRESTEndpointTest test class."); }
};

TEST_F(MobilityRESTEndpointTest, testGetGeoNodes) {
    web::json::value getNodesJsonReturn;
    web::http::client::http_client getNodesClient(apiEndpoint);
    web::uri_builder builder("/geo/nodes");
    getNodesClient.request(web::http::methods::GET, builder.to_string())
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        return response.extract_json();
    })
    .then([&getNodesJsonReturn](const pplx::task<web::json::value>& task) {
        try {
            NES_INFO("get nodes: set return");
            getNodesJsonReturn = task.get();
        } catch (const web::http::http_exception& e) {
            NES_ERROR("get nodes: error while setting return" << e.what());
        }
    })
    .wait();

    NES_INFO("get nodes: try to acc return");
    NES_DEBUG("getNodes response: " << getNodesJsonReturn.serialize());
    const auto* expected =
        R"({"sinks":[{"id":"test_sink","latitude":1,"longitude":2}],"sources":[{"enabled":false,"id":"test_source","latitude":3,"longitude":4}]})";
    NES_DEBUG("getNodes response: expected = " << expected);
    ASSERT_EQ(getNodesJsonReturn.serialize(), expected);
}

TEST_F(MobilityRESTEndpointTest, testGetGeoSource) {
    web::json::value getSourceJsonReturn;
    web::http::client::http_client httpClient(apiEndpoint);
    web::uri_builder builder("/geo/source");
    builder.append_query(("nodeId"), "test_source");
    httpClient.request(web::http::methods::GET, builder.to_string())
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        return response.extract_json();
    })
    .then([&getSourceJsonReturn](const pplx::task<web::json::value>& task) {
        try {
            NES_INFO("get source: set return");
            getSourceJsonReturn = task.get();
        } catch (const web::http::http_exception& e) {
            NES_ERROR("get source: error while setting return" << e.what());
        }
    })
    .wait();

    NES_INFO("get source: try to acc return");
    NES_DEBUG("getSource response: " << getSourceJsonReturn.serialize());
    const auto* expected =
        R"({"enabled":false,"id":"test_source","latitude":3,"longitude":4})";
    NES_DEBUG("getSource response: expected = " << expected);
    ASSERT_EQ(getSourceJsonReturn.serialize(), expected);
}

TEST_F(MobilityRESTEndpointTest, testGetGeoSink) {
    web::json::value getSinkJsonReturn;
    web::http::client::http_client httpClient(apiEndpoint);
    web::uri_builder builder(("/geo/sink"));
    builder.append_query(("nodeId"), "test_sink");
    httpClient.request(web::http::methods::GET, builder.to_string())
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        return response.extract_json();
    })
    .then([&getSinkJsonReturn](const pplx::task<web::json::value>& task) {
        try {
            NES_INFO("get source: set return");
            getSinkJsonReturn = task.get();
        } catch (const web::http::http_exception& e) {
            NES_ERROR("get source: error while setting return" << e.what());
        }
    })
    .wait();

    NES_INFO("get source: try to acc return");
    NES_DEBUG("getSource response: " << getSinkJsonReturn.serialize());
    const auto* expected =
        R"({"id":"test_sink","latitude":1,"longitude":2})";
    NES_DEBUG("getSource response: expected = " << expected);
    ASSERT_EQ(getSinkJsonReturn.serialize(), expected);
}

TEST_F(MobilityRESTEndpointTest, testAddGeoSink) {
    web::http::client::http_client httpClient(apiEndpoint);
    web::json::value msg{};
    msg["nodeId"] =  web::json::value::string("new_sink");
    msg["movingRange"] =  web::json::value::number(500);


    httpClient.request(web::http::methods::POST, "/geo/sink", msg)
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        EXPECT_EQ(response.status_code(), 200);
        return response;
    })
    .wait();

    LocationCatalogPtr catalog = crd->getLocationService()->getLocationCatalog();
    EXPECT_TRUE(catalog->contains("new_sink"));
}

TEST_F(MobilityRESTEndpointTest, testAddGeoSources) {
web::http::client::http_client httpClient(apiEndpoint + "/geo/source");
    //    web::uri_builder builder(("/geo/sink"));
    web::json::value msg{};
    msg["nodeId"] =  web::json::value::string("new_source");

    httpClient.request(web::http::methods::POST, "", msg)
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        EXPECT_EQ(response.status_code(), 200);
        return response;
    })
    .wait();

    LocationCatalogPtr catalog = crd->getLocationService()->getLocationCatalog();
    EXPECT_TRUE(catalog->contains("new_source"));
}

TEST_F(MobilityRESTEndpointTest, testAddGeoSourceWithRange) {
    web::http::client::http_client httpClient(apiEndpoint + "/geo/source");
    //    web::uri_builder builder(("/geo/sink"));
    web::json::value msg{};
    msg["nodeId"] =  web::json::value::string("new_source");
    msg["range"] =  web::json::value::number(500);

    httpClient.request(web::http::methods::POST, "", msg)
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        EXPECT_EQ(response.status_code(), 200);
        return response;
    })
    .wait();

    LocationCatalogPtr catalog = crd->getLocationService()->getLocationCatalog();
    EXPECT_TRUE(catalog->contains("new_source"));

    httpClient = web::http::client::http_client(apiEndpoint + "/geo/location");
    msg = {};

    msg["nodeId"] =  web::json::value::string("new_source");
    msg["latitude"] =  web::json::value::number(66);
    msg["longitude"] =  web::json::value::number(77);

    httpClient.request(web::http::methods::POST, "", msg)
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        EXPECT_EQ(response.status_code(), 200);
        return response;
    })
    .wait();

    EXPECT_TRUE(catalog->getSource("new_source")->hasRange());
}

TEST_F(MobilityRESTEndpointTest, testUpdateLocation) {
    LocationCatalogPtr catalog = crd->getLocationService()->getLocationCatalog();

    web::http::client::http_client httpClient(apiEndpoint + "/geo/location");
    web::json::value msg{};

    msg["nodeId"] =  web::json::value::string("test_sink");
    msg["latitude"] =  web::json::value::number(66);
    msg["longitude"] =  web::json::value::number(77);

    httpClient.request(web::http::methods::POST, "", msg)
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        EXPECT_EQ(response.status_code(), 200);
        return response;
    })
    .wait();

    GeoSinkPtr sink = catalog->getSink("test_sink");
    EXPECT_EQ_GEOPOINTS(GeoPoint(66,77), sink->getCurrentLocation());

    msg["nodeId"] =  web::json::value::string("test_source");
    msg["latitude"] =  web::json::value::number(88);
    msg["longitude"] =  web::json::value::number(99);

    httpClient.request(web::http::methods::POST, "", msg)
    .then([](const web::http::http_response& response) {
        NES_INFO("get first then");
        EXPECT_EQ(response.status_code(), 200);
        return response;
    })
    .wait();

    GeoSourcePtr source = catalog->getSource("test_source");
    EXPECT_EQ_GEOPOINTS(GeoPoint(88,99), source->getCurrentLocation());
}

}
