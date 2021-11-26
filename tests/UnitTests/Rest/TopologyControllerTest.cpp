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

#include <gtest/gtest.h>

#include <REST/Controller/TopologyController.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/http_client.h>
#include <memory>
#include <utility>

namespace NES {

class TopologyControllerTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("TopologyControllerTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down TopologyControllerTest test class."); }
};

TEST_F(TopologyControllerTest, testAddParentTopologyWhenChildParentAreEqual) {
    TopologyPtr topology = Topology::create();
    TopologyControllerPtr topologyController = std::make_shared<TopologyController>(topology);

    uint64_t parentId = 2;
    uint64_t childId = 2;
    std::string body = R"({"parentId":")" + std::to_string(parentId) + R"(", "childId":")" + std::to_string(childId) + "\"}";

    web::http::http_request msg(web::http::methods::POST);
    msg.set_body(body, "text/plain; charset=utf-8");

    web::http::http_response httpResponse;
    web::json::value response;

    topologyController->handlePost(std::vector<utility::string_t>{"topology", "addParent"}, msg);
    msg.get_response()
        .then([&httpResponse](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse.extract_json()
        .then([&response](const pplx::task<web::json::value>& task) {
            response = task.get();
        })
        .wait();
    auto addParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(addParentResponse.size() == 1);
    EXPECT_TRUE(addParentResponse.find("message") != addParentResponse.end());
    EXPECT_EQ(addParentResponse.at("message").as_string(),
              "Could not add parent for node in topology: childId and parentId must be different.");
}

TEST_F(TopologyControllerTest, testAddParentTopologyWhenChildIsAbsent) {
    TopologyPtr topology = Topology::create();
    TopologyControllerPtr topologyController = std::make_shared<TopologyController>(topology);

    uint64_t parentId = 2;
    uint64_t childId = 3;

    auto parentNode = TopologyNode::create(parentId, "192.168.1.1", 128, 129, 1000);

    topology->setAsRoot(parentNode);

    std::string body = R"({"parentId":")" + std::to_string(parentId) + R"(", "childId":")" + std::to_string(childId) + "\"}";

    web::http::http_request msg(web::http::methods::POST);
    msg.set_body(body, "text/plain; charset=utf-8");

    web::http::http_response httpResponse;
    web::json::value response;

    topologyController->handlePost(std::vector<utility::string_t>{"topology", "addParent"}, msg);
    msg.get_response()
        .then([&httpResponse](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse.extract_json()
        .then([&response](const pplx::task<web::json::value>& task) {
            response = task.get();
        })
        .wait();
    auto addParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(addParentResponse.size() == 1);
    EXPECT_TRUE(addParentResponse.find("message") != addParentResponse.end());
    EXPECT_EQ(addParentResponse.at("message").as_string(),
              "Could not add parent for node in topology: Node with childId=3 not found.");
}

TEST_F(TopologyControllerTest, testAddParentTopologyWhenParentIsAbsent) {
    TopologyPtr topology = Topology::create();
    TopologyControllerPtr topologyController = std::make_shared<TopologyController>(topology);

    uint64_t parentId = 2;
    uint64_t childId = 3;

    auto childNode = TopologyNode::create(childId, "192.168.1.1", 128, 129, 1000);

    topology->setAsRoot(childNode);

    std::string body = R"({"parentId":")" + std::to_string(parentId) + R"(", "childId":")" + std::to_string(childId) + "\"}";

    web::http::http_request msg(web::http::methods::POST);
    msg.set_body(body, "text/plain; charset=utf-8");

    web::http::http_response httpResponse;
    web::json::value response;

    topologyController->handlePost(std::vector<utility::string_t>{"topology", "addParent"}, msg);
    msg.get_response()
        .then([&httpResponse](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse.extract_json()
        .then([&response](const pplx::task<web::json::value>& task) {
            response = task.get();
        })
        .wait();
    auto addParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(addParentResponse.size() == 1);
    EXPECT_TRUE(addParentResponse.find("message") != addParentResponse.end());
    EXPECT_EQ(addParentResponse.at("message").as_string(),
              "Could not add parent for node in topology: Node with parentId=2 not found.");
}

TEST_F(TopologyControllerTest, testRemoveParentTopologyWhenChildParentAreEqual) {
    TopologyPtr topology = Topology::create();
    TopologyControllerPtr topologyController = std::make_shared<TopologyController>(topology);

    uint64_t parentId = 2;
    uint64_t childId = 2;
    std::string body = R"({"parentId":")" + std::to_string(parentId) + R"(", "childId":")" + std::to_string(childId) + "\"}";

    web::http::http_request msg(web::http::methods::POST);
    msg.set_body(body, "text/plain; charset=utf-8");

    web::http::http_response httpResponse;
    web::json::value response;

    topologyController->handlePost(std::vector<utility::string_t>{"topology", "removeParent"}, msg);
    msg.get_response()
        .then([&httpResponse](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse.extract_json()
        .then([&response](const pplx::task<web::json::value>& task) {
            response = task.get();
        })
        .wait();
    auto removeParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(removeParentResponse.size() == 1);
    EXPECT_TRUE(removeParentResponse.find("message") != removeParentResponse.end());
    EXPECT_EQ(removeParentResponse.at("message").as_string(),
              "Could not remove parent for node in topology: childId and parentId must be different.");
}

TEST_F(TopologyControllerTest, testRemoveParentTopologyWhenChildIsAbsent) {
    TopologyPtr topology = Topology::create();
    TopologyControllerPtr topologyController = std::make_shared<TopologyController>(topology);

    uint64_t parentId = 2;
    uint64_t childId = 3;

    auto parentNode = TopologyNode::create(parentId, "192.168.1.1", 128, 129, 1000);

    topology->setAsRoot(parentNode);

    std::string body = R"({"parentId":")" + std::to_string(parentId) + R"(", "childId":")" + std::to_string(childId) + "\"}";

    web::http::http_request msg(web::http::methods::POST);
    msg.set_body(body, "text/plain; charset=utf-8");

    web::http::http_response httpResponse;
    web::json::value response;

    topologyController->handlePost(std::vector<utility::string_t>{"topology", "removeParent"}, msg);
    msg.get_response()
        .then([&httpResponse](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse.extract_json()
        .then([&response](const pplx::task<web::json::value>& task) {
            response = task.get();
        })
        .wait();
    auto removeParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(removeParentResponse.size() == 1);
    EXPECT_TRUE(removeParentResponse.find("message") != removeParentResponse.end());
    EXPECT_EQ(removeParentResponse.at("message").as_string(),
              "Could not remove parent for node in topology: Node with childId=3 not found.");
}

TEST_F(TopologyControllerTest, testRemoveParentTopologyWhenParentIsAbsent) {
    TopologyPtr topology = Topology::create();
    TopologyControllerPtr topologyController = std::make_shared<TopologyController>(topology);

    uint64_t parentId = 2;
    uint64_t childId = 3;

    auto childNode = TopologyNode::create(childId, "192.168.1.1", 128, 129, 1000);

    topology->setAsRoot(childNode);

    std::string body = R"({"parentId":")" + std::to_string(parentId) + R"(", "childId":")" + std::to_string(childId) + "\"}";

    web::http::http_request msg(web::http::methods::POST);
    msg.set_body(body, "text/plain; charset=utf-8");

    web::http::http_response httpResponse;
    web::json::value response;

    topologyController->handlePost(std::vector<utility::string_t>{"topology", "removeParent"}, msg);
    msg.get_response()
        .then([&httpResponse](const pplx::task<web::http::http_response>& task) {
            try {
                httpResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    httpResponse.extract_json()
        .then([&response](const pplx::task<web::json::value>& task) {
            response = task.get();
        })
        .wait();
    auto removeParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(removeParentResponse.size() == 1);
    EXPECT_TRUE(removeParentResponse.find("message") != removeParentResponse.end());
    EXPECT_EQ(removeParentResponse.at("message").as_string(),
              "Could not remove parent for node in topology: Node with parentId=2 not found.");
}
}// namespace NES
