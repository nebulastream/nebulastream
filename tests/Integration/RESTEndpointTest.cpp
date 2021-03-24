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

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/http_client.h>
#include <iostream>

namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class RESTEndpointTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("RESTEndpointTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup RESTEndpointTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    static void TearDownTestCase() { NES_INFO("Tear down RESTEndpointTest test class."); }
};

TEST_F(RESTEndpointTest, DISABLED_testGetExecutionPlanFromWithSingleWorker) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("RESTEndpointTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("RESTEndpointTest: Coordinator started successfully");

    NES_INFO("RESTEndpointTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("RESTEndpointTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    // get the execution plan
    std::stringstream getExecutionPlanStringStream;
    getExecutionPlanStringStream << "{\"queryId\" : ";
    getExecutionPlanStringStream << queryId;
    getExecutionPlanStringStream << "}";
    getExecutionPlanStringStream << endl;

    NES_INFO("get execution plan request body=" << getExecutionPlanStringStream.str());
    string getExecutionPlanRequestBody = getExecutionPlanStringStream.str();
    web::json::value getExecutionPlanJsonReturn;

    web::http::client::http_client getExecutionPlanClient("http://127.0.0.1:" + std::to_string(restPort)
                                                          + "/v1/nes/query/execution-plan");
    web::uri_builder builder(("/"));
    builder.append_query(("queryId"), queryId);
    getExecutionPlanClient.request(web::http::methods::GET, builder.to_string())
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&getExecutionPlanJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("get execution-plan: set return");
                getExecutionPlanJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("get execution-plan: error while setting return" << e.what());
            }
        })
        .wait();

    NES_INFO("get execution-plan: try to acc return");
    NES_DEBUG("getExecutionPlan response: " << getExecutionPlanJsonReturn.serialize());
    auto expected =
        R"({"executionNodes":[{"ScheduledQueries":[{"queryId":1,"querySubPlans":[{"operator":"SINK(4)\n  SOURCE(1,default_logical)\n","querySubPlanId":1}]}],"executionNodeId":2,"topologyNodeId":2,"topologyNodeIpAddress":"127.0.0.1"},{"ScheduledQueries":[{"queryId":1,"querySubPlans":[{"operator":"SINK(2)\n  SOURCE(3,)\n","querySubPlanId":2}]}],"executionNodeId":1,"topologyNodeId":1,"topologyNodeIpAddress":"127.0.0.1"}]})";
    NES_DEBUG("getExecutionPlan response: expected = " << expected);
    ASSERT_EQ(getExecutionPlanJsonReturn.serialize(), expected);

    NES_INFO("RESTEndpointTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

TEST_F(RESTEndpointTest, testGetAllRegisteredQueries) {
    CoordinatorConfigPtr coordinatorConfig = CoordinatorConfig::create();
    WorkerConfigPtr workerConfig = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    coordinatorConfig->setRpcPort(rpcPort);
    coordinatorConfig->setRestPort(restPort);
    workerConfig->setCoordinatorPort(rpcPort);

    NES_INFO("RESTEndpointTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("RESTEndpointTest: Coordinator started successfully");

    NES_INFO("RESTEndpointTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("RESTEndpointTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    // get the execution plan
    web::json::value response;
    web::http::client::http_client getExecutionPlanClient("http://127.0.0.1:" + std::to_string(restPort)
                                                          + "/v1/nes/queryCatalog/allRegisteredQueries");
    web::uri_builder builder(("/"));
    getExecutionPlanClient.request(web::http::methods::GET, builder.to_string())
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&response](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("get all registered queries: set return");
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();

    //Assertions
    auto catalogEntries = response.as_array();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(catalogEntries.size() == 1);
    EXPECT_TRUE(catalogEntries.at(0).has_number_field("queryId"));
    EXPECT_TRUE(catalogEntries.at(0).has_object_field("queryPlan"));
    EXPECT_TRUE(catalogEntries.at(0).has_string_field("queryStatus"));
    EXPECT_TRUE(catalogEntries.at(0).has_string_field("queryString"));

    NES_INFO("RESTEndpointTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}
}// namespace NES
