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

#include "SerializableQueryPlan.pb.h"
#include "CoordinatorRPCService.pb.h"
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
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

TEST_F(RESTEndpointTest, testGetExecutionPlanFromWithSingleWorker) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    // as default physical no longer automatically registered, call manually
    // so that it is in the streamCatalog
    wrk1->registerPhysicalStream(PhysicalStreamConfig::createEmpty());

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

TEST_F(RESTEndpointTest, testPostExecuteQueryExWithEmptyQuery) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //make httpclient with new endpoint -ex:
    web::http::client::http_client httpClient("http://127.0.0.1:" + std::to_string(restPort) + "/v1/nes/query/execute-query-ex");

    QueryPlanPtr queryPlan = QueryPlan::create();

    //make a Protobuff object
    SerializableQueryPlan* plan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    //convert it to string for the request function
    SubmitQueryRequest request;
    request.set_querystring("");
    request.set_placement("");
    request.set_allocated_queryplan(plan);

    std::string msg = request.SerializeAsString();

    web::json::value postJsonReturn;
    httpClient.request(web::http::methods::POST, "", msg)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&postJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post execute-query-ex: set return");
                postJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post execute-query-ex: error while setting return" << e.what());
            }
        })
        .wait();

    EXPECT_TRUE(postJsonReturn.has_field("queryId"));

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

TEST_F(RESTEndpointTest, testPostExecuteQueryExWithNonEmptyQuery) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //make httpclient with new endpoint -ex:
    web::http::client::http_client httpClient("http://127.0.0.1:" + std::to_string(restPort) + "/v1/nes/query/execute-query-ex");

    /* REGISTER QUERY */
    SourceConfigPtr sourceConfig;
    sourceConfig = SourceConfig::create();
    sourceConfig->setSourceConfig("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    std::vector<std::string> logStreamNames{"test_stream"};
    sourceConfig->setLogicalStreamName(logStreamNames);

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    streamCatalog->addPhysicalStream(logStreamNames, sce);

    Query query = Query::from("default_logical");
    QueryPlanPtr queryPlan = query.getQueryPlan();

    //make a Protobuff object
    SerializableQueryPlan* plan = QueryPlanSerializationUtil::serializeQueryPlan(queryPlan);
    SubmitQueryRequest request;
    request.set_querystring("default_logical");
    request.set_placement("");
    request.set_allocated_queryplan(plan);

    std::string msg = request.SerializeAsString();

    web::json::value postJsonReturn;
    httpClient.request(web::http::methods::POST, "", msg)
        .then([](const web::http::http_response& response) {
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&postJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post execute-query-ex: set return");
                postJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post execute-query-ex: error while setting return" << e.what());
            }
        })
        .wait();

    EXPECT_TRUE(postJsonReturn.has_field("queryId"));
    EXPECT_TRUE(queryCatalog->queryExists(postJsonReturn.at("queryId").as_integer()));
    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

TEST_F(RESTEndpointTest, testPostExecuteQueryExWrongPayload) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //make httpclient with new endpoint -ex:
    web::http::client::http_client httpClient("http://127.0.0.1:" + std::to_string(restPort) + "/v1/nes/query/execute-query-ex");

    std::string msg = "hello";
    web::json::value postJsonReturn;
    int statusCode;
    httpClient.request(web::http::methods::POST, "", msg)
        .then([&statusCode](const web::http::http_response& response) {
            statusCode = response.status_code();
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&postJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post execute-query-ex: set return");
                postJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post execute-query-ex: error while setting return" << e.what());
            }
        })
        .wait();
    EXPECT_EQ(statusCode, 400);
    EXPECT_TRUE(postJsonReturn.has_field("detail"));
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    // as default physical no longer automatically registered, call manually
    // so that it is in the streamCatalog
    wrk1->registerPhysicalStream(PhysicalStreamConfig::createEmpty());

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

TEST_F(RESTEndpointTest, testConnectivityCheck) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    web::json::value response;
    web::http::client::http_client getConnectivityCheck("http://127.0.0.1:" + std::to_string(restPort)
                                                        + "/v1/nes/connectivity/check");

    web::uri_builder builder(("/"));
    getConnectivityCheck.request(web::http::methods::GET, builder.to_string())
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&response](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("get status of checking connectivity");
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();

    auto connectivityResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(connectivityResponse.size() == 1);
    EXPECT_TRUE(connectivityResponse.find("success") != connectivityResponse.end());
    EXPECT_TRUE(connectivityResponse.at("success").as_bool());

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

//streamCatalog/allLogicalStream?physicalStreamName=ICE2201
TEST_F(RESTEndpointTest, testGetAllLogicalStreams) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    //create test schemas
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = crd->getNesWorker()->registerLogicalStream("testSchema", testSchemaFileName);
    EXPECT_TRUE(success);

    srcConf->setPhysicalStreamName("test_physical");
    srcConf->setLogicalStreamName("testSchema,default_logical");
    success = wrk1->registerPhysicalStream(PhysicalStreamConfig::create(srcConf));
    EXPECT_TRUE(success);

    web::http::client::http_client getAllLogicalStreamsClient("http://127.0.0.1:" + std::to_string(restPort)
                                                              + "/v1/nes/streamCatalog/allLogicalStream?physicalStreamName=test_physical");
    web::json::value getAllLogicalStreamsJsonReturn;

    getAllLogicalStreamsClient.request(web::http::methods::GET, "")
        .then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();})
        .then([&getAllLogicalStreamsJsonReturn](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("get execution-plan: set return");
              getAllLogicalStreamsJsonReturn = task.get();
          } catch (const web::http::http_exception& e) {
              NES_ERROR("get execution-plan: error while setting return" << e.what());
          }})
        .wait();

    NES_INFO("getAllLogicalStreams: try to acc return");
    NES_DEBUG("getAllLogicalStreams response: " << getAllLogicalStreamsJsonReturn.serialize());
    // expected: default_logical and here create testSchema
    auto expected =
        R"({"default_logical":"id:INTEGER value:INTEGER ","testSchema":"id:INTEGER value:INTEGER "})";
    NES_DEBUG("getAllLogicalStreams response: expected = " << expected);
    ASSERT_EQ(getAllLogicalStreamsJsonReturn.serialize(), expected);

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

TEST_F(RESTEndpointTest, testGetAllPhysicalStreams) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    NES_INFO("RESTEndpointTest: Start worker 2");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 20);
    workerConfig->setDataPort(port + 21);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("RESTEndpointTest: Worker2 started successfully");

    srcConf->setPhysicalStreamName("test_physical1");
    srcConf->setLogicalStreamName("default_logical");
    bool success = wrk1->registerPhysicalStream(PhysicalStreamConfig::create(srcConf));
    EXPECT_TRUE(success);

    srcConf->setPhysicalStreamName("test_physical2");
    srcConf->setLogicalStreamName("default_logical");
    success = wrk2->registerPhysicalStream(PhysicalStreamConfig::create(srcConf));
    EXPECT_TRUE(success);

    web::http::client::http_client getAllPhysicalStreamClient("http://127.0.0.1:" + std::to_string(restPort)
                                                              + "/v1/nes/streamCatalog/allPhysicalStream?logicalStreamName=default_logical");
    web::json::value getAllPhysicalStreamJsonReturn;

    getAllPhysicalStreamClient.request(web::http::methods::GET, "")
        .then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();})
        .then([&getAllPhysicalStreamJsonReturn](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("get execution-plan: set return");
              getAllPhysicalStreamJsonReturn = task.get();
          } catch (const web::http::http_exception& e) {
              NES_ERROR("get execution-plan: error while setting return" << e.what());
          }})
        .wait();

    NES_INFO("allPhysicalStream: try to acc return");
    NES_DEBUG("allPhysicalStream response: " << getAllPhysicalStreamJsonReturn.serialize());
    std::string expected= "{\"Physical Streams\":[\"physicalName=test_physical1 logicalStreamName=(default_logical) sourceType=DefaultSource on node="+std::to_string(wrk1->getWorkerId())+"\",\"physicalName=test_physical2 logicalStreamName=(default_logical) sourceType=DefaultSource on node="+std::to_string(wrk2->getWorkerId())+"\"]}";    NES_DEBUG("allPhysicalStream response: expected = " << expected);
    ASSERT_EQ(getAllPhysicalStreamJsonReturn.serialize(), expected);

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("RESTEndpointTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

TEST_F(RESTEndpointTest, testAddPhysicalToLogicalStream) {
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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    srcConf->setPhysicalStreamName("test_physical");

    wrk1->registerPhysicalStream(PhysicalStreamConfig::create(srcConf));

    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = crd->getNesWorker()->registerLogicalStream("testSchema", testSchemaFileName);
    EXPECT_TRUE(success);

    RegisterPhysicalStreamRequest req;
    req.set_physicalstreamname("test_physical");
    req.set_logicalstreamname("testSchema");

   std::string msg = req.SerializeAsString();

    web::http::client::http_client addPhysicalToLogicalStreamClient("http://127.0.0.1:" + std::to_string(restPort)
                                                                   + "/v1/nes/streamCatalog/addPhysicalToLogicalStream");
    web::json::value addJsonReturn;
    addPhysicalToLogicalStreamClient.request(web::http::methods::POST, "", msg)
        .then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&addJsonReturn](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("addPhysicalToLogical: set return");
              addJsonReturn = task.get();
          } catch (const web::http::http_exception& e) {
              NES_ERROR("addPhysicalToLogical: error while setting return" << e.what());
          }
        })
        .wait();

    NES_INFO("addPhysicalToLogical: try to acc return");
    NES_DEBUG("addPhysicalToLogical response: " << addJsonReturn.serialize());

    auto vec = crd->getStreamCatalog()->getPhysicalStreams("testSchema");
    EXPECT_TRUE(!vec.empty());

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}

TEST_F(RESTEndpointTest, testRemoveLogicalStreamFromPhysicalStream) {
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

    //create two test schemas
    std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
                             "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName1 = "testSchema1.hpp";
    std::ofstream out1(testSchemaFileName1);
    out1 << testSchema;
    out1.close();

    std::string testSchemaFileName2 = "testSchema2.hpp";
    std::ofstream out2(testSchemaFileName2);
    out2 << testSchema;
    out2.close();

    bool success = crd->getNesWorker()->registerLogicalStream("testSchema1", testSchemaFileName1);
    EXPECT_TRUE(success);
    success = crd->getNesWorker()->registerLogicalStream("testSchema2", testSchemaFileName2);
    EXPECT_TRUE(success);

    //create worker with physical stream which registers for both
    NES_INFO("RESTEndpointTest: Start worker 1");
    workerConfig->setCoordinatorPort(port);
    workerConfig->setRpcPort(port + 10);
    workerConfig->setDataPort(port + 11);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(workerConfig, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("RESTEndpointTest: Worker1 started successfully");

    std::vector<std::string> logicalStreamNames{"testSchema1", "testSchema2"};
    srcConf->setLogicalStreamName(logicalStreamNames);
    srcConf->setPhysicalStreamName("test_physical");
    wrk1->registerPhysicalStream(PhysicalStreamConfig::create(srcConf));

    // now two logical to physical stream mapping entries should exist
    auto vec1 = crd->getStreamCatalog()->getPhysicalStreams("testSchema1");
    auto vec2 = crd->getStreamCatalog()->getPhysicalStreams("testSchema2");
    EXPECT_TRUE(!vec1.empty() && !vec2.empty());

    web::http::client::http_client removeLogicalFromPhysicalClient("http://127.0.0.1:" + std::to_string(restPort)
                                                              + "/v1/nes/streamCatalog/removePhysicalStreamFromLogicalStream"
                                                                     "?logicalStreamName=testSchema1&physicalStreamName=test_physical");
    web::json::value delJsonReturn;
    removeLogicalFromPhysicalClient.request(web::http::methods::DEL)
        .then([](const web::http::http_response& response) {
          NES_INFO("get first then");
          return response.extract_json();
        })
        .then([&delJsonReturn](const pplx::task<web::json::value>& task) {
          try {
              NES_INFO("del remove from physical a logical: set return");
              delJsonReturn = task.get();
          } catch (const web::http::http_exception& e) {
              NES_ERROR("del remove from physical a logical: error while setting return" << e.what());
          }
        })
        .wait();

    NES_INFO("removePhysicalStreamFromLogicalStream: try to acc return");
    NES_DEBUG("removePhysicalStreamFromLogicalStream response: " << delJsonReturn.serialize());

    // expected: testSchema1 deleted -> empty, testSchema2 -> still one entry
    vec1 = crd->getStreamCatalog()->getPhysicalStreams("testSchema1");
    vec2 = crd->getStreamCatalog()->getPhysicalStreams("testSchema2");
    EXPECT_TRUE(vec1.empty() && !vec2.empty());

    NES_INFO("RESTEndpointTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);
    NES_INFO("RESTEndpointTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("RESTEndpointTest: Test finished");
}


}// namespace NES
