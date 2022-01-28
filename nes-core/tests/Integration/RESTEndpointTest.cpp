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

#include <Util/TestHarness/TestHarness.hpp>
#include "../util/ProtobufMessageFactory.hpp"
#include "SerializableQueryPlan.pb.h"
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Plans/Query/QueryId.hpp>
#include <REST/Controller/UdfCatalogController.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpprest/http_client.h>
#include <iostream>

using namespace std::string_literals;

namespace NES {

using namespace Configurations;

class RESTEndpointTest : public testing::Test {
  protected:
    static void SetUpTestCase() {
        NES::setupLogging("RESTEndpointTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup RESTEndpointTest test class.");
    }

    void TearDown() override { NES_INFO("RESTEndpointTest: Test finished"); }

    static void TearDownTestCase() { NES_INFO("Tear down RESTEndpointTest test class."); }

    RESTEndpointTest() : coordinatorRpcPort(getNextFreePort()), coordinatorRestPort(getNextFreePort()) {}

    NesCoordinatorPtr createAndStartCoordinator() const {
        NES_INFO("RESTEndpointTest: Start coordinator");
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->setRpcPort(coordinatorRpcPort);
        coordinatorConfig->setRestPort(coordinatorRestPort);
        auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        EXPECT_EQ(coordinator->startCoordinator(false), coordinatorRpcPort);
        NES_INFO("RESTEndpointTest: Coordinator started successfully");
        return coordinator;
    }

    static void stopCoordinator(NesCoordinator& coordinator) {
        NES_INFO("RESTEndpointTest: Stop Coordinator");
        EXPECT_TRUE(coordinator.stopCoordinator(true));
    }

    NesWorkerPtr createAndStartWorkerWithSourceConfig(uint8_t id, PhysicalSourcePtr sourceConfig) {
        NES_INFO("RESTEndpointTest: Start worker " << id);
        WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
        workerConfig->setCoordinatorPort(coordinatorRpcPort);
        workerConfig->setRpcPort(getNextFreePort());
        workerConfig->setDataPort(getNextFreePort());
        workerConfig->addPhysicalSource(sourceConfig);
        NesWorkerPtr worker = std::make_shared<NesWorker>(workerConfig);
        EXPECT_TRUE(worker->start(/**blocking**/ false, /**withConnect**/ true));
        NES_INFO("RESTEndpointTest: Worker " << id << " started successfully");
        return worker;
    }

    // The id parameter is just used to recreate the log output before refactoring.
    // The test code also works if the id parameter is omitted.
    NesWorkerPtr createAndStartWorker(uint8_t id = 1) {
        NES_INFO("RESTEndpointTest: Start worker " << id);
        WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
        workerConfig->setCoordinatorPort(coordinatorRpcPort);
        workerConfig->setRpcPort(getNextFreePort());
        workerConfig->setDataPort(getNextFreePort());
        NesWorkerPtr worker = std::make_shared<NesWorker>(workerConfig);
        EXPECT_TRUE(worker->start(/**blocking**/ false, /**withConnect**/ true));
        NES_INFO("RESTEndpointTest: Worker " << id << " started successfully");
        return worker;
    }

    static void stopWorker(NesWorker& worker, uint8_t id = 1) {
        NES_INFO("RESTEndpointTest: Stop worker " << id);
        EXPECT_TRUE(worker.stop(true));
    }

    [[nodiscard]] web::http::client::http_client createRestClient(const std::string& restEndpoint) const {
        auto url = "http://127.0.0.1:" + std::to_string(coordinatorRestPort) + "/v1/nes/" + restEndpoint;
        return web::http::client::http_client{url};
    }

    static uint64_t getNextFreePort() {
        // Skip 3 ports because some ports are created by default and not handled by this test.
        // TODO: Consolidate assignment of ports with #2007
        nextFreePort += 3;
        return nextFreePort;
    }

  private:
    static uint64_t nextFreePort;
    uint64_t coordinatorRpcPort;
    uint64_t coordinatorRestPort;
};

uint64_t RESTEndpointTest::nextFreePort = 1024;

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testGetExecutionPlanFromWithSingleWorker) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    auto getExecutionPlanClient = createRestClient("query/execution-plan");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("RESTEndpointTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
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
    const auto* expected =
        R"({"executionNodes":[{"ScheduledQueries":[{"queryId":1,"querySubPlans":[{"operator":"SINK(4)\n  SOURCE(1,default_logical)\n","querySubPlanId":1}]}],"executionNodeId":2,"topologyId":2,"topologyNodeIpAddress":"127.0.0.1"},{"ScheduledQueries":[{"queryId":1,"querySubPlans":[{"operator":"SINK(2)\n  SOURCE(3,)\n","querySubPlanId":2}]}],"executionNodeId":1,"topologyId":1,"topologyNodeIpAddress":"127.0.0.1"}]})";
    NES_DEBUG("getExecutionPlan response: expected = " << expected);
    ASSERT_EQ(getExecutionPlanJsonReturn.serialize(), expected);

    NES_INFO("RESTEndpointTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testPostExecuteQueryExWithEmptyQuery) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    //make httpclient with new endpoint -ex:
    auto httpClient = createRestClient("query/execute-query-ex");

    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    //make a Protobuff object
    SubmitQueryRequest request;
    auto serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    //convert it to string for the request function
    request.set_querystring("");

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

    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testPostExecuteQueryExWithNonEmptyQuery) {
    auto crd = createAndStartCoordinator();
    //make httpclient with new endpoint -ex:
    auto httpClient = createRestClient("query/execute-query-ex");

    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    /* REGISTER QUERY */
    CSVSourceTypePtr sourceConfig;
    sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    auto windowStream = PhysicalSource::create("test_stream", "test2", sourceConfig);
    auto wrk1 = createAndStartWorkerWithSourceConfig(1, windowStream);
    // Removed this and replaced it by the above. Test is disabled, cannot check correctness. Leaving this for future fixing.
    //    PhysicalSourcePtr conf = PhysicalSourceType::create(sourceConfig);
    //    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(conf, physicalNode);
    //    SourceCatalogPtr sourceCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    //    sourceCatalog->addPhysicalSource("default_logical", sce);

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    Query query = Query::from("default_logical");
    QueryPlanPtr queryPlan = query.getQueryPlan();

    //make a Protobuff object
    SubmitQueryRequest request;
    auto serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan, true);
    request.set_querystring("default_logical");
    auto& context = *request.mutable_context();

    auto bottomUpPlacement = google::protobuf::Any();
    bottomUpPlacement.set_value("BottomUp");
    context["placement"] = bottomUpPlacement;

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

    EXPECT_TRUE(postJsonReturn.has_field("queryId"));
    EXPECT_TRUE(crd->getQueryCatalog()->queryExists(postJsonReturn.at("queryId").as_integer()));

    auto insertedQueryPlan =
        crd->getQueryCatalog()->getQueryCatalogEntry(postJsonReturn.at("queryId").as_integer())->getInputQueryPlan();
    // Expect that the query id and query sub plan id from the deserialized query plan are valid
    EXPECT_FALSE(insertedQueryPlan->getQueryId() == INVALID_QUERY_ID);
    EXPECT_FALSE(insertedQueryPlan->getQuerySubPlanId() == INVALID_QUERY_SUB_PLAN_ID);
    // Since the deserialization acquires the next queryId and querySubPlanId from the PlanIdGenerator, the deserialized Id should not be the same with the original Id
    EXPECT_TRUE(insertedQueryPlan->getQueryId() != queryPlan->getQueryId());
    EXPECT_TRUE(insertedQueryPlan->getQuerySubPlanId() != queryPlan->getQuerySubPlanId());

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testPostExecuteQueryExWrongPayload) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    //make httpclient with new endpoint -ex:
    auto httpClient = createRestClient("query/execute-query-ex");

    std::string msg = "hello";
    web::json::value postJsonReturn;
    int statusCode = 0;
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

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testGetAllRegisteredQueries) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    auto getExecutionPlanClient = createRestClient("queryCatalog/allRegisteredQueries");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_INFO("RESTEndpointTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    QueryId queryId =
        queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    // get the execution plan
    web::json::value response;
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

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testAddParentTopology) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    auto wrk2 = createAndStartWorker(2);
    auto addParent = createRestClient("topology/addParent");

    uint64_t parentId = wrk2->getWorkerId();
    uint64_t childId = wrk1->getWorkerId();

    auto parent = crd->getTopology()->findNodeWithId(parentId);
    auto child = crd->getTopology()->findNodeWithId(childId);

    ASSERT_FALSE(child->containAsParent(parent));

    web::json::value response;
    std::string msg = "{\"parentId\":\"" + std::to_string(parentId) + "\", \"childId\":\"" + std::to_string(childId) + "\"}";
    addParent.request(web::http::methods::POST, "", msg)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&response](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("get status of adding parent");
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();

    auto addParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(addParentResponse.size() == 1);
    EXPECT_TRUE(addParentResponse.find("Success") != addParentResponse.end());
    EXPECT_TRUE(addParentResponse.at("Success").as_bool());

    ASSERT_TRUE(child->containAsParent(parent));

    stopWorker(*wrk1);
    stopWorker(*wrk2, 2);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testRemoveParentTopology) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    auto removeParent = createRestClient("topology/removeParent");

    uint64_t parentId = crd->getNesWorker()->getWorkerId();
    uint64_t childId = wrk1->getWorkerId();

    auto parent = crd->getTopology()->findNodeWithId(parentId);
    auto child = crd->getTopology()->findNodeWithId(childId);

    ASSERT_TRUE(child->containAsParent(parent));

    web::json::value response;
    std::string msg = "{\"parentId\":\"" + std::to_string(parentId) + "\", \"childId\":\"" + std::to_string(childId) + "\"}";
    removeParent.request(web::http::methods::POST, "", msg)
        .then([](const web::http::http_response& response) {
            return response.extract_json();
        })
        .then([&response](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("get status of removing parent");
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();

    auto removeParentResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(removeParentResponse.size() == 1);
    EXPECT_TRUE(removeParentResponse.find("Success") != removeParentResponse.end());
    EXPECT_TRUE(removeParentResponse.at("Success").as_bool());

    ASSERT_FALSE(child->containAsParent(parent));

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testConnectivityCheck) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    auto getConnectivityCheck = createRestClient("connectivity/check");

    web::json::value response;

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

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testAddLogicalStreamEx) {
    auto crd = createAndStartCoordinator();
    //make httpclient with new endpoint -ex:
    auto httpClient = createRestClient("sourceCatalog/addLogicalStream-ex");

    SourceCatalogPtr streamCatalog = crd->getStreamCatalog();

    //create message as Protobuf encoded object
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    SerializableSchemaPtr serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    SerializableNamedSchema request;
    request.set_streamname("test");
    request.set_allocated_schema(serializableSchema.get());
    std::string msg = request.SerializeAsString();
    request.release_schema();

    web::json::value postJsonReturn;
    int statusCode = 0;
    httpClient.request(web::http::methods::POST, "", msg)
        .then([&statusCode](const web::http::http_response& response) {
            statusCode = response.status_code();
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&postJsonReturn](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post addLogicalStream-ex: set return");
                postJsonReturn = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post addLogicalStream-ex: error while setting return" << e.what());
            }
        })
        .wait();

    EXPECT_EQ(statusCode, 200);
    EXPECT_TRUE(postJsonReturn.has_field("Success"));
    EXPECT_EQ(streamCatalog->getAllLogicalStreamAsString().size(), 3U);

    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_DelegatePostRequestToRegisterUdf) {
    auto coordinator = createAndStartCoordinator();
    // when a REST client tries to register a Java UDF
    auto udfName = "my_udf"s;
    auto javaUdfRequest = ProtobufMessageFactory::createRegisterJavaUdfRequest(udfName,
                                                                               "some_package.my_udf_class",
                                                                               "udf_method",
                                                                               {1},
                                                                               {{"some_package.my_udf_class", {1}}});
    auto restClient = createRestClient(UdfCatalogController::path_prefix);
    auto request = restClient.request(web::http::methods::POST, "registerJavaUdf", javaUdfRequest.SerializeAsString());
    request.wait();
    // then the Java UDF is stored in the UDF catalog of the coordinator
    auto udfCatalog = coordinator->getUdfCatalog();
    ASSERT_NE(udfCatalog->getUdfDescriptor(udfName), nullptr);
    stopCoordinator(*coordinator);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_DelegateDeleteRequestToRemoveUdf) {
    auto coordinator = createAndStartCoordinator();
    // given the udfCatalog contains a Java UDF
    auto javaUdfDescriptor =
        JavaUdfDescriptor::create("some_package.my_udf_class", "udf_method", {1}, {{"some_package.my_udf_class", {1}}});
    auto udfCatalog = coordinator->getUdfCatalog();
    auto udfName = "my_udf"s;
    udfCatalog->registerJavaUdf(udfName, javaUdfDescriptor);
    // when a REST client tries to remove the Java UDF
    auto restClient = createRestClient(UdfCatalogController::path_prefix);
    auto request = restClient.request(web::http::methods::DEL, "removeUdf?udfName="s + udfName);
    request.wait();
    // then the Java UDF is no longer stored in the UDF catalog
    ASSERT_EQ(udfCatalog->getUdfDescriptor(udfName), nullptr);
    stopCoordinator(*coordinator);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_DelegateGetRequestToRetrieveUdfDescriptor) {
    auto coordinator = createAndStartCoordinator();
    // given the udfCatalog contains a Java UDF
    auto javaUdfDescriptor =
        JavaUdfDescriptor::create("some_package.my_udf_class", "udf_method", {1}, {{"some_package.my_udf_class", {1}}});
    auto udfCatalog = coordinator->getUdfCatalog();
    auto udfName = "my_udf"s;
    udfCatalog->registerJavaUdf(udfName, javaUdfDescriptor);
    // when a REST client tries to remove the Java UDF
    auto restClient = createRestClient(UdfCatalogController::path_prefix);
    auto request = restClient.request(web::http::methods::GET, "getUdfDescriptor?udfName="s + udfName);
    // then the response contains the Java UDF
    GetJavaUdfDescriptorResponse response;
    request
        .then([&](const web::http::http_response& http_response) {
            return http_response.extract_string(true);
        })
        .then([&response](const pplx::task<std::string>& task) {
            response.ParseFromString(task.get());
        })
        .wait();
    ASSERT_TRUE(response.found());
    // Skip verifying the remaining contents of the response, that is already done in UdfCatalogControllerTest.
    stopCoordinator(*coordinator);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_MaintenanceServiceTest) {
    auto coordinator = createAndStartCoordinator();
    auto restClient = createRestClient("maintenance/mark");
    web::json::value missingIDRequest;
    missingIDRequest["migrationType"] = web::json::value::string("random");
    web::json::value missingIDResponse;
    int missingIDStatusCode = 0;
    restClient.request(web::http::methods::POST, "maintenance/mark",missingIDRequest)
        .then([&missingIDStatusCode](const web::http::http_response& response) {
            missingIDStatusCode = response.status_code();
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&missingIDResponse](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post addLogicalStream-ex: set return");
                missingIDResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post addLogicalStream-ex: error while setting return" << e.what());
            }
        })
        .wait();
    EXPECT_EQ(missingIDStatusCode, 400);
    EXPECT_TRUE(missingIDResponse.has_field("detail"));
    EXPECT_EQ(missingIDResponse["detail"].as_string(), "Field id must be provided");



    web::json::value missingMigrationTypeRequest;
    missingMigrationTypeRequest["id"] = web::json::value::number(1);
    web::json::value missingMigrationTypeResponse;
    int missingMigrationTypeStatusCode = 0;
    restClient.request(web::http::methods::POST, "maintenance/mark",missingMigrationTypeRequest)
        .then([&missingMigrationTypeStatusCode](const web::http::http_response& response) {
            missingMigrationTypeStatusCode = response.status_code();
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&missingMigrationTypeResponse](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post addLogicalStream-ex: set return");
                missingMigrationTypeResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post addLogicalStream-ex: error while setting return" << e.what());
            }
        })
        .wait();
    EXPECT_EQ(missingMigrationTypeStatusCode, 400);
    EXPECT_TRUE(missingMigrationTypeResponse.has_field("detail"));
    EXPECT_EQ(missingMigrationTypeResponse["detail"].as_string(), "Field migrationType must be provided");



    web::json::value validRequest;
    validRequest["id"] = web::json::value::number(1);
    validRequest["migrationType"] = web::json::value::string("restart");
    web::json::value validRequestResponse;
    int validRequestStatusCode = 0;
    restClient.request(web::http::methods::POST, "maintenance/mark", validRequest)
        .then([&validRequestStatusCode](const web::http::http_response& response) {
            validRequestStatusCode = response.status_code();
            NES_INFO("get first then");
            return response.extract_json();
        })
        .then([&validRequestResponse](const pplx::task<web::json::value>& task) {
            try {
                NES_INFO("post addLogicalStream-ex: set return");
                validRequestResponse = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("post addLogicalStream-ex: error while setting return" << e.what());
            }
        })
        .wait();
    EXPECT_EQ(validRequestStatusCode, 200);
    EXPECT_TRUE(validRequestResponse.has_field("Info"));
    EXPECT_TRUE(validRequestResponse.has_field("Node Id"));
    EXPECT_TRUE(validRequestResponse.has_field("Migration Type"));
    EXPECT_EQ(validRequestResponse["Info"].as_string(), "Successfully submitted Maintenance Request");
    EXPECT_EQ(validRequestResponse["Node Id"].as_integer(), 1);
    EXPECT_EQ(validRequestResponse["Migration Type"].as_string(), "restart");

    stopCoordinator(*coordinator);
}
}// namespace NES
