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

#include "SerializableQueryPlan.pb.h"
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/Identifiers.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <GRPC/Serialization/QueryPlanSerializationUtil.hpp>
#include <GRPC/Serialization/SchemaSerializationUtil.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <REST/Controller/UdfCatalogController.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/ProtobufMessageFactory.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>
#include <nlohmann/json.hpp>

using namespace std::string_literals;

namespace NES {

using namespace Configurations;

class RESTEndpointTest : public Testing::NESBaseTest {
  protected:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("RESTEndpointTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup RESTEndpointTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down RESTEndpointTest test class."); }

    NesCoordinatorPtr createAndStartCoordinator() const {
        NES_INFO("RESTEndpointTest: Start coordinator");
        CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;

        auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
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
        workerConfig->coordinatorPort = *rpcCoordinatorPort;
        workerConfig->physicalSources.add(sourceConfig);
        NesWorkerPtr worker = std::make_shared<NesWorker>(std::move(workerConfig));
        EXPECT_TRUE(worker->start(/**blocking**/ false, /**withConnect**/ true));
        NES_INFO("RESTEndpointTest: Worker " << id << " started successfully");
        return worker;
    }

    // The id parameter is just used to recreate the log output before refactoring.
    // The test code also works if the id parameter is omitted.
    NesWorkerPtr createAndStartWorker(uint8_t id = 1) {
        NES_INFO("RESTEndpointTest: Start worker " << id);
        WorkerConfigurationPtr workerConfig = WorkerConfiguration::create();
        workerConfig->coordinatorPort = *rpcCoordinatorPort;
        NesWorkerPtr worker = std::make_shared<NesWorker>(std::move(workerConfig));
        EXPECT_TRUE(worker->start(/**blocking**/ false, /**withConnect**/ true));
        NES_INFO("RESTEndpointTest: Worker " << id << " started successfully");
        return worker;
    }

    static void stopWorker(NesWorker& worker, uint8_t id = 1) {
        NES_INFO("RESTEndpointTest: Stop worker " << id);
        EXPECT_TRUE(worker.stop(true));
    }
};

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testGetExecutionPlanFromWithSingleWorker) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("RESTEndpointTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    // get the execution plan
    std::stringstream getExecutionPlanStringSource;
    getExecutionPlanStringSource << "{\"queryId\" : ";
    getExecutionPlanStringSource << queryId;
    getExecutionPlanStringSource << "}";
    getExecutionPlanStringSource << endl;

    NES_INFO("get execution plan request body=" << getExecutionPlanStringSource.str());
    string getExecutionPlanRequestBody = getExecutionPlanStringSource.str();
    nlohmann::json getExecutionPlanJsonReturn;

    auto future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execution-plan"},
                                cpr::Parameters{{"queryId", std::to_string(queryId)}});
    future.wait();
    auto result = future.get();
    EXPECT_EQ(result.status_code, 200l);
    getExecutionPlanJsonReturn = nlohmann::json::parse(result.text);

    NES_INFO("get execution-plan: try to acc return");
    NES_DEBUG("getExecutionPlan response: " << getExecutionPlanJsonReturn.dump());
    const auto* expected =
        R"({"executionNodes":[{"ScheduledQueries":[{"queryId":1,"querySubPlans":[{"operator":"SINK(4)\n  SOURCE(1,default_logical)\n","querySubPlanId":1}]}],"executionNodeId":2,"topologyId":2,"topologyNodeIpAddress":"127.0.0.1"},{"ScheduledQueries":[{"queryId":1,"querySubPlans":[{"operator":"SINK(2)\n  SOURCE(3,)\n","querySubPlanId":2}]}],"executionNodeId":1,"topologyId":1,"topologyNodeIpAddress":"127.0.0.1"}]})";
    NES_DEBUG("getExecutionPlan response: expected = " << expected);
    ASSERT_EQ(getExecutionPlanJsonReturn.dump(), expected);

    NES_INFO("RESTEndpointTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testPostExecuteQueryExWithEmptyQuery) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();

    auto query = Query::from("default_logical").sink(PrintSinkDescriptor::create());
    auto queryPlan = query.getQueryPlan();

    //make a Protobuff object
    SubmitQueryRequest request;
    auto serializedQueryPlan = request.mutable_queryplan();
    QueryPlanSerializationUtil::serializeQueryPlan(queryPlan, serializedQueryPlan);
    //convert it to string for the request function
    request.set_querystring("");

    std::string msg = request.SerializeAsString();

    nlohmann::json postJsonReturn;

    auto future =
        cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query-ex"}, cpr::Body{msg});
    future.wait();
    auto result = future.get();
    postJsonReturn = nlohmann::json::parse(result.text);

    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testPostExecuteQueryExWithNonEmptyQuery) {
    auto crd = createAndStartCoordinator();

    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    /* REGISTER QUERY */
    CSVSourceTypePtr sourceConfig;
    sourceConfig = CSVSourceType::create();
    sourceConfig->setFilePath("");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    auto windowSource = PhysicalSource::create("test_stream", "test2", sourceConfig);
    auto wrk1 = createAndStartWorkerWithSourceConfig(1, windowSource);
    // Removed this and replaced it by the above. Test is disabled, cannot check correctness. Leaving this for future fixing.
    //    PhysicalSourcePtr conf = PhysicalSourceType::create(sourceConfig);
    //    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(conf, physicalNode);
    //    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());
    //    sourceCatalog->physicalSources.add("default_logical", sce);

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

    nlohmann::json postJsonReturn;

    auto future =
        cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execution-plan-ex"}, cpr::Body{msg});
    future.wait();
    auto result = future.get();
    postJsonReturn = nlohmann::json::parse(result.text);

    EXPECT_TRUE(postJsonReturn.contains("queryId"));
    EXPECT_TRUE(queryCatalogService->getEntryForQuery(postJsonReturn["queryId"].get<int>()));

    EXPECT_TRUE(postJsonReturn.contains("queryId"));
    EXPECT_TRUE(crd->getQueryCatalogService()->getEntryForQuery(postJsonReturn["queryId"].get<int>()));

    auto insertedQueryPlan =
        crd->getQueryCatalogService()->getEntryForQuery(postJsonReturn["queryId"].get<int>())->getInputQueryPlan();
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

    std::string msg = "hello";
    nlohmann::json postJsonReturn;
    int statusCode = 0;

    auto future =
        cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query-ex"}, cpr::Body{msg});
    future.wait();
    auto result = future.get();
    postJsonReturn = nlohmann::json::parse(result.text);
    EXPECT_EQ(statusCode, 400);
    EXPECT_TRUE(postJsonReturn.contains("detail"));

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testGetAllRegisteredQueries) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogServicePtr queryCatalogService = crd->getQueryCatalogService();

    NES_INFO("RESTEndpointTest: Submit query");
    string query = "Query::from(\"default_logical\").sink(PrintSinkDescriptor::create());";
    QueryId queryId =
        queryService->validateAndQueueAddQueryRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalogService));

    cpr::AsyncResponse future1 =
        cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/queryCatalog/allRegisteredQueries"});
    future1.wait();
    auto r = future1.get();
    EXPECT_EQ(r.status_code, 200l);
    nlohmann::json response = nlohmann::json::parse(r.text);

    //Assertions
    NES_DEBUG("Response: " << response.dump());
    EXPECT_TRUE(response.size() == 1);
    EXPECT_TRUE(response.at(0).contains("queryId"));
    EXPECT_TRUE(response.at(0).contains("queryPlan"));
    EXPECT_TRUE(response.at(0).contains("queryStatus"));
    EXPECT_TRUE(response.at(0).contains("queryString"));

    NES_INFO("RESTEndpointTest: Remove query");
    ;
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalogService));

    stopWorker(*wrk1);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testAddParentTopology) {
    auto crd = createAndStartCoordinator();
    auto wrk1 = createAndStartWorker();
    auto wrk2 = createAndStartWorker(2);

    uint64_t parentId = wrk2->getWorkerId();
    uint64_t childId = wrk1->getWorkerId();

    auto parent = crd->getTopology()->findNodeWithId(parentId);
    auto child = crd->getTopology()->findNodeWithId(childId);

    ASSERT_FALSE(child->containAsParent(parent));

    nlohmann::json request{};
    request["parentId"] = parentId;
    request["childId"] = childId;
    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/topology/addParent"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{request.dump()});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    nlohmann::json res = nlohmann::json::parse(response.text);

    NES_DEBUG("Response: " << res.dump());
    EXPECT_TRUE(res.size() == 1);
    EXPECT_TRUE(res.contains("success"));
    EXPECT_TRUE(res["success"]);

    ASSERT_TRUE(child->containAsParent(parent));

    stopWorker(*wrk1);
    stopWorker(*wrk2, 2);
    stopCoordinator(*crd);
}

// Tests in RESTEndpointTest.cpp have been observed to fail randomly. Related issue: #2239
TEST_F(RESTEndpointTest, DISABLED_testAddLogicalSourceEx) {
    auto crd = createAndStartCoordinator();

    Catalogs::Source::SourceCatalogPtr sourceCatalog = crd->getSourceCatalog();

    //create message as Protobuf encoded object
    SchemaPtr schema = Schema::create()->addField("id", BasicType::UINT32)->addField("value", BasicType::UINT64);
    SerializableSchemaPtr serializableSchema = SchemaSerializationUtil::serializeSchema(schema, new SerializableSchema());
    SerializableNamedSchema request;
    request.set_sourcename("test");
    request.set_allocated_schema(serializableSchema.get());
    std::string msg = request.SerializeAsString();
    request.release_schema();

    auto future = cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/addLogicalSource-ex"},
                                 cpr::Header{{"Content-Type", "application/json"}},
                                 cpr::Body{msg});
    future.wait();
    auto response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    nlohmann::json res = nlohmann::json::parse(response.text);

    EXPECT_TRUE(res.contains("success"));
    EXPECT_EQ(sourceCatalog->getAllLogicalSourceAsString().size(), 3U);

    stopCoordinator(*crd);
}
}// namespace NES
