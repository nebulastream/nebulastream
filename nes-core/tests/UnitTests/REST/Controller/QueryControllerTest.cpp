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

#include <Catalogs/Query/QueryCatalog.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/UDF/UdfCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <REST/Controller/QueryController.hpp>
#include <Services/QueryParsingService.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <cpprest/http_client.h>

namespace NES {

//TODO: to be deleted with #3001
class QueryControllerTest : public Testing::NESBaseTest {
  public:
    //use of TestCase to refer to a group of logically connected tests is deprecated
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down QueryControllerTest test class."); }

    void SetUp() {
        Testing::NESBaseTest::SetUp();
        TopologyPtr topology = Topology::create();
        Catalogs::Query::QueryCatalogPtr queryCatalog = std::make_shared<Catalogs::Query::QueryCatalog>();
        QueryCatalogServicePtr queryCatalogService = std::make_shared<QueryCatalogService>(queryCatalog);
        RequestQueuePtr queryRequestQueue = std::make_shared<RequestQueue>(1);
        Compiler::JITCompilerBuilder jitCompilerBuilder = Compiler::JITCompilerBuilder();
        jitCompilerBuilder.registerLanguageCompiler(Compiler::CPPCompiler::create());
        QueryParsingServicePtr queryParsingService = QueryParsingService::create(jitCompilerBuilder.build());
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
        Configurations::OptimizerConfiguration optimizerConfiguration = OptimizerConfiguration();
        Catalogs::UDF::UdfCatalogPtr udfCatalog = Catalogs::UDF::UdfCatalog::create();
        QueryServicePtr queryService = std::make_shared<QueryService>(queryCatalogService,
                                                                      queryRequestQueue,
                                                                      sourceCatalog,
                                                                      queryParsingService,
                                                                      optimizerConfiguration,
                                                                      udfCatalog);
        GlobalExecutionPlanPtr globalExecutionPlan = GlobalExecutionPlan::create();
        queryController = std::make_shared<QueryController>(queryService, queryCatalogService, globalExecutionPlan);
    };

    Catalogs::Source::SourceCatalogPtr sourceCatalog;
    QueryControllerPtr queryController;
};

TEST_F(QueryControllerTest, testGetExecutionPlan) {

    web::http::http_request msg1(web::http::methods::GET);
    //set query string
    msg1.set_request_uri(web::http::uri(""));

    web::http::http_response httpResponse1;
    web::json::value response1;

    queryController->handleGet(std::vector<utility::string_t>{"query", "execution-plan"}, msg1);
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
    auto getExecutionPlanResponse1 = response1.as_object();
    NES_DEBUG("Response: " << response1.serialize());
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse1.status_code());
    EXPECT_EQ(getExecutionPlanResponse1.at("message").as_string(), "Parameter queryId must be provided");
    EXPECT_EQ(getExecutionPlanResponse1.at("code").as_integer(), httpResponse1.status_code());

    web::http::http_request msg2(web::http::methods::GET);
    //set query string
    msg2.set_request_uri(web::http::uri("?queryId=1"));

    web::http::http_response httpResponse2;
    web::json::value response2;

    queryController->handleGet(std::vector<utility::string_t>{"query", "execution-plan"}, msg2);
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
    auto getExecutionPlanResponse2 = response2.as_object();
    NES_DEBUG("Response: " << response2.serialize());
    EXPECT_TRUE(web::http::status_codes::NotFound == httpResponse2.status_code());
    EXPECT_EQ(getExecutionPlanResponse2.at("message").as_string(), "Provided QueryId: 1 does not exist");
    EXPECT_EQ(getExecutionPlanResponse2.at("code").as_integer(), httpResponse2.status_code());
}

TEST_F(QueryControllerTest, testCorrectExecuteQuery) {
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "default_physical", DefaultSourceType::create());
    TopologyNodePtr topologyNode = TopologyNode::create(1, "0", 0, 0, 1);
    Catalogs::Source::SourceCatalogEntryPtr entry =
        Catalogs::Source::SourceCatalogEntry::create(physicalSource,
                                                     sourceCatalog->getLogicalSource("default_logical"),
                                                     topologyNode);
    ASSERT_TRUE(sourceCatalog->addPhysicalSource("default_logical", entry));
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["userQuery"] = web::json::value::string(queryString);
    messageContent["placement"] = web::json::value::string("BottomUp");
    messageContent["faultTolerance"] = web::json::value::string("AT_MOST_ONCE");
    messageContent["lineage"] = web::json::value::string("IN_MEMORY");
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
            try {
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();

    auto result = response.as_object();
    auto queryId = response.at("queryId");
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(web::http::status_codes::Accepted == httpResponse.status_code());
}

TEST_F(QueryControllerTest, testExecuteQueryNoPhyiscalSource) {
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["userQuery"] = web::json::value::string(queryString);
    messageContent["placement"] = web::json::value::string("BottomUp");
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
            try {
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    auto result = response.as_object();
    auto message = response.at("message").as_string();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_EQ(message, "SourceCatalog: Logical source(s) [default_logical] are found to have no physical source(s) defined. ");
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse.status_code());
}

TEST_F(QueryControllerTest, testExecuteQueryNoPlacementStrategyProvided) {
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["userQuery"] = web::json::value::string(queryString);
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
            try {
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    auto result = response.as_object();
    auto message = response.at("message").as_string();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_EQ(message, "No placement strategy specified. Specify a placement strategy using 'placement'.");
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse.status_code());
}

TEST_F(QueryControllerTest, testExecuteQueryInvalidPlacementStrategyProvided) {
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["userQuery"] = web::json::value::string(queryString);
    std::string invalidPlacementStrategy = "MumboJumbo";
    messageContent["placement"] = web::json::value::string(invalidPlacementStrategy);
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
            try {
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    auto result = response.as_object();
    auto message = response.at("message").as_string();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_EQ(message, "Invalid Placement Strategy: " + invalidPlacementStrategy);
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse.status_code());
}

TEST_F(QueryControllerTest, testExecuteQueryIncorrectOrMissingUserQuery) {
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["wrongKeyword"] = web::json::value::string(queryString);
    messageContent["placement"] = web::json::value::string("BottomUp");
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
            try {
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    auto result = response.as_object();
    auto message = response.at("message").as_string();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_EQ(message, "Incorrect or missing key word for user query, use 'userQuery'.");
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse.status_code());
}

TEST_F(QueryControllerTest, testExecuteQueryInvalidFaultToleranceType) {
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["userQuery"] = web::json::value::string(queryString);
    messageContent["placement"] = web::json::value::string("BottomUp");
    std::string invalidFaultToleranceType = "EXACTLY_TWICE";
    messageContent["faultTolerance"] = web::json::value::string(invalidFaultToleranceType);
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
            try {
                response = task.get();
            } catch (const web::http::http_exception& e) {
                NES_ERROR("Error while setting return. " << e.what());
            }
        })
        .wait();
    auto result = response.as_object();
    auto message = response.at("message").as_string();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_EQ(message,
              "Invalid Fault Tolerance Type provided: " + invalidFaultToleranceType
                  + ". Valid Fault Tolerance Types are: 'AT_MOST_ONCE', 'AT_LEAST_ONCE', 'EXACTLY_ONCE', 'NONE'.");
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse.status_code());
}

TEST_F(QueryControllerTest, testStopQueryInvalidParameterOrNoQueryIdProvided) {
    web::http::http_request msg1(web::http::methods::DEL);
    //set query string
    msg1.set_request_uri(web::http::uri(""));

    web::http::http_response httpResponse1;
    web::json::value response1;

    //tests handling of missing queryId in URI parameters
    queryController->handleGet(std::vector<utility::string_t>{"query", "stop-query"}, msg1);
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
    auto getExecutionPlanResponse1 = response1.as_object();
    NES_DEBUG("Response: " << response1.serialize());
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse1.status_code());
    EXPECT_EQ(getExecutionPlanResponse1.at("message").as_string(), "Parameter queryId must be provided");

    web::http::http_request msg2(web::http::methods::GET);
    //set query string
    msg2.set_request_uri(web::http::uri("?queryId=1"));

    web::http::http_response httpResponse2;
    web::json::value response2;

    //tests handling of providing non-existent queryId
    queryController->handleGet(std::vector<utility::string_t>{"query", "execution-plan"}, msg2);
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
    auto getExecutionPlanResponse2 = response2.as_object();
    NES_DEBUG("Response: " << response2.serialize());
    EXPECT_TRUE(web::http::status_codes::NotFound == httpResponse2.status_code());
    EXPECT_EQ(getExecutionPlanResponse2.at("message").as_string(), "Provided QueryId: " + std::to_string(1) + " does not exist");
    EXPECT_EQ(getExecutionPlanResponse2.at("code").as_integer(), httpResponse2.status_code());
}

TEST_F(QueryControllerTest, testNonExistentSchemaAttribtue) {
    PhysicalSourcePtr physicalSource = PhysicalSource::create("default_logical", "default_physical", DefaultSourceType::create());
    TopologyNodePtr topologyNode = TopologyNode::create(1, "0", 0, 0, 1);
    Catalogs::Source::SourceCatalogEntryPtr entry =
        Catalogs::Source::SourceCatalogEntry::create(physicalSource,
                                                     sourceCatalog->getLogicalSource("default_logical"),
                                                     topologyNode);
    ASSERT_TRUE(sourceCatalog->addPhysicalSource("default_logical", entry));
    web::http::http_request msg(web::http::methods::POST);
    //set query string
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("BAD") < 42).sink(PrintSinkDescriptor::create()); )";
    web::json::value messageContent{};
    messageContent["userQuery"] = web::json::value::string(queryString);
    messageContent["placement"] = web::json::value::string("BottomUp");
    msg.set_body(messageContent);

    web::http::http_response httpResponse;
    web::json::value response;

    queryController->handlePost(std::vector<utility::string_t>{"query", "execute-query"}, msg);
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
    auto getExecutionPlanResponse = response.as_object();
    NES_DEBUG("Response: " << response.serialize());
    EXPECT_TRUE(web::http::status_codes::BadRequest == httpResponse.status_code());
    EXPECT_TRUE(getExecutionPlanResponse.at("message").as_string().find(
                    "FieldAccessExpression: the field BAD is not defined in the  schema")
                != string::npos);
}

}// namespace NES
