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
#include "Compiler/CPPCompiler/CPPCompiler.hpp"
#include "NesBaseTest.hpp"
#include "Plans/Utils/PlanIdGenerator.hpp"
#include <Plans/Query/QueryPlan.hpp>
#include "REST/ServerTypes.hpp"
#include "Util/Logger/Logger.hpp"
#include "Util/TestUtils.hpp"
#include <Compiler/JITCompilerBuilder.hpp>
#include <Services/QueryParsingService.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <API/Query.hpp>

namespace NES {
class QueryCatalogControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConnectivityControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup TopologyControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down ConnectivityControllerTest test class."); }
};

TEST_F(QueryCatalogControllerTest, testGetRequestAllRegistedQueries) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerCreationOrTimeout(coordinatorConfig->restPort.getValue(),5);

    if(!success){
        FAIL() << "Rest server failed to start";
    }

    cpr::Response r =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/allRegisteredQueries"});
    EXPECT_EQ(r.status_code, 204l);

    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto queryParsingService = QueryParsingService::create(jitCompiler);
    auto queryCatalogService = coordinator->getQueryCatalogService();
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, "BottomUp");
    cpr::Response re =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/allRegisteredQueries"});
    std::cout << re.text; //TODO: do some text comparsion?
    EXPECT_EQ(re.status_code, 200l);

}

TEST_F(QueryCatalogControllerTest, testGetQueriesWithSpecificStatus) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerCreationOrTimeout(coordinatorConfig->restPort.getValue(),5);

    if(!success){
        FAIL() << "Rest server failed to start";
    }
    cpr::Response r1 =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/queries"});
    EXPECT_EQ(r1.status_code, 400l);
    cpr::Response r2=
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/queries"},
                 cpr::Parameters{{"status", "REGISTERED"}});
    EXPECT_EQ(r2.status_code, 204l);
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto queryParsingService = QueryParsingService::create(jitCompiler);
    auto queryCatalogService = coordinator->getQueryCatalogService();
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, "BottomUp");
    cpr::Response r3=
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/queries"},
                 cpr::Parameters{{"status", "REGISTERED"}});
    EXPECT_EQ(r3.status_code, 200l);

}
TEST_F(QueryCatalogControllerTest, testGetRequestStatusOfQuery) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerCreationOrTimeout(coordinatorConfig->restPort.getValue(),5);
    if(!success){
        FAIL() << "Rest server failed to start";
    }

    cpr::Response r1 =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/status"});
    EXPECT_EQ(r1.status_code, 400l);
    cpr::Response r2=
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/status"},
                 cpr::Parameters{{"queryId", "1"}});
    EXPECT_EQ(r2.status_code, 204l);
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    auto cppCompiler = Compiler::CPPCompiler::create();
    auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
    auto queryParsingService = QueryParsingService::create(jitCompiler);
    auto queryCatalogService = coordinator->getQueryCatalogService();
    QueryPtr query = queryParsingService->createQueryFromCodeString(queryString);
    QueryId queryId = PlanIdGenerator::getNextQueryId();
    const QueryPlanPtr queryPlan = query->getQueryPlan();
    queryPlan->setQueryId(queryId);
    auto catalogEntry = queryCatalogService->createNewEntry(queryString, queryPlan, "BottomUp");
    cpr::Response r3=
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/status"},
                 cpr::Parameters{{"queryId", std::to_string(queryId)}});
    EXPECT_EQ(r3.status_code, 200l);
    std::cout << r3.text; //TODO: do some text comparsion?
}
TEST_F(QueryCatalogControllerTest, testGetRequestNumberOfBuffersProduced) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    EXPECT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerCreationOrTimeout(coordinatorConfig->restPort.getValue(),5);
    if(!success){
        FAIL() << "Rest server failed to start";
    }

    cpr::Response r1 =
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/getNumberOfProducedBuffers"});
    EXPECT_EQ(r1.status_code, 400);
    cpr::Response r2=
        cpr::Get(cpr::Url{"http://127.0.0.1:" + std::to_string(*restPort) + "/v1/nes/queryCatalog/getNumberOfProducedBuffers"},
                 cpr::Parameters{{"queryId", "1"}});
    EXPECT_EQ(r2.status_code, 204l);
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
  //TODO: add test for buffer production
}

}//namespace NES
