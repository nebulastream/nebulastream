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
#include <API/Query.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <NesBaseTest.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <REST/ServerTypes.hpp>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <nlohmann/json.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES {
class QueryControllerTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryControllerTest test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down QueryControllerTest test class."); }
};

TEST_F(QueryControllerTest, testExecuteQueryMalformedBody) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    std::string body = "{\"malformed\": \"test\"}";
    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                              cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{body},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    EXPECT_EQ(response.status_code, 500l);
    //TODO: compare content of response to expected values. To be added once json library found #2950
}

TEST_F(QueryControllerTest, testSubmitQuery) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    nlohmann::json request;
    request["userQuery"] = R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    request["strategyName"] = "BottomUp";
    request["faultTolerance"] ="AT_MOST_ONCE";
    request["lineage"] = "IN_MEMORY";
    auto response   = cpr::Post(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execute-query"},
                              cpr::Header{{"Content-Type", "application/json"}}, cpr::Body{request.dump()},
                              cpr::ConnectTimeout{3000}, cpr::Timeout{3000});
    EXPECT_EQ(response.status_code, 200l);
    NES_DEBUG(response.text);
    //TODO: compare content of response to expected values. To be added once json library found #2950
}

TEST_F(QueryControllerTest, testGetExecutionPlan) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("QueryControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    cpr::Response response = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/query/execution-plan"},
                                cpr::Parameters{{"queryId", "1"}});
    EXPECT_EQ(response.status_code, 404l);
    //TODO: compare content of response to expected values. To be added once json library found #2950
}
} // namespace NES