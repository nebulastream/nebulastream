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
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>

namespace NES {
class SourceCatalogControllerTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceCatalogTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down SourceCatalogTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SourceCatalogTest test class."); }
};

TEST_F(SouceCatalogControllerTest, getAllLogicalSource) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    NES_INFO("SourceCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);

    if (!success) {
        FAIL() << "Rest server failed to start";
    }

    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());

    sourceCatalog->addLogicalSource("test_stream", Schema::create());
    /*
    sourceCatalog->addLogicalSource("test_stream", Schema::create());
    SchemaPtr sPtr = sourceCatalog->getSchemaForLogicalSource("test_stream");
    EXPECT_NE(sPtr, nullptr);
    */
    map<std::string, SchemaPtr> allLogicalSource = sourceCatalog->getAllLogicalSource();
    /*
    string exp = "id:INTEGER value:INTEGER ";
    EXPECT_EQ(allLogicalSource.size(), 3U);

    SchemaPtr testSchema = allLogicalSource["test_stream"];
    EXPECT_EQ("", testSchema->toString());

    SchemaPtr defaultSchema = allLogicalSource["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());
    */
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/AllLogicalSource"});
    EXPECT_EQ(r.status_code, 200l);

    // TODO compare value of response with expected value

}


}//namespace NES