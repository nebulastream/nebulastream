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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
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

using namespace std;

namespace NES {
using namespace Configurations;
std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
const std::string defaultLogicalSourceName = "default_logical";

class SourceCatalogControllerTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Catalogs::Source::SourceCatalog> sourceCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceCatalogControllerTest test class.");
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SourceCatalogControllerTest test class."); }
};

TEST_F(SourceCatalogControllerTest, testGetAllLogicalSource) {
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
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/allLogicalSource"});
    EXPECT_EQ(r.status_code, 200l);
    // TODO compare value of response with expected value  To be added once json library found #2950
    NES_INFO("This is the content of response:");
    NES_INFO(r.text);
}

TEST_F(SourceCatalogControllerTest, testGetPhysicalSource) {
    NES_INFO("TestsForOatppEndpoints: Start coordinator");
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::create();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->restServerType = ServerType::Oatpp;
    auto coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = coordinator->startCoordinator(/**blocking**/ false);
    ASSERT_EQ(port, *rpcCoordinatorPort);
    NES_INFO("SourceCatalogControllerTest: Coordinator started successfully");
    bool success = TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5);
    if (!success) {
        FAIL() << "Rest server failed to start";
    }
    NES_DEBUG("SourceCatalogControllerTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    auto csvSourceType1 = CSVSourceType::create();
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    auto physicalSource1 = PhysicalSource::create("default_logical", "physical_test", csvSourceType1);
    workerConfig1->physicalSources.add(physicalSource1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SourceCatalogRemoteTest: Worker1 started successfully");
    NES_INFO(coordinator->getSourceCatalog()->getPhysicalSourceAndSchemaAsString());
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/allPhysicalSource"},
                               cpr::Parameters{{"logicalSourceName", "default_logical"}});
    EXPECT_EQ(r.status_code, 200l);
    // TODO compare value of response with expected value  To be added once json library found #2950
    NES_INFO("This is the content of response:");
    NES_INFO(r.text);
    NES_INFO("stopping worker");
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);
}

TEST_F(SourceCatalogControllerTest, DISABLED_testGetSchema) {
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
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/schema"}, cpr::Parameters{{"logicalSourceName", "test_stream"}});
    EXPECT_EQ(r.status_code, 200l);
    // TODO compare value of response with expected value To be added once json library found #2950
    NES_INFO("This is the content of response:");
    NES_INFO(r.text);
}

TEST_F(SourceCatalogControllerTest, DISABLED_testPostLogicalSource) {
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
    }/*
    Catalogs::Source::SourceCatalogPtr sourceCatalog =
        std::make_shared<Catalogs::Source::SourceCatalog>(QueryParsingServicePtr());
    sourceCatalog->addLogicalSource("test_stream", Schema::create());*/
    //toDO pfad ändern
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/addLogicalSource"}, cpr::Parameters{{"logicalSourceName", "test_stream"}});
    EXPECT_EQ(r.status_code, 200l);
    // TODO compare value of response with expected value  To be added once json library found #2950
    NES_INFO("This is the content of response:");
    NES_INFO(r.text);
}

TEST_F(SourceCatalogControllerTest, DISABLED_testUpdateLogicalSource) {
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
    //toDO pfad ändern
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/updateLogicalSource"}, cpr::Parameters{{"logicalSourceName", "test_stream"}});
    EXPECT_EQ(r.status_code, 200l);
    // TODO compare value of response with expected value  To be added once json library found #2950
    NES_INFO("This is the content of response:");
    NES_INFO(r.text);
}

TEST_F(SourceCatalogControllerTest, testDeleteLogicalSource) {
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
    cpr::Response r = cpr::Get(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/deleteLogicalSource"},
                               cpr::Parameters{{"logicalSourceName", "test_stream"}});
    EXPECT_EQ(r.status_code, 200l);
    // TODO compare value of response with expected value  To be added once json library found #2950
    NES_INFO("This is the content of response:");
    NES_INFO(r.text);
}

} // NES