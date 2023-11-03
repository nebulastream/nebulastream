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

#include <API/AttributeField.hpp>
#include <API/Query.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <SerializableOperator.pb.h>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <cpr/cpr.h>
#include <gtest/gtest.h>
#include <memory>
#include <nlohmann/json.hpp>

using namespace std;

namespace NES {
using namespace Configurations;

class SourceCatalogControllerTest : public Testing::BaseIntegrationTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogControllerTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup SourceCatalogControllerTest test class.");
    }
    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down SourceCatalogControllerTest test class."); }

    /**
     * Starts a coordinator with the following configurations
     * rpcPort = rpcCoordinatorPort specified in BaseIntegrationTest
     * restPort = restPort specified in BaseIntegrationTest
     */
    void startCoordinator() {
        NES_INFO("SourceCatalogControllerTest: Start coordinator");
        coordinatorConfig = CoordinatorConfiguration::createDefault();
        coordinatorConfig->rpcPort = *rpcCoordinatorPort;
        coordinatorConfig->restPort = *restPort;
        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    }

    /**
     * Starts a coordinator with the given configurations
     * Use when configurations mentioned in startCoordinator() dont suit needs
     * @param coordinatorConfig
     */
    void startCoordinator(CoordinatorConfigurationPtr coordinatorConfiguration) {
        NES_INFO("SourceCatalogControllerTest: Start coordinator");
        coordinatorConfig = coordinatorConfiguration;
        coordinator = std::make_shared<NesCoordinator>(coordinatorConfig);
        ASSERT_EQ(coordinator->startCoordinator(false), *rpcCoordinatorPort);
    }

    void stopCoordinator() {
        bool stopCrd = coordinator->stopCoordinator(true);
        ASSERT_TRUE(stopCrd);
    }

    CoordinatorConfigurationPtr coordinatorConfig;
    NesCoordinatorPtr coordinator;
};

TEST_F(SourceCatalogControllerTest, testGetAllLogicalSource) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    Catalogs::Source::SourceCatalogPtr sourceCatalog = coordinator->getSourceCatalog();
    sourceCatalog->addLogicalSource("test_stream", Schema::create());

    cpr::AsyncResponse future =
        cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/allLogicalSource"});
    future.wait();
    cpr::Response r = future.get();
    EXPECT_EQ(r.status_code, 200l);
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Headers"));
    nlohmann::json response;
    ASSERT_NO_THROW(response = nlohmann::json::parse(r.text));
    NES_DEBUG("{}", r.text);
    bool found = false;
    for (auto& el : response.items()) {
        if (el.value().contains("test_stream")) {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);
    stopCoordinator();
}

TEST_F(SourceCatalogControllerTest, testGetPhysicalSource) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    NES_DEBUG("SourceCatalogControllerTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    auto csvSourceType1 = CSVSourceType::create("default_logical", "physical_test");
    csvSourceType1->setFilePath("");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SourceCatalogControllerTest: Worker1 started successfully");
    NES_INFO("{}", coordinator->getSourceCatalog()->getPhysicalSourceAndSchemaAsString());
    cpr::AsyncResponse future =
        cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/allPhysicalSource"},
                      cpr::Parameters{{"logicalSourceName", "default_logical"}});
    future.wait();
    cpr::Response r = future.get();
    EXPECT_EQ(r.status_code, 200l);
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Headers"));
    nlohmann::json response;
    ASSERT_NO_THROW(response = nlohmann::json::parse(r.text));
    ASSERT_TRUE(response.contains("Physical Sources") && response["Physical Sources"].size() != 0);
    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);
    stopCoordinator();
}

TEST_F(SourceCatalogControllerTest, testGetSchema) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    // create a schema
    SchemaPtr schema = Schema::create();
    schema->addField("ID", BasicType::UINT64);
    // and add it to the source catalog
    coordinator->getSourceCatalog()->addLogicalSource("test_stream", schema);
    // submitting a GET request for the above defined schema
    cpr::AsyncResponse future = cpr::GetAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/schema"},
                                              cpr::Parameters{{"logicalSourceName", "test_stream"}});
    future.wait();
    cpr::Response r = future.get();
    // returns 200 OK
    EXPECT_EQ(r.status_code, 200l);
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Headers"));
    SerializableSchema response;
    // parse the returned schema
    response.ParseFromString(r.text);
    // and check if its fields match with the fields defined in the schema above
    ASSERT_TRUE(response.fields().size() == 1);
    ASSERT_TRUE(response.fields(0).name() == "ID");
    stopCoordinator();
}

TEST_F(SourceCatalogControllerTest, testPostLogicalSource) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    Catalogs::Source::SourceCatalogPtr sourceCatalog = coordinator->getSourceCatalog();
    ASSERT_FALSE(sourceCatalog->containsLogicalSource("car"));
    std::string schema = "Schema::create()->addField(\"$ID\", BasicType::UINT64)";
    const std::string sourceName = "car";
    nlohmann::json request;
    request["logicalSourceName"] = sourceName;
    request["schema"] = schema;
    cpr::AsyncResponse future =
        cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/addLogicalSource"},
                       cpr::Header{{"Content-Type", "application/json"}},
                       cpr::Body{request.dump()});
    future.wait();
    cpr::Response response = future.get();
    EXPECT_EQ(response.status_code, 200l);
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(response.header.contains("Access-Control-Allow-Headers"));
    nlohmann::json success;
    ASSERT_NO_THROW(success = nlohmann::json::parse(response.text));
    ASSERT_TRUE(success["success"]);
    ASSERT_TRUE(!sourceCatalog->getAllLogicalSource().empty());
    SchemaPtr schemaFromCoordinator = sourceCatalog->getLogicalSource("car")->getSchema();
    //TODO: is it a bug that one has to define the field name with a '$' in the schema for it to be found using hasFieldName ?
    ASSERT_TRUE(schemaFromCoordinator->getField("ID") != nullptr);
    stopCoordinator();
}

TEST_F(SourceCatalogControllerTest, testUpdateLogicalSource) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    Catalogs::Source::SourceCatalogPtr sourceCatalog = coordinator->getSourceCatalog();
    auto schema = Schema::create()->addField("ID", BasicType::UINT64);
    const std::string sourceName = "car";
    sourceCatalog->addLogicalSource(sourceName, schema);
    nlohmann::json request;
    std::string updatedSchema = "Schema::create()->addField(\"$ID\", BasicType::UINT64)"
                                "->addField(\"$value\", BasicType::UINT64)"
                                "->addField(\"$timestamp\", DataTypeFactory::createUInt64());";
    request["logicalSourceName"] = sourceName;
    request["schema"] = updatedSchema;
    cpr::AsyncResponse future =
        cpr::PostAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/updateLogicalSource"},
                       cpr::Header{{"Content-Type", "application/json"}},
                       cpr::Body{request.dump()});
    future.wait();
    cpr::Response r = future.get();
    ASSERT_EQ(r.status_code, 200l);
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Headers"));
    nlohmann::json jsonResponse;
    ASSERT_NO_THROW(jsonResponse = nlohmann::json::parse(r.text));
    ASSERT_TRUE(jsonResponse["success"]);
    auto coordinatorSchema = sourceCatalog->getLogicalSource("car")->getSchema();
    //TODO: is it a bug that one has to define the field name with a '$' in the schema for it to be found using hasFieldName ?
    ASSERT_TRUE(coordinatorSchema->getField("ID") != nullptr);
    ASSERT_TRUE(coordinatorSchema->getField("value") != nullptr);
    ASSERT_TRUE(coordinatorSchema->getField("timestamp") != nullptr);
    stopCoordinator();
}
TEST_F(SourceCatalogControllerTest, testDeleteLogicalSource) {
    startCoordinator();
    ASSERT_TRUE(TestUtils::checkRESTServerStartedOrTimeout(coordinatorConfig->restPort.getValue(), 5));
    SchemaPtr schema = Schema::create();
    schema->addField("ID", BasicType::UINT64);
    Catalogs::Source::SourceCatalogPtr sourceCatalog = coordinator->getSourceCatalog();
    sourceCatalog->addLogicalSource("test_stream", schema);
    cpr::AsyncResponse future =
        cpr::DeleteAsync(cpr::Url{BASE_URL + std::to_string(*restPort) + "/v1/nes/sourceCatalog/deleteLogicalSource"},
                         cpr::Parameters{{"logicalSourceName", "test_stream"}});
    future.wait();
    cpr::Response r = future.get();
    ASSERT_EQ(r.status_code, 200l);
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Origin"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Methods"));
    EXPECT_FALSE(r.header.contains("Access-Control-Allow-Headers"));
    nlohmann::json success;
    ASSERT_NO_THROW(success = nlohmann::json::parse(r.text));
    ASSERT_TRUE(success["success"]);
    ASSERT_FALSE(sourceCatalog->containsLogicalSource("test_stream"));
    stopCoordinator();
}

}// namespace NES