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

#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Services/QueryParsingService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <gtest/gtest.h>
#include <string>

using namespace std;
using namespace NES;
using namespace Configurations;

class SourceCatalogServiceTest : public Testing::BaseIntegrationTest {
  public:
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";

    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("SourceCatalogServiceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NES SourceCatalogService test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("Setup NES SourceCatalogService test case.");
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
        borrowed_publish_port = getAvailablePort();
        publish_port = *borrowed_publish_port;
    }

    std::string ip = "127.0.0.1";
    uint16_t receive_port = 0;
    std::string host = "localhost";
    Testing::BorrowedPortPtr borrowed_publish_port;
    uint16_t publish_port = 4711;
    //std::string sensor_type = "default";
};

TEST_F(SourceCatalogServiceTest, testRegisterUnregisterLogicalSource) {
    std::string address = ip + ":" + std::to_string(publish_port);
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();

    std::string logicalSourceName = "testStream";
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    bool successRegisterLogicalSource = sourceCatalog->addLogicalSource(logicalSourceName, testSchema);
    EXPECT_TRUE(successRegisterLogicalSource);

    //test register existing source
    bool successRegisterExistingLogicalSource = sourceCatalog->addLogicalSource(logicalSourceName, testSchema);
    EXPECT_TRUE(!successRegisterExistingLogicalSource);

    //test unregister not existing node
    bool successUnregisterNotExistingLogicalSource = sourceCatalog->removeLogicalSource("asdasd");
    EXPECT_TRUE(!successUnregisterNotExistingLogicalSource);

    //test unregister existing node
    bool successUnregisterExistingLogicalSource = sourceCatalog->removeLogicalSource(logicalSourceName);
    EXPECT_TRUE(successUnregisterExistingLogicalSource);
}

TEST_F(SourceCatalogServiceTest, testRegisterUnregisterPhysicalSource) {
    std::string address = ip + ":" + std::to_string(publish_port);
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>();
    TopologyPtr topology = Topology::create();

    std::string physicalSourceName = "testStream";

    auto csvSourceType = CSVSourceType::create("testStream", "physical_test");
    csvSourceType->setFilePath("testCSV.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setNumberOfBuffersToProduce(3);
    auto physicalSource = PhysicalSource::create(csvSourceType);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::NO_LOCATION;
    auto bandwidthInMbps = 50;
    auto latencyInMs = 1;
    uint64_t nodeId =
        topology->registerWorker(INVALID_WORKER_NODE_ID, address, 4000, 5000, 6, properties, bandwidthInMbps, latencyInMs);
    EXPECT_NE(nodeId, 0u);

    //setup test
    auto testSchema = Schema::create()->addField(createField("campaign_id", BasicType::UINT64));
    bool successRegisterLogicalSource =
        sourceCatalog->addLogicalSource(physicalSource->getLogicalSourceName(), testSchema);
    EXPECT_TRUE(successRegisterLogicalSource);

    // common case
    bool successRegisterPhysicalSource = sourceCatalog->registerPhysicalSource(physicalSource->getPhysicalSourceName(),
                                                                                      physicalSource->getLogicalSourceName(),
                                                                                      nodeId);
    EXPECT_TRUE(successRegisterPhysicalSource);

    //test register existing source
    bool successRegisterExistingPhysicalSource =
        sourceCatalog->registerPhysicalSource(physicalSource->getPhysicalSourceName(),
                                                     physicalSource->getLogicalSourceName(),
                                                     nodeId);
    EXPECT_TRUE(!successRegisterExistingPhysicalSource);

    //test unregister not existing physical source
    bool successUnregisterNotExistingPhysicalSource =
        sourceCatalog->removePhysicalSource("asd", physicalSource->getLogicalSourceName(), nodeId);
    EXPECT_TRUE(!successUnregisterNotExistingPhysicalSource);

    //test unregister not existing local source
    bool successUnregisterNotExistingLogicalSource =
        sourceCatalog->removePhysicalSource(physicalSource->getPhysicalSourceName(), "asd", nodeId);
    EXPECT_TRUE(!successUnregisterNotExistingLogicalSource);

    //test unregister existing node
    bool successUnregisterExistingPhysicalSource =
        sourceCatalog->removePhysicalSource(physicalSource->getPhysicalSourceName(),
                                                       physicalSource->getLogicalSourceName(),
                                                       nodeId);
    EXPECT_TRUE(successUnregisterExistingPhysicalSource);
}
