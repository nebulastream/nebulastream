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
#include <gtest/gtest.h>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Services/QueryParsingService.hpp>
#include <Catalogs/Source/SourceCatalogService.hpp>
#include <Catalogs/Topology/TopologyManagerService.hpp>
#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

using namespace std;
using namespace NES;

class TopologyManagerServiceTest : public Testing::BaseIntegrationTest {
  public:
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";

    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyManager.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NES TopologyManagerService test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        NES_DEBUG("Setup NES TopologyManagerService test case.");
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
    int publish_port;
    //std::string sensor_type = "default";
};

TEST_F(TopologyManagerServiceTest, testRegisterUnregisterNode) {
    Catalogs::Source::SourceCatalogPtr sourceCatalog = std::make_shared<Catalogs::Source::SourceCatalog>(queryParsingService);
    TopologyPtr topology = Topology::create();
    auto locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>();
    TopologyManagerServicePtr topologyManagerService = std::make_shared<TopologyManagerService>(topology, locationIndex);

    std::map<std::string, std::any> properties;
    properties[NES::Worker::Properties::MAINTENANCE] = false;
    properties[NES::Worker::Configuration::SPATIAL_SUPPORT] = NES::Spatial::Experimental::SpatialType::FIXED_LOCATION;

    uint64_t nodeId = topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, ip, publish_port, 5000, 6, properties);
    EXPECT_NE(nodeId, 0u);

    uint64_t nodeId1 =
        topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, ip, publish_port + 2, 5000, 6, properties);
    EXPECT_EQ(nodeId1, 2u);

    //test register existing node
    // when trying to register with a workerId belonging to an active worker,
    // the next available workerId will be assigned instead
    uint64_t nodeId2 = topologyManagerService->registerWorker(2, ip, publish_port + 4, 5000, 6, properties);
    EXPECT_EQ(nodeId2, 3u);

    //test unregister not existing node
    bool successUnregisterNotExistingNode = topologyManagerService->unregisterNode(552);
    EXPECT_FALSE(successUnregisterNotExistingNode);

    //test unregister existing node
    bool successUnregisterExistingNode = topologyManagerService->unregisterNode(nodeId1);
    EXPECT_TRUE(successUnregisterExistingNode);

    //test register new node
    uint64_t nodeId3 =
        topologyManagerService->registerWorker(INVALID_TOPOLOGY_NODE_ID, ip, publish_port + 6, 5000, 6, properties);
    EXPECT_EQ(nodeId3, 4u);

    //test register new node with misconfigured worker id
    //when trying to register with a workerId that does not belong to an already active/inactive worker,
    //the next available workerId will be assigned
    uint64_t nodeId4 = topologyManagerService->registerWorker(123, ip, publish_port + 8, 5000, 6, properties);
    EXPECT_EQ(nodeId4, 5u);
}
