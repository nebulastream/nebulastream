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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Services/QueryParsingService.hpp>
#include <Services/SourceCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

using namespace std;
using namespace NES;

class TopologyManagerServiceTest : public Testing::NESBaseTest {
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
        Testing::NESBaseTest::SetUp();
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
    TopologyManagerServicePtr topologyManagerService = std::make_shared<TopologyManagerService>(topology);

    uint64_t nodeId = topologyManagerService->registerNode(ip,
                                                           publish_port,
                                                           5000,
                                                           6,
                                                           NES::Spatial::Index::Experimental::Location(),
                                                           NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION,
                                                           /* isTfInstalled */ false);
    EXPECT_NE(nodeId, 0u);

    uint64_t nodeId1 = topologyManagerService->registerNode(ip,
                                                            publish_port + 2,
                                                            5000,
                                                            6,
                                                            NES::Spatial::Index::Experimental::Location(),
                                                            NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION,
                                                            /* isTfInstalled */ false);
    EXPECT_NE(nodeId1, 0u);

    //test register existing node
    uint64_t nodeId2 = topologyManagerService->registerNode(ip,
                                                            publish_port,
                                                            5000,
                                                            6,
                                                            NES::Spatial::Index::Experimental::Location(),
                                                            NES::Spatial::Index::Experimental::NodeType::FIXED_LOCATION,
                                                            /* isTfInstalled */ false);
    EXPECT_EQ(nodeId2, 0u);

    //test unregister not existing node
    bool successUnregisterNotExistingNode = topologyManagerService->unregisterNode(552);
    EXPECT_FALSE(successUnregisterNotExistingNode);

    //test unregister existing node
    bool successUnregisterExistingNode = topologyManagerService->unregisterNode(nodeId1);
    EXPECT_TRUE(successUnregisterExistingNode);
}
