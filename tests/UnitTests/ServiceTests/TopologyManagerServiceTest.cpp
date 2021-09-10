/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <gtest/gtest.h>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <CoordinatorRPCService.pb.h>
#include <Services/QueryParsingService.hpp>
#include <Services/StreamCatalogService.hpp>
#include <Services/TopologyManagerService.hpp>
#include <Topology/Topology.hpp>
#include <Util/Logger.hpp>
#include <string>

using namespace std;
using namespace NES;

class TopologyManagerServiceTest : public testing::Test {
  public:
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";

    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup NES TopologyManagerService test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup NES TopologyManagerService test case." << std::endl;
        NES::setupLogging("TopologyManager.log", NES::LOG_DEBUG);
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup NES TopologyManagerService test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down NES TopologyManagerService test class." << std::endl; }

    std::string ip = "127.0.0.1";
    uint16_t receive_port = 0;
    std::string host = "localhost";
    uint16_t publish_port = 4711;
    //std::string sensor_type = "default";
};

TEST_F(TopologyManagerServiceTest, testRegisterUnregisterNode) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    TopologyPtr topology = Topology::create();
    TopologyManagerServicePtr topologyManagerService = std::make_shared<TopologyManagerService>(topology, streamCatalog);

    auto nodeStats = std::make_shared<NodeStats>();
    uint64_t nodeId = topologyManagerService->registerNode(ip, publish_port, 5000, 6, nodeStats, NodeType::Sensor);
    EXPECT_NE(nodeId, 0u);

    uint64_t nodeId1 = topologyManagerService->registerNode(ip, publish_port + 2, 5000, 6, nodeStats, NodeType::Sensor);
    EXPECT_NE(nodeId1, 0u);

    //test register existing node
    auto nodeStats2 = std::make_shared<NodeStats>();
    uint64_t nodeId2 = topologyManagerService->registerNode(ip, publish_port, 5000, 6, nodeStats2, NodeType::Sensor);
    EXPECT_EQ(nodeId2, 0u);
    //test unregister not existing node
    bool successUnregisterNotExistingNode = topologyManagerService->unregisterNode(552);
    EXPECT_FALSE(successUnregisterNotExistingNode);

    //test unregister existing node
    bool successUnregisterExistingNode = topologyManagerService->unregisterNode(nodeId1);
    EXPECT_TRUE(successUnregisterExistingNode);
}
