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
#include <gtest/gtest.h>
#include <string>

using namespace std;
using namespace NES;

class StreamCatalogServiceTest : public testing::Test {
  public:
    std::string queryString =
        R"(Query::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";

    std::shared_ptr<QueryParsingService> queryParsingService;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup NES StreamCatalogService test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup NES StreamCatalogService test case." << std::endl;
        NES::setupLogging("StreamCatalogServiceTest.log", NES::LOG_DEBUG);
        NES_DEBUG("FINISHED ADDING 5 Serialization to topology");
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        queryParsingService = QueryParsingService::create(jitCompiler);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Setup NES Coordinator test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down NES Coordinator test class." << std::endl; }

    std::string ip = "127.0.0.1";
    uint16_t receive_port = 0;
    std::string host = "localhost";
    uint16_t publish_port = 4711;
    //std::string sensor_type = "default";
};

TEST_F(StreamCatalogServiceTest, testRegisterUnregisterLogicalStream) {
    std::string address = ip + ":" + std::to_string(publish_port);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    StreamCatalogServicePtr streamCatalogService = std::make_shared<StreamCatalogService>(streamCatalog);

    std::string logicalStreamName = "testStream";
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    bool successRegisterLogicalStream = streamCatalogService->registerLogicalStream(logicalStreamName, testSchema);
    EXPECT_TRUE(successRegisterLogicalStream);

    //test register existing stream
    bool successRegisterExistingLogicalStream = streamCatalogService->registerLogicalStream(logicalStreamName, testSchema);
    EXPECT_TRUE(!successRegisterExistingLogicalStream);

    //test unregister not existing node
    bool successUnregisterNotExistingLogicalStream = streamCatalogService->unregisterLogicalStream("asdasd");
    EXPECT_TRUE(!successUnregisterNotExistingLogicalStream);

    //test unregister existing node
    bool successUnregisterExistingLogicalStream = streamCatalogService->unregisterLogicalStream(logicalStreamName);
    EXPECT_TRUE(successUnregisterExistingLogicalStream);
}

TEST_F(StreamCatalogServiceTest, testRegisterUnregisterPhysicalStream) {
    std::string address = ip + ":" + std::to_string(publish_port);
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>(queryParsingService);
    TopologyPtr topology = Topology::create();
    StreamCatalogServicePtr streamCatalogService = std::make_shared<StreamCatalogService>(streamCatalog);
    TopologyManagerServicePtr topologyManagerService = std::make_shared<TopologyManagerService>(topology, streamCatalog);

    std::string physicalStreamName = "testStream";

    SourceConfigPtr sourceConfig = SourceConfig::create();
    sourceConfig->setSourceType("CSVSource");
    sourceConfig->setSourceConfig("testCSV.csv");
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("physical_test");
    sourceConfig->setLogicalStreamName("testStream");

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(sourceConfig);

    auto nodeStats = std::make_shared<NodeStats>();
    uint64_t nodeId = topologyManagerService->registerNode(address, 4000, 5000, 6, nodeStats, NodeType::Sensor);
    EXPECT_NE(nodeId, 0u);

    //setup test
    std::string testSchema = "Schema::create()->addField(createField(\"campaign_id\", UINT64));";
    bool successRegisterLogicalStream = streamCatalogService->registerLogicalStream(conf->getLogicalStreamName(), testSchema);
    EXPECT_TRUE(successRegisterLogicalStream);

    // common case
    TopologyNodePtr physicalNode = topology->findNodeWithId(nodeId);
    bool successRegisterPhysicalStream = streamCatalogService->registerPhysicalStream(physicalNode,
                                                                                      conf->getSourceType(),
                                                                                      conf->getPhysicalStreamName(),
                                                                                      conf->getLogicalStreamName());
    EXPECT_TRUE(successRegisterPhysicalStream);

    //test register existing stream
    bool successRegisterExistingPhysicalStream = streamCatalogService->registerPhysicalStream(physicalNode,
                                                                                              conf->getSourceType(),
                                                                                              conf->getPhysicalStreamName(),
                                                                                              conf->getLogicalStreamName());
    EXPECT_TRUE(!successRegisterExistingPhysicalStream);

    //test unregister not existing physical stream
    bool successUnregisterNotExistingPhysicalStream =
        streamCatalogService->unregisterPhysicalStream(physicalNode, "asd", conf->getLogicalStreamName());
    EXPECT_TRUE(!successUnregisterNotExistingPhysicalStream);

    //test unregister not existing local stream
    bool successUnregisterNotExistingLocicalStream =
        streamCatalogService->unregisterPhysicalStream(physicalNode, conf->getPhysicalStreamName(), "asd");
    EXPECT_TRUE(!successUnregisterNotExistingLocicalStream);

    //test unregister existing node
    bool successUnregisterExistingPhysicalStream =
        streamCatalogService->unregisterPhysicalStream(physicalNode, conf->getPhysicalStreamName(), conf->getLogicalStreamName());
    EXPECT_TRUE(successUnregisterExistingPhysicalStream);
}