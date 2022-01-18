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

#include "gtest/gtest.h"
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/PhysicalStreamConfigFactory.hpp>
#include <Services/QueryParsingService.hpp>
#include <iostream>

#include <API/Schema.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <Util/Logger.hpp>

using namespace std;
using namespace NES;
using namespace Configurations;
std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
const std::string defaultLogicalStreamName = "default_logical";

/* - nesTopologyManager ---------------------------------------------------- */
class StreamCatalogTest : public testing::Test {
  public:
    SourceConfigPtr sourceConfig;
    std::shared_ptr<SourceCatalog> streamCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("StreamCatalogTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup StreamCatalogTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        sourceConfig = PhysicalStreamConfigFactory::createSourceConfig();
        auto cppCompiler = Compiler::CPPCompiler::create();
        auto jitCompiler = Compiler::JITCompilerBuilder().registerLanguageCompiler(cppCompiler).build();
        auto queryParsingService = QueryParsingService::create(jitCompiler);
        streamCatalog = std::make_shared<SourceCatalog>(queryParsingService);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { NES_INFO("Tear down StreamCatalogTest test case."); }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StreamCatalogTest test class."); }
};

TEST_F(StreamCatalogTest, testAddGetLogStream) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());

    streamCatalog->addLogicalStream("test_stream", Schema::create());
    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream("test_stream");
    EXPECT_NE(sPtr, nullptr);

    map<std::string, SchemaPtr> allLogicalStream = streamCatalog->getAllLogicalStream();
    string exp = "id:INTEGER value:INTEGER ";
    EXPECT_EQ(allLogicalStream.size(), 3U);

    SchemaPtr testSchema = allLogicalStream["test_stream"];
    EXPECT_EQ("", testSchema->toString());

    SchemaPtr defaultSchema = allLogicalStream["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());
}

TEST_F(StreamCatalogTest, testAddRemoveLogStream) {
    streamCatalog->addLogicalStream("test_stream", Schema::create());

    EXPECT_TRUE(streamCatalog->removeLogicalStream("test_stream"));

    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream("test_stream");
    EXPECT_EQ(sPtr, nullptr);

    string exp = "logical stream name=default_logical schema: name=id UINT32 name=value UINT64\n\nlogical stream "
                 "name=test_stream schema:\n\n";

    map<std::string, SchemaPtr> allLogicalStream = streamCatalog->getAllLogicalStream();

    EXPECT_NE(1ul, allLogicalStream.size());
    EXPECT_FALSE(streamCatalog->removeLogicalStream("test_stream22"));
}

TEST_F(StreamCatalogTest, testGetNotExistingKey) {
    SourceCatalogPtr streamCatalog = std::make_shared<SourceCatalog>(QueryParsingServicePtr());

    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream("test_stream22");
    EXPECT_EQ(sPtr, nullptr);
}

TEST_F(StreamCatalogTest, testAddGetPhysicalStream) {
    TopologyPtr topology = Topology::create();

    streamCatalog->addLogicalStream("test_stream", Schema::create());

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    sourceConfig->resetSourceOptions();
    sourceConfig->setSourceFrequency(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    PhysicalSourcePtr conf = PhysicalStreamConfig::create(sourceConfig);

    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(conf, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalStream(conf->getLogicalStreamName(), sce));

    std::string expected = "stream name=test_stream with 1 elements:physicalName=test2 logicalStreamName=test_stream "
                           "sourceType=DefaultSource on node=1\n";
    cout << " string=" << streamCatalog->getPhysicalStreamAndSchemaAsString() << endl;

    EXPECT_EQ(expected, streamCatalog->getPhysicalStreamAndSchemaAsString());
}

//TODO: add test for a second physical stream add

TEST_F(StreamCatalogTest, testAddRemovePhysicalStream) {
    TopologyPtr topology = Topology::create();

    streamCatalog->addLogicalStream("test_stream", Schema::create());

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    sourceConfig->resetSourceOptions();
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    PhysicalSourcePtr conf = PhysicalStreamConfig::create(sourceConfig);

    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(conf, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalStream(conf->getLogicalStreamName(), sce));
    EXPECT_TRUE(
        streamCatalog->removePhysicalStream(conf->getLogicalStreamName(), conf->getPhysicalStreamName(), physicalNode->getId()));
    NES_INFO(streamCatalog->getPhysicalStreamAndSchemaAsString());
}

TEST_F(StreamCatalogTest, testAddPhysicalForNotExistingLogicalStream) {
    TopologyPtr topology = Topology::create();

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalSourcePtr streamConf = PhysicalStreamConfig::createEmpty();

    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(streamConf, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalStream(streamConf->getLogicalStreamName(), sce));

    std::string expected =
        "stream name=default_logical with 1 elements:physicalName=default_physical logicalStreamName=default_logical "
        "sourceType=DefaultSource on node=1\n";
    EXPECT_EQ(expected, streamCatalog->getPhysicalStreamAndSchemaAsString());
}
//new from service
TEST_F(StreamCatalogTest, testGetAllLogicalStream) {

    const map<std::string, std::string>& allLogicalStream = streamCatalog->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 2U);
    for (auto const& [key, value] : allLogicalStream) {
        bool cmp = key != defaultLogicalStreamName && key != "exdra";
        EXPECT_EQ(cmp, false);
    }
}

TEST_F(StreamCatalogTest, testAddLogicalStreamFromString) {
    streamCatalog->addLogicalStream("test", testSchema);
    const map<std::string, std::string>& allLogicalStream = streamCatalog->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 3U);
}

TEST_F(StreamCatalogTest, testGetPhysicalStreamForLogicalStream) {
    TopologyPtr topology = Topology::create();

    std::string newLogicalStreamName = "test_stream";

    streamCatalog->addLogicalStream(newLogicalStreamName, testSchema);

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    sourceConfig->resetSourceOptions();
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(3);
    sourceConfig->setPhysicalStreamName("test2");
    sourceConfig->setLogicalStreamName("test_stream");

    PhysicalSourcePtr conf = PhysicalStreamConfig::create(sourceConfig);

    SourceCatalogEntryPtr catalogEntryPtr = std::make_shared<SourceCatalogEntry>(conf, physicalNode);
    streamCatalog->addPhysicalStream(newLogicalStreamName, catalogEntryPtr);
    const vector<SourceCatalogEntryPtr>& allPhysicalStream = streamCatalog->getPhysicalStreams(newLogicalStreamName);
    EXPECT_EQ(allPhysicalStream.size(), 1U);
}

TEST_F(StreamCatalogTest, testDeleteLogicalStream) {
    bool success = streamCatalog->removeLogicalStream(defaultLogicalStreamName);
    EXPECT_TRUE(success);
}

TEST_F(StreamCatalogTest, testUpdateLogicalStreamWithInvalidStreamName) {
    std::string logicalStreamName = "test";
    std::string newSchema = "Schema::create()->addField(\"id\", BasicType::UINT32);";
    bool success = streamCatalog->updatedLogicalStream(logicalStreamName, newSchema);
    EXPECT_FALSE(success);
}

TEST_F(StreamCatalogTest, testUpdateLogicalStream) {
    std::string logicalStreamName = "test";
    bool success = streamCatalog->addLogicalStream(logicalStreamName, testSchema);
    EXPECT_TRUE(success);

    std::string newSchema = "Schema::create()->addField(\"id\", BasicType::UINT32);";
    success = streamCatalog->updatedLogicalStream(logicalStreamName, newSchema);
    EXPECT_TRUE(success);
}