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

#include "gtest/gtest.h"
#include <Catalogs/Source/LogicalSource.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Compiler/CPPCompiler/CPPCompiler.hpp>
#include <Compiler/JITCompilerBuilder.hpp>
#include <Services/QueryParsingService.hpp>
#include <iostream>
#include <NesBaseTest.hpp>
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
class StreamCatalogTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<SourceCatalog> streamCatalog;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::setupLogging("StreamCatalogTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup StreamCatalogTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
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
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    streamCatalog->addLogicalStream(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
}

//TODO: add test for a second physical stream add

TEST_F(StreamCatalogTest, testAddRemovePhysicalStream) {
    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    streamCatalog->addLogicalStream(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
    EXPECT_TRUE(streamCatalog->removePhysicalStream(physicalSource->getLogicalSourceName(),
                                                    physicalSource->getPhysicalSourceName(),
                                                    physicalNode->getId()));
    NES_INFO(streamCatalog->getPhysicalStreamAndSchemaAsString());
}

TEST_F(StreamCatalogTest, testAddPhysicalForNotExistingLogicalStream) {
    TopologyPtr topology = Topology::create();
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    streamCatalog->addLogicalStream(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
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
    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    auto logicalSource = LogicalSource::create("test_stream", Schema::create());
    streamCatalog->addLogicalStream(logicalSource->getLogicalSourceName(), logicalSource->getSchema());
    auto defaultSourceType = DefaultSourceType::create();
    auto physicalSource = PhysicalSource::create(logicalSource->getLogicalSourceName(), "physicalSource", defaultSourceType);
    SourceCatalogEntryPtr sce = std::make_shared<SourceCatalogEntry>(physicalSource, logicalSource, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalSource(logicalSource->getLogicalSourceName(), sce));
    const vector<SourceCatalogEntryPtr>& allPhysicalSources =
        streamCatalog->getPhysicalSources(logicalSource->getLogicalSourceName());
    EXPECT_EQ(allPhysicalSources.size(), 1U);
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