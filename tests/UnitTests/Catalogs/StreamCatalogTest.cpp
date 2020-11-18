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

#include <iostream>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Catalogs/StreamCatalog.hpp>

#include <API/Schema.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>

#include <Util/Logger.hpp>

using namespace std;
using namespace NES;
std::string testSchema = "Schema::create()->addField(\"id\", BasicType::UINT32)"
                         "->addField(\"value\", BasicType::UINT64);";
const std::string defaultLogicalStreamName = "default_logical";

/* - nesTopologyManager ---------------------------------------------------- */
class StreamCatalogTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() { std::cout << "Setup StreamCatalogTest test class." << std::endl; }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("StreamCatalogTest.log", NES::LOG_DEBUG);
        std::cout << "Setup StreamCatalogTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() { std::cout << "Tear down StreamCatalogTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down StreamCatalogTest test class." << std::endl; }
};

TEST_F(StreamCatalogTest, testAddGetLogStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    streamCatalog->addLogicalStream("test_stream", Schema::create());
    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream("test_stream");
    EXPECT_NE(sPtr, nullptr);

    map<std::string, SchemaPtr> allLogicalStream = streamCatalog->getAllLogicalStream();
    string exp = "id:INTEGER value:INTEGER ";
    EXPECT_EQ(allLogicalStream.size(), 4);

    SchemaPtr testSchema = allLogicalStream["test_stream"];
    EXPECT_EQ("", testSchema->toString());

    SchemaPtr defaultSchema = allLogicalStream["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());
}

TEST_F(StreamCatalogTest, testAddRemoveLogStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    streamCatalog->addLogicalStream("test_stream", Schema::create());

    EXPECT_TRUE(streamCatalog->removeLogicalStream("test_stream"));

    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream("test_stream");
    EXPECT_EQ(sPtr, nullptr);

    string exp = "logical stream name=default_logical schema: name=id UINT32 name=value UINT64\n\nlogical stream "
                 "name=test_stream schema:\n\n";

    map<std::string, SchemaPtr> allLogicalStream = streamCatalog->getAllLogicalStream();

    EXPECT_NE(1, allLogicalStream.size());
    EXPECT_FALSE(streamCatalog->removeLogicalStream("test_stream22"));
}

TEST_F(StreamCatalogTest, testGetNotExistingKey) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream("test_stream22");
    EXPECT_EQ(sPtr, nullptr);
}

TEST_F(StreamCatalogTest, testAddGetPhysicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();

    streamCatalog->addLogicalStream("test_stream", Schema::create());

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "1",
                                     /**Source Frequence**/ 0, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                     /**Number of Buffers To Produce**/ 1, /**Physical Stream Name**/ "test2",
                                     /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalStream(conf->getLogicalStreamName(), sce));

    std::string expected = "stream name=test_stream with 1 elements:physicalName=test2 logicalStreamName=test_stream "
                           "sourceType=DefaultSource sourceConfig=1 sourceFrequency=0 numberOfBuffersToProduce=1 on node=1\n";
    cout << " string=" << streamCatalog->getPhysicalStreamAndSchemaAsString() << endl;

    EXPECT_EQ(expected, streamCatalog->getPhysicalStreamAndSchemaAsString());
}

//TODO: add test for a second physical stream add

TEST_F(StreamCatalogTest, testAddRemovePhysicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();

    streamCatalog->addLogicalStream("test_stream", Schema::create());

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create(/**Source Type**/ "DefaultSource", /**Source Config**/ "",
                                     /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                     /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
                                     /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(conf, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalStream(conf->getLogicalStreamName(), sce));
    EXPECT_TRUE(
        streamCatalog->removePhysicalStream(conf->getLogicalStreamName(), conf->getPhysicalStreamName(), physicalNode->getId()));
    NES_INFO(streamCatalog->getPhysicalStreamAndSchemaAsString());
}

TEST_F(StreamCatalogTest, testAddPhysicalForNotExistingLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf, physicalNode);

    EXPECT_TRUE(streamCatalog->addPhysicalStream(streamConf->getLogicalStreamName(), sce));

    std::string expected =
        "stream name=default_logical with 1 elements:physicalName=default_physical logicalStreamName=default_logical "
        "sourceType=DefaultSource sourceConfig=1 sourceFrequency=1 numberOfBuffersToProduce=1 on node=1\n";
    EXPECT_EQ(expected, streamCatalog->getPhysicalStreamAndSchemaAsString());
}
//new from service
TEST_F(StreamCatalogTest, testGetAllLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    const map<std::string, std::string>& allLogicalStream = streamCatalog->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 3);
    for (auto const [key, value] : allLogicalStream) {
        bool cmp = key != defaultLogicalStreamName && key != "exdra" && key != "ysb";
        EXPECT_EQ(cmp, false);
    }
}

TEST_F(StreamCatalogTest, testAddLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    streamCatalog->addLogicalStream("test", testSchema);
    const map<std::string, std::string>& allLogicalStream = streamCatalog->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 4);
}

TEST_F(StreamCatalogTest, testGetPhysicalStreamForLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyPtr topology = Topology::create();

    std::string newLogicalStreamName = "test_stream";

    streamCatalog->addLogicalStream(newLogicalStreamName, testSchema);

    TopologyNodePtr physicalNode = TopologyNode::create(1, "localhost", 4000, 4002, 4);

    PhysicalStreamConfigPtr conf =
        PhysicalStreamConfig::create(/**Source Type**/ "sensor", /**Source Config**/ "",
                                     /**Source Frequence**/ 1, /**Number Of Tuples To Produce Per Buffer**/ 0,
                                     /**Number of Buffers To Produce**/ 3, /**Physical Stream Name**/ "test2",
                                     /**Logical Stream Name**/ "test_stream");

    StreamCatalogEntryPtr catalogEntryPtr = std::make_shared<StreamCatalogEntry>(conf, physicalNode);
    streamCatalog->addPhysicalStream(newLogicalStreamName, catalogEntryPtr);
    const vector<StreamCatalogEntryPtr>& allPhysicalStream = streamCatalog->getPhysicalStreams(newLogicalStreamName);
    EXPECT_EQ(allPhysicalStream.size(), 1);
}

TEST_F(StreamCatalogTest, testDeleteLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    bool success = streamCatalog->removeLogicalStream(defaultLogicalStreamName);
    EXPECT_TRUE(success);
}

TEST_F(StreamCatalogTest, testUpdateLogicalStreamWithInvalidStreamName) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    std::string logicalStreamName = "test";
    std::string newSchema = "Schema::create()->addField(\"id\", BasicType::UINT32);";
    bool success = streamCatalog->updatedLogicalStream(logicalStreamName, newSchema);
    EXPECT_FALSE(success);
}

TEST_F(StreamCatalogTest, testUpdateLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    std::string logicalStreamName = "test";
    bool success = streamCatalog->addLogicalStream(logicalStreamName, testSchema);
    EXPECT_TRUE(success);

    std::string newSchema = "Schema::create()->addField(\"id\", BasicType::UINT32);";
    success = streamCatalog->updatedLogicalStream(logicalStreamName, newSchema);
    EXPECT_TRUE(success);
}