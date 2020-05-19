#include "gtest/gtest.h"

#include <iostream>

#include <Catalogs/StreamCatalog.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

#include <Topology/TopologyManager.hpp>
#include <API/Schema.hpp>

#include <Util/Logger.hpp>

using namespace NES;
std::string testSchema =
    "Schema::create()->addField(\"id\", BasicType::UINT32)"
    "->addField(\"value\", BasicType::UINT64);";
const std::string defaultLogicalStreamName = "default_logical";

/* - nesTopologyManager ---------------------------------------------------- */
class StreamCatalogTest : public testing::Test {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        std::cout << "Setup StreamCatalogTest test class." << std::endl;
    }

    /* Will be called before a test is executed. */
    void SetUp() {
        NES::setupLogging("StreamCatalogTest.log", NES::LOG_DEBUG);
        std::cout << "Setup StreamCatalogTest test case." << std::endl;
    }

    /* Will be called before a test is executed. */
    void TearDown() {
        std::cout << "Tear down StreamCatalogTest test case." << std::endl;
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        std::cout << "Tear down StreamCatalogTest test class." << std::endl;
    }
};

TEST_F(StreamCatalogTest, testAddGetLogStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    streamCatalog->addLogicalStream(
        "test_stream", Schema::create());
    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream(
        "test_stream");
    EXPECT_NE(sPtr, nullptr);

    map<std::string, SchemaPtr> allLogicalStream = streamCatalog->getAllLogicalStream();
    string exp = "id:UINT32 value:UINT64 ";
    EXPECT_EQ(allLogicalStream.size(), 3);

    SchemaPtr testSchema = allLogicalStream["test_stream"];
    EXPECT_EQ("", testSchema->toString());

    SchemaPtr defaultSchema = allLogicalStream["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());
}

TEST_F(StreamCatalogTest, testAddRemoveLogStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();


    streamCatalog->addLogicalStream(
        "test_stream", Schema::create());

    EXPECT_TRUE(streamCatalog->removeLogicalStream("test_stream"));

    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream(
        "test_stream");
    EXPECT_EQ(sPtr, nullptr);

    string exp =
        "logical stream name=default_logical schema: name=id UINT32 name=value UINT64\n\nlogical stream name=test_stream schema:\n\n";

    map<std::string, SchemaPtr> allLogicalStream = streamCatalog->getAllLogicalStream();

    EXPECT_NE(1, allLogicalStream.size());
    EXPECT_FALSE(streamCatalog->removeLogicalStream("test_stream22"));
}

TEST_F(StreamCatalogTest, testGetNotExistingKey) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    SchemaPtr sPtr = streamCatalog->getSchemaForLogicalStream(
        "test_stream22");
    EXPECT_EQ(sPtr, nullptr);
}

TEST_F(StreamCatalogTest, testAddGetPhysicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();

    streamCatalog->addLogicalStream(
        "test_stream", Schema::create());

    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    streamConf.logicalStreamName = "test_stream";

    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);

    EXPECT_TRUE(
        streamCatalog->addPhysicalStream(streamConf.logicalStreamName,
                                                    sce));

    std::string expected =
        "stream name=test_stream with 1 elements:physicalName=test2 logicalStreamName=test_stream sourceType=DefaultSource sourceConfig=1 sourceFrequency=0 numberOfBuffersToProduce=1 on node=1\n";
    cout << " string="
         << streamCatalog->getPhysicalStreamAndSchemaAsString()
         << endl;

    EXPECT_EQ(expected,
              streamCatalog->getPhysicalStreamAndSchemaAsString());
}

//TODO: add test for a second physical stream add

TEST_F(StreamCatalogTest, testAddRemovePhysicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();

    streamCatalog->addLogicalStream(
        "test_stream", Schema::create());

    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    streamConf.physicalStreamName = "test2";
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);

    EXPECT_TRUE(
        streamCatalog->addPhysicalStream(streamConf.logicalStreamName,
                                                    sce));

    EXPECT_TRUE(
        streamCatalog->removePhysicalStream(
            streamConf.logicalStreamName, streamConf.physicalStreamName,
            sensorNode->getId()));

    cout << streamCatalog->getPhysicalStreamAndSchemaAsString()
         << endl;
}

TEST_F(StreamCatalogTest, testAddPhysicalForNotExistingLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();

    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(1, "localhost", CPUCapacity::HIGH);

    PhysicalStreamConfig streamConf;
    StreamCatalogEntryPtr sce = std::make_shared<StreamCatalogEntry>(streamConf,
                                                                     sensorNode);

    EXPECT_TRUE(
        streamCatalog->addPhysicalStream(streamConf.logicalStreamName,
                                                    sce));

    std::string expected =
        "stream name=default_logical with 1 elements:physicalName=default_physical logicalStreamName=default_logical sourceType=DefaultSource sourceConfig=1 sourceFrequency=0 numberOfBuffersToProduce=1 on node=1\n";
    EXPECT_EQ(expected,
              streamCatalog->getPhysicalStreamAndSchemaAsString());
}
//new from service
TEST_F(StreamCatalogTest, testGetAllLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    const map<std::string, std::string>& allLogicalStream =
        streamCatalog->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 2);
    for (auto const[key, value] : allLogicalStream) {
        bool cmp = key != defaultLogicalStreamName && key != "exdra";
        EXPECT_EQ(cmp, false);
    }
}

TEST_F(StreamCatalogTest, testAddLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    streamCatalog->addLogicalStream("test", testSchema);
    const map<std::string, std::string>& allLogicalStream =
        streamCatalog->getAllLogicalStreamAsString();
    EXPECT_EQ(allLogicalStream.size(), 3);
}

TEST_F(StreamCatalogTest, testGetPhysicalStreamForLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();
    TopologyManagerPtr topologyManager = std::make_shared<TopologyManager>();

    std::string newLogicalStreamName = "test_stream";

    streamCatalog->addLogicalStream(newLogicalStreamName,
                                               testSchema);

    NESTopologySensorNodePtr sensorNode = topologyManager->createNESSensorNode(
        1, "127.0.0.1", CPUCapacity::LOW);

    PhysicalStreamConfig conf;
    conf.sourceType = "sensor";
    StreamCatalogEntryPtr catalogEntryPtr = std::make_shared<StreamCatalogEntry>(
        conf, sensorNode);
    streamCatalog->addPhysicalStream(newLogicalStreamName,
                                                catalogEntryPtr);
    const vector<StreamCatalogEntryPtr>& allPhysicalStream =
        streamCatalog->getPhysicalStreams(newLogicalStreamName);
    EXPECT_EQ(allPhysicalStream.size(), 1);
}

TEST_F(StreamCatalogTest, testDeleteLogicalStream) {
    StreamCatalogPtr streamCatalog = std::make_shared<StreamCatalog>();

    bool success = streamCatalog->removeLogicalStream(
        defaultLogicalStreamName);
    EXPECT_TRUE(success);
}