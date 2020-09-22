#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Util/Logger.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <NodeEngine/NodeEngine.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>

namespace NES {
class ConvertLogicalToPhysicalSourceTest : public testing::Test {
  public:
    NodeEnginePtr engine;

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ConvertLogicalToPhysicalSourceTest test class." << std::endl;
    }

    void SetUp() {
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test instance.");
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::create();
        engine = NodeEngine::create("127.0.0.1", 9090, streamConf);
    }

    void TearDown() {
        NES_INFO("TearDown ConvertLogicalToPhysicalSourceTest test instance.");
        engine.reset();
    }
};

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingCsvFileLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = CsvSourceDescriptor::create(schema, "csv.log", ",", 0, 10, 10);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(csvFileSource->getType(),  CSV_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingBinaryFileLogicalToPhysicalSource) {
    std::string filePath =
        "../tests/test_data/ysb-tuples-100-campaign-100.bin";
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = BinarySourceDescriptor::create(schema, filePath);
    DataSourcePtr binaryFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(binaryFileSource->getType(), BINARY_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingZMQLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = ZmqSourceDescriptor::create(schema, "127.0.0.1", 10000);
    DataSourcePtr zqmSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(zqmSource->getType(), ZMQ_SOURCE);
}
#ifdef ENABLE_KAFKA_BUILD
TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingKafkaLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = KafkaSourceDescriptor::create(schema, "localhost:9092", "topic", /**group Id**/ "groupId", /**auto commit*/ true, /**timeout*/ 1000);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(csvFileSource->getType(), KAFKA_SOURCE);
}
#endif
TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingSenseLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = SenseSourceDescriptor::create(schema, "some_udf");
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(senseSource->getType(), SENSE_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingDefaultLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = DefaultSourceDescriptor::create(schema, /**Number Of Buffers*/ 1, /**Frequency*/1);
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(senseSource->getType(), DEFAULT_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingNetworkLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    Network::NesPartition nesPartition{1, 22, 33, 44};
    SourceDescriptorPtr sourceDescriptor = Network::NetworkSourceDescriptor::create(schema, nesPartition);
    DataSourcePtr networkSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(networkSource->getType(), NETWORK_SOURCE);
}
}

