#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSinkTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ConvertLogicalToPhysicalSourceTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingCsvFileLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = std::make_shared<CsvSourceDescriptor>(schema, "csv.log", ",", 10, 10);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(csvFileSource->getType(), CSV_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingBinaryFileLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = std::make_shared<BinarySourceDescriptor>(schema, "binary.log");
    DataSourcePtr binaryFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(binaryFileSource->getType(), BINARY_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingZMQLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = std::make_shared<ZmqSourceDescriptor>(schema, "localhost", 10000);
    DataSourcePtr zqmSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(zqmSource->getType(), ZMQ_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingKafkaLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    const cppkafka::Configuration kafkaConfig = {};
    SourceDescriptorPtr sourceDescriptor = std::make_shared<KafkaSourceDescriptor>(schema, "localhost:9092", "topic", /**auto commit*/ true, kafkaConfig, /**timeout*/ 1000);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(csvFileSource->getType(), KAFKA_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingSenseLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = std::make_shared<SenseSourceDescriptor>(schema, "some_udf");
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(senseSource->getType(), SENSE_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingDefaultLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = std::make_shared<DefaultSourceDescriptor>(schema, /**Number Of Buffers*/ 1, /**Frequency*/1);
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(senseSource->getType(), SENSE_SOURCE);
}
}

