#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSinkTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSinkTest test class.");
    }

    static void TearDownTestCase() {
        std::cout << "Tear down ConvertLogicalToPhysicalSinkTest test class." << std::endl;
    }

    void TearDown() {
    }
};

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingFileLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = std::make_shared<FileSinkDescriptor>(schema,
                                                                            "file.log",
                                                                            FileOutPutMode::FILE_OVERWRITE,
                                                                            FileOutPutType::CSV_TYPE);
    DataSinkPtr fileOutputSink = ConvertLogicalToPhysicalSink::createDataSink(sinkDescriptor);
    EXPECT_EQ(fileOutputSink->getType(), FILE_SINK);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingZMQLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = std::make_shared<ZmqSinkDescriptor>(schema, "loclahost", 2000);
    DataSinkPtr zmqSink = ConvertLogicalToPhysicalSink::createDataSink(sinkDescriptor);
    EXPECT_EQ(zmqSink->getType(), ZMQ_SINK);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingKafkaLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    const cppkafka::Configuration kafkaConfig = {{"metadata.broker.list", "localhost:9092"}};
    SinkDescriptorPtr sinkDescriptor = std::make_shared<KafkaSinkDescriptor>(schema, "test", kafkaConfig);
    DataSinkPtr kafkaSink = ConvertLogicalToPhysicalSink::createDataSink(sinkDescriptor);
    EXPECT_EQ(kafkaSink->getType(), KAFKA_SINK);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingPrintLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = std::make_shared<PrintSinkDescriptor>(schema, std::cout);
    DataSinkPtr printSink = ConvertLogicalToPhysicalSink::createDataSink(sinkDescriptor);
    EXPECT_EQ(printSink->getType(), PRINT_SINK);
}

}

