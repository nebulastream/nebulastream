#include "gtest/gtest.h"
#include <API/Schema.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/KafkaSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>

#include <Nodes/Phases/ConvertLogicalToPhysicalSink.hpp>
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
    SinkDescriptorPtr sinkDescriptor = FileSinkDescriptor::create("file.log");
    DataSinkPtr fileOutputSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor);
    EXPECT_EQ(fileOutputSink->getType(), FILE_SINK);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingZMQLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = ZmqSinkDescriptor::create("loclahost", 2000);
    DataSinkPtr zmqSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor);
    EXPECT_EQ(zmqSink->getType(), ZMQ_SINK);
}
#ifdef ENABLE_KAFKA_BUILD
TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingKafkaLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = KafkaSinkDescriptor::create("test", "localhost:9092", 1000);
    DataSinkPtr kafkaSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor);
    EXPECT_EQ(kafkaSink->getType(), KAFKA_SINK);
}
#endif
TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingPrintLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = PrintSinkDescriptor::create();
    DataSinkPtr printSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor);
    EXPECT_EQ(printSink->getType(), PRINT_SINK);
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingNetworkLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    Network::NodeLocation nodeLocation{1, "localhost", 31337};
    Network::NesPartition nesPartition{1, 22, 33, 44};
    SinkDescriptorPtr sinkDescriptor = Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition,std::chrono::seconds(1), 1);
    DataSinkPtr networkSink = ConvertLogicalToPhysicalSink::createDataSink(schema, sinkDescriptor);
    EXPECT_EQ(networkSink->getType(), NETWORK_SINK);
}

}// namespace NES
