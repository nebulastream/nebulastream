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
#include <API/Schema.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Operators/LogicalOperators/Sinks/FileSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/PrintSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sinks/ZmqSinkDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSink.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSinkTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSinkTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSinkTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down ConvertLogicalToPhysicalSinkTest test class." << std::endl; }

    void SetUp() override {
        PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();
        nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 12345, conf);
        testPlan = QueryCompilation::PipelineQueryPlan::create(0, 0);
    }

    void TearDown() override {
        nodeEngine->stop();
        nodeEngine.reset();
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    QueryCompilation::PipelineQueryPlanPtr testPlan;
};

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingFileLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = FileSinkDescriptor::create("file.log", "CSV_FORMAT", "APPEND");
    SinkLogicalOperatorNodePtr testSink = std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, 0);
    testSink->setOutputSchema(schema);
    DataSinkPtr fileOutputSink =
        ConvertLogicalToPhysicalSink::createDataSink(testSink->getId(), sinkDescriptor, schema, nodeEngine, testPlan, 1);
    EXPECT_EQ(fileOutputSink->toString(), "FileSink(SCHEMA())");
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingZMQLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = ZmqSinkDescriptor::create("127.0.0.1", 2000);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();

    SinkLogicalOperatorNodePtr testSink = std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, 0);
    DataSinkPtr zmqSink =
        ConvertLogicalToPhysicalSink::createDataSink(testSink->getId(), sinkDescriptor, schema, nodeEngine, testPlan, 0);
    EXPECT_EQ(zmqSink->toString(), "ZMQ_SINK(SCHEMA(), HOST=127.0.0.1, PORT=2000)");
}
#ifdef ENABLE_KAFKA_BUILD
TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingKafkaLogicalToPhysicalSink) {

    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = KafkaSinkDescriptor::create("test", "localhost:9092", 1000);

    SinkLogicalOperatorNodePtr testSink = std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, 0);
    testSink->setOutputSchema(schema);
    DataSinkPtr kafkaSink = ConvertLogicalToPhysicalSink::createDataSink(testSink);
    EXPECT_EQ(kafkaSink->getType(), KAFKA_SINK);
}
#endif
TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingPrintLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    SinkDescriptorPtr sinkDescriptor = PrintSinkDescriptor::create();
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();

    SinkLogicalOperatorNodePtr testSink = std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, 0);
    testSink->setOutputSchema(schema);
    DataSinkPtr printSink =
        ConvertLogicalToPhysicalSink::createDataSink(testSink->getId(), sinkDescriptor, schema, nodeEngine, testPlan, 0);
    EXPECT_EQ(printSink->toString(), "PRINT_SINK(SCHEMA())");
}

TEST_F(ConvertLogicalToPhysicalSinkTest, testConvertingNetworkLogicalToPhysicalSink) {
    SchemaPtr schema = Schema::create();
    Network::NodeLocation nodeLocation{1, "localhost", 31337};
    Network::NesPartition nesPartition{1, 22, 33, 44};
    SinkDescriptorPtr sinkDescriptor =
        Network::NetworkSinkDescriptor::create(nodeLocation, nesPartition, std::chrono::seconds(1), 1);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::createEmpty();

    SinkLogicalOperatorNodePtr testSink = std::make_shared<SinkLogicalOperatorNode>(sinkDescriptor, 0);
    testSink->setOutputSchema(schema);
    DataSinkPtr networkSink =
        ConvertLogicalToPhysicalSink::createDataSink(testSink->getId(), sinkDescriptor, schema, nodeEngine, testPlan, 0);
    EXPECT_EQ(networkSink->toString(), "NetworkSink: 1::22::33::44");
}

}// namespace NES
