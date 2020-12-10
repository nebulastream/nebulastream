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
#include <NodeEngine/NodeEngine.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Util/Logger.hpp>

namespace NES {
class ConvertLogicalToPhysicalSourceTest : public testing::Test {
  public:
    NodeEnginePtr engine;

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down ConvertLogicalToPhysicalSourceTest test class." << std::endl; }

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
    SourceDescriptorPtr sourceDescriptor = CsvSourceDescriptor::create(schema, "testStream", "csv.log", ",", 0, 10, 1, false, 1);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(csvFileSource->getType(), CSV_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingBinaryFileLogicalToPhysicalSource) {
    std::string filePath = "../tests/test_data/ysb-tuples-100-campaign-100.bin";
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = BinarySourceDescriptor::create(schema, filePath, 1);
    DataSourcePtr binaryFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(binaryFileSource->getType(), BINARY_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingZMQLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = ZmqSourceDescriptor::create(schema, "127.0.0.1", 10000, 1);
    DataSourcePtr zqmSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(zqmSource->getType(), ZMQ_SOURCE);
}
#ifdef ENABLE_KAFKA_BUILD
TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingKafkaLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = KafkaSourceDescriptor::create(
        schema, "localhost:9092", "topic", /**group Id**/ "groupId", /**auto commit*/ true, /**timeout*/ 1000);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(csvFileSource->getType(), KAFKA_SOURCE);
}
#endif
TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingSenseLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = SenseSourceDescriptor::create(schema, "some_udf", 1);
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor, engine);
    EXPECT_EQ(senseSource->getType(), SENSE_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingDefaultLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = DefaultSourceDescriptor::create(schema, /**Number Of Buffers*/ 1, /**Frequency*/ 1, 1);
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
}// namespace NES
