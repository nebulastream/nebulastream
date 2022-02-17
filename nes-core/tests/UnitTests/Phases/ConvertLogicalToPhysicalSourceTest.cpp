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
#include <API/Schema.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <Phases/ConvertLogicalToPhysicalSource.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Util/Logger.hpp>

namespace NES {
using namespace Configurations;
static constexpr auto NSOURCE_RETRIES = 100;
static constexpr auto NSOURCE_RETRY_WAIT = std::chrono::milliseconds(5);
class ConvertLogicalToPhysicalSourceTest : public testing::Test {
  public:
    Runtime::NodeEnginePtr engine;

    static void SetUpTestCase() {
        NES::setupLogging("ConvertLogicalToPhysicalSourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test class.");
    }

    static void TearDownTestCase() { std::cout << "Tear down ConvertLogicalToPhysicalSourceTest test class." << std::endl; }

    void SetUp() override {
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test instance.");
        PhysicalSourcePtr physicalSource = PhysicalSource::create("x", "x1");
        engine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                              0,
                                                              {physicalSource},
                                                              1,
                                                              4096,
                                                              1024,
                                                              12,
                                                              12,
                                                              Configurations::QueryCompilerConfiguration());
    }

    void TearDown() override {
        NES_INFO("TearDown ConvertLogicalToPhysicalSourceTest test instance.");
        ASSERT_TRUE(engine->stop());
        engine.reset();
    }
};

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingCsvFileLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    auto csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath("csv.log");
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setSourceFrequency(1);
    SourceDescriptorPtr sourceDescriptor = CsvSourceDescriptor::create(schema, csvSourceType);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(1, sourceDescriptor, engine, 12);
    EXPECT_EQ(csvFileSource->getType(), CSV_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingBinaryFileLogicalToPhysicalSource) {
    std::string filePath = std::string(TEST_DATA_DIRECTORY) + "ysb-tuples-100-campaign-100.bin";
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = BinarySourceDescriptor::create(schema, filePath);
    DataSourcePtr binaryFileSource = ConvertLogicalToPhysicalSource::createDataSource(1, sourceDescriptor, engine, 12);
    EXPECT_EQ(binaryFileSource->getType(), BINARY_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingZMQLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = ZmqSourceDescriptor::create(schema, "127.0.0.1", 10000);
    DataSourcePtr zqmSource = ConvertLogicalToPhysicalSource::createDataSource(1, sourceDescriptor, engine, 12);
    EXPECT_EQ(zqmSource->getType(), ZMQ_SOURCE);
}
#ifdef ENABLE_KAFKA_BUILD
TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingKafkaLogiclaToPhysicalSource) {

    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = KafkaSourceDescriptor::create(schema,
                                                                         "localhost:9092",
                                                                         "topic",
                                                                         /**group Id**/ "groupId",
                                                                         /**auto commit*/ true,
                                                                         /**timeout*/ 1000);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(sourceDescriptor);
    EXPECT_EQ(csvFileSource->getType(), KAFKA_SOURCE);
}
#endif
TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingSenseLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = SenseSourceDescriptor::create(schema, "some_udf");
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(1, sourceDescriptor, engine, 12);
    EXPECT_EQ(senseSource->getType(), SENSE_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingDefaultLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = DefaultSourceDescriptor::create(schema, /**Number Of Buffers*/ 1, /**Frequency*/ 1000);
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(1, sourceDescriptor, engine, 12);
    EXPECT_EQ(senseSource->getType(), DEFAULT_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingNetworkLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    Network::NesPartition nesPartition{1, 22, 33, 44};
    Network::NodeLocation nodeLocation(0, "*", 31337);
    SourceDescriptorPtr sourceDescriptor =
        Network::NetworkSourceDescriptor::create(schema, nesPartition, nodeLocation, NSOURCE_RETRY_WAIT, NSOURCE_RETRIES);
    DataSourcePtr networkSource = ConvertLogicalToPhysicalSource::createDataSource(1, sourceDescriptor, engine, 12);
    EXPECT_EQ(networkSource->getType(), NETWORK_SOURCE);
}
}// namespace NES
