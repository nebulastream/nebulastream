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

#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/BinarySourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/DefaultSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/KafkaSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SenseSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/TCPSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/ZmqSourceDescriptor.hpp>
#include <QueryCompiler/Phases/Translations/ConvertLogicalToPhysicalSource.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>

namespace NES {
using namespace Configurations;
static constexpr auto NSOURCE_RETRIES = 100;
static constexpr auto NSOURCE_RETRY_WAIT = std::chrono::milliseconds(5);
class ConvertLogicalToPhysicalSourceTest : public Testing::BaseUnitTest {
  public:
    Runtime::NodeEnginePtr engine;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("ConvertLogicalToPhysicalSourceTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test class.");
    }

    void SetUp() override {
        Testing::BaseUnitTest::SetUp();
        NES_INFO("Setup ConvertLogicalToPhysicalSourceTest test instance.");
        auto physicalSource = CSVSourceType::create("x", "x1");
        auto workerConfiguration = WorkerConfiguration::create();
        workerConfiguration->physicalSourceTypes.add(physicalSource);
        workerConfiguration->numberOfBuffersInSourceLocalBufferPool.setValue(12);
        workerConfiguration->numberOfBuffersPerWorker.setValue(12);

        engine = Runtime::NodeEngineBuilder::create(workerConfiguration)
                     .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                     .build();
    }

    void TearDown() override {
        NES_INFO("TearDown ConvertLogicalToPhysicalSourceTest test instance.");
        ASSERT_TRUE(engine->stop());
        engine.reset();
        Testing::BaseUnitTest::TearDown();
    }
};

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingCsvFileLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    auto csvSourceType = CSVSourceType::create("logical", "physical");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "QnV_short_R2000073.csv");
    csvSourceType->setNumberOfBuffersToProduce(10);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
    csvSourceType->setGatheringInterval(1000);
    SourceDescriptorPtr sourceDescriptor = CsvSourceDescriptor::create(schema, csvSourceType);
    DataSourcePtr csvFileSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(csvFileSource->getType(), SourceType::CSV_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingTCPLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    auto tcpSourceType = TCPSourceType::create("logical", "physical");
    SourceDescriptorPtr sourceDescriptor = TCPSourceDescriptor::create(schema, tcpSourceType);
    DataSourcePtr tcpSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(tcpSource->getType(), SourceType::TCP_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingBinaryFileLogicalToPhysicalSource) {
    std::string filePath = std::filesystem::path(TEST_DATA_DIRECTORY) / "ysb-tuples-100-campaign-100.bin";
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = BinarySourceDescriptor::create(schema, filePath);
    DataSourcePtr binaryFileSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(binaryFileSource->getType(), SourceType::BINARY_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingZMQLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = ZmqSourceDescriptor::create(schema, "127.0.0.1", 10000);
    DataSourcePtr zqmSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(zqmSource->getType(), SourceType::ZMQ_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingSenseLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = SenseSourceDescriptor::create(schema, "some_udf");
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(senseSource->getType(), SourceType::SENSE_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingDefaultLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    SourceDescriptorPtr sourceDescriptor = DefaultSourceDescriptor::create(schema, /**Number Of Buffers*/ 1, /**Frequency*/ 1000);
    DataSourcePtr senseSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(senseSource->getType(), SourceType::DEFAULT_SOURCE);
}

TEST_F(ConvertLogicalToPhysicalSourceTest, testConvertingNetworkLogicalToPhysicalSource) {
    SchemaPtr schema = Schema::create();
    Network::NesPartition nesPartition{1, 22, 33, 44};
    Network::NodeLocation nodeLocation(0, "*", 31337);
    DecomposedQueryPlanVersion version = 0;
    SourceDescriptorPtr sourceDescriptor = Network::NetworkSourceDescriptor::create(schema,
                                                                                    nesPartition,
                                                                                    nodeLocation,
                                                                                    NSOURCE_RETRY_WAIT,
                                                                                    NSOURCE_RETRIES,
                                                                                    version);
    DataSourcePtr networkSource = ConvertLogicalToPhysicalSource::createDataSource(1, 0, sourceDescriptor, engine, 12);
    EXPECT_EQ(networkSource->getType(), SourceType::NETWORK_SOURCE);
}
}// namespace NES
