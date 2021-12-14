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

#define _TURN_OFF_PLATFORM_STRING// for cpprest/details/basic_types.h
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/QueryManager.hpp>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>

#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger.hpp>

#include <Monitoring/MetricValues/GroupedMetricValues.hpp>
#include <Monitoring/MetricValues/MetricValueType.hpp>
#include <Monitoring/Metrics/MetricCatalog.hpp>
#include <Monitoring/Metrics/MetricGroup.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Monitoring/Util/MetricUtils.hpp>

#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Services/QueryService.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/LambdaSource.hpp>
#include <Sources/MonitoringSource.hpp>
#include <Util/TestUtils.hpp>

#include <Runtime/WorkerContext.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES {

struct __attribute__((packed)) ysbRecord {
    char user_id[16]{};
    char page_id[16]{};
    char campaign_id[16]{};
    char ad_type[9]{};
    char event_type[9]{};
    int64_t current_ms;
    uint32_t ip;

    ysbRecord() {
        event_type[0] = '-';// invalid record
        current_ms = 0;
        ip = 0;
    }

    ysbRecord(const ysbRecord& rhs) {
        memcpy(&user_id, &rhs.user_id, 16);
        memcpy(&page_id, &rhs.page_id, 16);
        memcpy(&campaign_id, &rhs.campaign_id, 16);
        memcpy(&ad_type, &rhs.ad_type, 9);
        memcpy(&event_type, &rhs.event_type, 9);
        current_ms = rhs.current_ms;
        ip = rhs.ip;
    }
};
// size 78 bytes

struct __attribute__((packed)) everyIntTypeRecord {
    uint64_t uint64_entry;
    int64_t int64_entry;
    uint32_t uint32_entry;
    int32_t int32_entry;
    uint16_t uint16_entry;
    int16_t int16_entry;
    uint8_t uint8_entry;
    int8_t int8_entry;
};

struct __attribute__((packed)) everyFloatTypeRecord {
    double float64_entry;
    float float32_entry;
};

struct __attribute__((packed)) everyBooleanTypeRecord {
    bool false_entry;
    bool true_entry;
    bool falsey_entry;
    bool truthy_entry;
};

struct __attribute__((packed)) IngestionRecord {
    IngestionRecord(uint64_t userId,
                    uint64_t pageId,
                    uint64_t campaignId,
                    uint64_t adType,
                    uint64_t eventType,
                    uint64_t currentMs,
                    uint64_t ip)
        : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType), currentMs(currentMs),
          ip(ip) {}

    uint64_t userId;
    uint64_t pageId;
    uint64_t campaignId;
    uint64_t adType;
    uint64_t eventType;
    uint64_t currentMs;
    uint64_t ip;

    // placeholder to reach 78 bytes
    uint64_t dummy1{0};
    uint64_t dummy2{0};
    uint32_t dummy3{0};
    uint16_t dummy4{0};

    IngestionRecord(const IngestionRecord& rhs) {
        userId = rhs.userId;
        pageId = rhs.pageId;
        campaignId = rhs.campaignId;
        adType = rhs.adType;
        eventType = rhs.eventType;
        currentMs = rhs.currentMs;
        ip = rhs.ip;
    }

    [[nodiscard]] std::string toString() const {
        return "Record(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId) + ", campaignId="
            + std::to_string(campaignId) + ", adType=" + std::to_string(adType) + ", eventType=" + std::to_string(eventType)
            + ", currentMs=" + std::to_string(currentMs) + ", ip=" + std::to_string(ip);
    }
};

using ::testing::_;
using ::testing::AnyNumber;
using ::testing::Exactly;
using ::testing::InvokeWithoutArgs;
using ::testing::Mock;
using ::testing::Return;

// testing w/ original running routine
class MockDataSource : public DataSource {
  public:
    MockDataSource(const SchemaPtr& schema,
                   Runtime::BufferManagerPtr bufferManager,
                   Runtime::QueryManagerPtr queryManager,
                   OperatorId operatorId,
                   size_t numSourceLocalBuffers,
                   GatheringMode gatheringMode,
                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
        : DataSource(schema, bufferManager, queryManager, operatorId, numSourceLocalBuffers, gatheringMode, executableSuccessors){
            // nop
        };
    MOCK_METHOD(void, runningRoutineWithFrequency, ());
    MOCK_METHOD(void, runningRoutineWithIngestionRate, ());
    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
    MOCK_METHOD(std::string, toString, (), (const));
    MOCK_METHOD(SourceType, getType, (), (const));
};

// testing w/ mocked running routine (no need for actual routines)
class MockDataSourceWithRunningRoutine : public DataSource {
  public:
    MockDataSourceWithRunningRoutine(const SchemaPtr& schema,
                                     Runtime::BufferManagerPtr bufferManager,
                                     Runtime::QueryManagerPtr queryManager,
                                     OperatorId operatorId,
                                     size_t numSourceLocalBuffers,
                                     GatheringMode gatheringMode,
                                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
        : DataSource(schema, bufferManager, queryManager, operatorId, numSourceLocalBuffers, gatheringMode, executableSuccessors){
            // nop
        };
    MOCK_METHOD(void, runningRoutine, ());
    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
    MOCK_METHOD(std::string, toString, (), (const));
    MOCK_METHOD(SourceType, getType, (), (const));

  private:
    FRIEND_TEST(SourceTest, testDataSourceHardStopSideEffect);
    FRIEND_TEST(SourceTest, testDataSourceGracefulStopSideEffect);
};

// proxy friendly to test class, exposing protected members
class DataSourceProxy : public DataSource, public Runtime::BufferRecycler {
  public:
    DataSourceProxy(const SchemaPtr& schema,
                    Runtime::BufferManagerPtr bufferManager,
                    Runtime::QueryManagerPtr queryManager,
                    OperatorId operatorId,
                    size_t numSourceLocalBuffers,
                    GatheringMode gatheringMode,
                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
        : DataSource(schema,
                     bufferManager,
                     queryManager,
                     operatorId,
                     numSourceLocalBuffers,
                     gatheringMode,
                     executableSuccessors){};

    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
    MOCK_METHOD(std::string, toString, (), (const));
    MOCK_METHOD(SourceType, getType, (), (const));
    MOCK_METHOD(void, emitWork, (Runtime::TupleBuffer & buffer));
    MOCK_METHOD(void, emitWorkFromSource, (Runtime::TupleBuffer & buffer));
    MOCK_METHOD(void, recycleUnpooledBuffer, (Runtime::detail::MemorySegment * buffer));

    Runtime::TupleBuffer getRecyclableBuffer() {
        auto* p = new uint8_t[5];
        auto fakeBuffer = Runtime::TupleBuffer::wrapMemory(p, sizeof(uint8_t) * 5, this);
        return fakeBuffer;
    }

    void recyclePooledBuffer(Runtime::detail::MemorySegment* buffer) { delete buffer; }

  private:
    FRIEND_TEST(SourceTest, testDataSourceFrequencyRoutineBufWithValue);
    FRIEND_TEST(SourceTest, testDataSourceIngestionRoutineBufWithValue);
    FRIEND_TEST(SourceTest, testDataSourceOpen);
};
using DataSourceProxyPtr = std::shared_ptr<DataSourceProxy>;

class BinarySourceProxy : public BinarySource {
  public:
    BinarySourceProxy(const SchemaPtr& schema,
                      Runtime::BufferManagerPtr bufferManager,
                      Runtime::QueryManagerPtr queryManager,
                      const std::string& file_path,
                      OperatorId operatorId,
                      size_t numSourceLocalBuffers,
                      std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : BinarySource(schema,
                       bufferManager,
                       queryManager,
                       file_path,
                       operatorId,
                       numSourceLocalBuffers,
                       DataSource::FREQUENCY_MODE,
                       successors){};

  private:
    FRIEND_TEST(SourceTest, testBinarySourceGetType);
    FRIEND_TEST(SourceTest, testBinarySourceWrongPath);
    FRIEND_TEST(SourceTest, testBinarySourceCorrectPath);
    FRIEND_TEST(SourceTest, testBinarySourceFillBuffer);
    FRIEND_TEST(SourceTest, testBinarySourceFillBufferRandomTimes);
    FRIEND_TEST(SourceTest, testBinarySourceFillBufferContents);
};

class CSVSourceProxy : public CSVSource {
  public:
    CSVSourceProxy(SchemaPtr schema,
                   Runtime::BufferManagerPtr bufferManager,
                   Runtime::QueryManagerPtr queryManager,
                   Configurations::CSVSourceConfigPtr sourceConfig,
                   OperatorId operatorId,
                   size_t numSourceLocalBuffers,
                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : CSVSource(schema,
                    bufferManager,
                    queryManager,
                    sourceConfig,
                    operatorId,
                    numSourceLocalBuffers,
                    DataSource::FREQUENCY_MODE,
                    successors){};

  private:
    FRIEND_TEST(SourceTest, testCSVSourceGetType);
    FRIEND_TEST(SourceTest, testCSVSourceWrongFilePath);
    FRIEND_TEST(SourceTest, testCSVSourceCorrectFilePath);
    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFileEnded);
    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFullFile);
    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFullFileColumnLayout);
    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFullFileOnLoop);
};

class GeneratorSourceProxy : public GeneratorSource {
  public:
    GeneratorSourceProxy(SchemaPtr schema,
                         Runtime::BufferManagerPtr bufferManager,
                         Runtime::QueryManagerPtr queryManager,
                         uint64_t numbersOfBufferToProduce,
                         OperatorId operatorId,
                         size_t numSourceLocalBuffers,
                         GatheringMode gatheringMode,
                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : GeneratorSource(schema,
                          bufferManager,
                          queryManager,
                          numbersOfBufferToProduce,
                          operatorId,
                          numSourceLocalBuffers,
                          gatheringMode,
                          successors){};
    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
};

class DefaultSourceProxy : public DefaultSource {
  public:
    DefaultSourceProxy(SchemaPtr schema,
                       Runtime::BufferManagerPtr bufferManager,
                       Runtime::QueryManagerPtr queryManager,
                       const uint64_t numbersOfBufferToProduce,
                       uint64_t frequency,
                       OperatorId operatorId,
                       size_t numSourceLocalBuffers,
                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : DefaultSource(schema,
                        bufferManager,
                        queryManager,
                        numbersOfBufferToProduce,
                        frequency,
                        operatorId,
                        numSourceLocalBuffers,
                        successors){};
};

class LambdaSourceProxy : public LambdaSource {
  public:
    LambdaSourceProxy(
        SchemaPtr schema,
        Runtime::BufferManagerPtr bufferManager,
        Runtime::QueryManagerPtr queryManager,
        uint64_t numbersOfBufferToProduce,
        uint64_t gatheringValue,
        std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
        OperatorId operatorId,
        size_t numSourceLocalBuffers,
        GatheringMode gatheringMode,
        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : LambdaSource(schema,
                       bufferManager,
                       queryManager,
                       numbersOfBufferToProduce,
                       gatheringValue,
                       std::move(generationFunction),
                       operatorId,
                       numSourceLocalBuffers,
                       gatheringMode,
                       successors){};

  private:
    FRIEND_TEST(SourceTest, testLambdaSourceInitAndTypeFrequency);
    FRIEND_TEST(SourceTest, testLambdaSourceInitAndTypeIngestion);
};

class MonitoringSourceProxy : public MonitoringSource {
  public:
    MonitoringSourceProxy(const MonitoringPlanPtr& monitoringPlan,
                          MetricCatalogPtr metricCatalog,
                          Runtime::BufferManagerPtr bufferManager,
                          Runtime::QueryManagerPtr queryManager,
                          const uint64_t numbersOfBufferToProduce,
                          uint64_t frequency,
                          OperatorId operatorId,
                          size_t numSourceLocalBuffers,
                          std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
        : MonitoringSource(monitoringPlan,
                           metricCatalog,
                           bufferManager,
                           queryManager,
                           numbersOfBufferToProduce,
                           frequency,
                           operatorId,
                           numSourceLocalBuffers,
                           successors){};
};

class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
  public:
    MockedPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, DataSinkPtr sink)
        : PipelineExecutionContext(
            0,
            std::move(queryManager),
            [sink](Runtime::TupleBuffer& buffer, Runtime::WorkerContextRef worker) {
                sink->writeData(buffer, worker);
            },
            [sink](Runtime::TupleBuffer&) {
            },
            std::vector<Runtime::Execution::OperatorHandlerPtr>()){};
};

class MockedExecutablePipeline : public Runtime::Execution::ExecutablePipelineStage {
  public:
    std::atomic<uint64_t> count = 0;
    std::promise<bool> completedPromise;

    ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
                            Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
                            Runtime::WorkerContext& wctx) override {
        count += inputTupleBuffer.getNumberOfTuples();

        Runtime::TupleBuffer outputBuffer = wctx.allocateTupleBuffer();
        auto arr = outputBuffer.getBuffer<uint32_t>();
        arr[0] = static_cast<uint32_t>(count.load());
        outputBuffer.setNumberOfTuples(count);
        pipelineExecutionContext.emitBuffer(outputBuffer, wctx);
        completedPromise.set_value(true);
        return ExecutionResult::Ok;
    }
};

class SourceTest : public testing::Test {
  public:
    void SetUp() override {
        PhysicalStreamConfigPtr streamConf = PhysicalStreamConfig::createEmpty();
        this->nodeEngine = Runtime::NodeEngineFactory::createDefaultNodeEngine("127.0.0.1", 31337, streamConf);
        this->path_to_file = "../tests/test_data/ysb-tuples-100-campaign-100.csv";
        this->path_to_file_head = "../tests/test_data/ysb-tuples-100-campaign-100-head.csv";
        this->path_to_bin_file = "../tests/test_data/ysb-tuples-100-campaign-100.bin";
        this->schema = Schema::create()
                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
                           ->addField("current_ms", UINT64)
                           ->addField("ip", INT32);
        this->lambdaSchema = Schema::create()
                                 ->addField("user_id", UINT64)
                                 ->addField("page_id", UINT64)
                                 ->addField("campaign_id", UINT64)
                                 ->addField("ad_type", UINT64)
                                 ->addField("event_type", UINT64)
                                 ->addField("current_ms", UINT64)
                                 ->addField("ip", UINT64)
                                 ->addField("d1", UINT64)
                                 ->addField("d2", UINT64)
                                 ->addField("d3", UINT32)
                                 ->addField("d4", UINT16);
        this->tuple_size = this->schema->getSchemaSizeInBytes();
        this->buffer_size = this->nodeEngine->getBufferManager()->getBufferSize();
        this->numberOfBuffers = 1;
        this->numberOfTuplesToProcess = this->numberOfBuffers * (this->buffer_size / this->tuple_size);
        this->operatorId = 1;
        this->numSourceLocalBuffersDefault = 12;
        this->frequency = 1000;
        this->wrong_filepath = "this_doesnt_exist";
        this->queryId = 1;
    }

    static void TearDownTestCase() { NES_INFO("Tear down SourceTest test class."); }

    static void SetUpTestCase() {
        NES::setupLogging("SourceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SourceTest test class.");
    }

    void TearDown() override { ASSERT_TRUE(nodeEngine->stop()); }

    std::optional<Runtime::TupleBuffer> GetEmptyBuffer() { return this->nodeEngine->getBufferManager()->getBufferNoBlocking(); }

    DataSourceProxyPtr createDataSourceProxy(const SchemaPtr& schema,
                                             Runtime::BufferManagerPtr bufferManager,
                                             Runtime::QueryManagerPtr queryManager,
                                             OperatorId operatorId,
                                             size_t numSourceLocalBuffers,
                                             DataSource::GatheringMode gatheringMode,
                                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors) {
        return std::make_shared<DataSourceProxy>(schema,
                                                 bufferManager,
                                                 queryManager,
                                                 operatorId,
                                                 numSourceLocalBuffers,
                                                 gatheringMode,
                                                 executableSuccessors);
    }

    std::shared_ptr<Runtime::Execution::ExecutablePipeline>
    createExecutablePipeline(std::shared_ptr<MockedExecutablePipeline> executableStage, std::shared_ptr<SinkMedium> sink) {
        auto context = std::make_shared<MockedPipelineExecutionContext>(this->nodeEngine->getQueryManager(), sink);
        return Runtime::Execution::ExecutablePipeline::create(0, this->queryId, context, executableStage, 1, {sink});
    }

    Runtime::NodeEnginePtr nodeEngine{nullptr};
    std::string path_to_file, path_to_bin_file, wrong_filepath, path_to_file_head;
    SchemaPtr schema, lambdaSchema;
    uint64_t tuple_size, buffer_size, numberOfBuffers, numberOfTuplesToProcess, operatorId, numSourceLocalBuffersDefault,
        frequency, queryId;
};

TEST_F(SourceTest, testDataSourceGetOperatorId) {
    const DataSourcePtr source = createDefaultDataSourceWithSchemaForOneBuffer(this->schema,
                                                                               this->nodeEngine->getBufferManager(),
                                                                               this->nodeEngine->getQueryManager(),
                                                                               this->operatorId,
                                                                               this->numSourceLocalBuffersDefault,
                                                                               {});
    ASSERT_EQ(source->getOperatorId(), this->operatorId);
}

TEST_F(SourceTest, testDataSourceGetSchema) {
    const DataSourcePtr source = createDefaultDataSourceWithSchemaForOneBuffer(this->schema,
                                                                               this->nodeEngine->getBufferManager(),
                                                                               this->nodeEngine->getQueryManager(),
                                                                               this->operatorId,
                                                                               this->numSourceLocalBuffersDefault,
                                                                               {});
    ASSERT_EQ(source->getSchema(), this->schema);
}

TEST_F(SourceTest, testDataSourceRunningImmediately) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ASSERT_FALSE(mDataSource.isRunning());
}

TEST_F(SourceTest, testDataSourceStartSideEffectRunningTrue) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
    EXPECT_TRUE(mDataSource.start());
    EXPECT_TRUE(mDataSource.isRunning());// the publicly visible side-effect
    EXPECT_TRUE(mDataSource.stop(false));
}

TEST_F(SourceTest, testDataSourceStartTwiceNoSideEffect) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
    EXPECT_TRUE(mDataSource.start());
    EXPECT_FALSE(mDataSource.start());
    EXPECT_TRUE(mDataSource.isRunning());// the publicly visible side-effect
    EXPECT_TRUE(mDataSource.stop(false));
}

TEST_F(SourceTest, testDataSourceStopImmediately) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ASSERT_FALSE(mDataSource.stop(false));
}

TEST_F(SourceTest, testDataSourceStopSideEffect) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
    EXPECT_TRUE(mDataSource.start());
    EXPECT_TRUE(mDataSource.isRunning());
    EXPECT_TRUE(mDataSource.stop(false));
    EXPECT_FALSE(mDataSource.isRunning());// the publicly visible side-effect
}

TEST_F(SourceTest, testDataSourceHardStopSideEffect) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
    EXPECT_TRUE(mDataSource.start());
    EXPECT_TRUE(mDataSource.isRunning());
    EXPECT_TRUE(mDataSource.wasGracefullyStopped);
    EXPECT_TRUE(mDataSource.stop(true));
    EXPECT_FALSE(mDataSource.isRunning());
    EXPECT_TRUE(mDataSource.wasGracefullyStopped);// private side-effect, use FRIEND_TEST
}

TEST_F(SourceTest, testDataSourceGracefulStopSideEffect) {
    MockDataSourceWithRunningRoutine mDataSource(this->schema,
                                                 this->nodeEngine->getBufferManager(),
                                                 this->nodeEngine->getQueryManager(),
                                                 this->operatorId,
                                                 this->numSourceLocalBuffersDefault,
                                                 DataSource::GatheringMode::FREQUENCY_MODE,
                                                 {});
    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
    EXPECT_TRUE(mDataSource.start());
    EXPECT_TRUE(mDataSource.isRunning());
    EXPECT_TRUE(mDataSource.wasGracefullyStopped);
    EXPECT_TRUE(mDataSource.stop(false));
    EXPECT_FALSE(mDataSource.isRunning());
    EXPECT_FALSE(mDataSource.wasGracefullyStopped);// private side-effect, use FRIEND_TEST
}

TEST_F(SourceTest, testDataSourceGetGatheringModeFromString) {
    // create a DefaultSource instead of raw DataSource
    const DataSourcePtr source = createDefaultDataSourceWithSchemaForOneBuffer(this->schema,
                                                                               this->nodeEngine->getBufferManager(),
                                                                               this->nodeEngine->getQueryManager(),
                                                                               this->operatorId,
                                                                               this->numSourceLocalBuffersDefault,
                                                                               {});
    ASSERT_EQ(source->getGatheringModeFromString("frequency"), source->GatheringMode::FREQUENCY_MODE);
    ASSERT_EQ(source->getGatheringModeFromString("ingestionrate"), source->GatheringMode::INGESTION_RATE_MODE);
    EXPECT_ANY_THROW(source->getGatheringModeFromString("clearly_an_erroneous_string"));
}

TEST_F(SourceTest, testDataSourceRunningRoutineFrequency) {
    MockDataSource mDataSource(this->schema,
                               this->nodeEngine->getBufferManager(),
                               this->nodeEngine->getQueryManager(),
                               this->operatorId,
                               this->numSourceLocalBuffersDefault,
                               DataSource::GatheringMode::FREQUENCY_MODE,
                               {});
    ON_CALL(mDataSource, runningRoutineWithFrequency()).WillByDefault(Return());
    EXPECT_CALL(mDataSource, runningRoutineWithFrequency()).Times(Exactly(1));
    mDataSource.runningRoutine();
}

TEST_F(SourceTest, testDataSourceRunningRoutineIngestion) {
    MockDataSource mDataSource(this->schema,
                               this->nodeEngine->getBufferManager(),
                               this->nodeEngine->getQueryManager(),
                               this->operatorId,
                               this->numSourceLocalBuffersDefault,
                               DataSource::GatheringMode::INGESTION_RATE_MODE,
                               {});
    ON_CALL(mDataSource, runningRoutineWithIngestionRate()).WillByDefault(Return());
    EXPECT_CALL(mDataSource, runningRoutineWithIngestionRate()).Times(Exactly(1));
    mDataSource.runningRoutine();
}

TEST_F(SourceTest, testDataSourceFrequencyRoutineBufWithValue) {
    // create executable stage
    auto executableStage = std::make_shared<MockedExecutablePipeline>();
    // create sink
    auto sink = createCSVFileSink(this->schema, 0, this->nodeEngine, "source-test-freq-routine.csv", false);
    // get mocked pipeline to add to source
    auto pipeline = this->createExecutablePipeline(executableStage, sink);
    // mock query manager for passing addEndOfStream
    DataSourceProxyPtr mDataSource = createDataSourceProxy(this->schema,
                                                           this->nodeEngine->getBufferManager(),
                                                           this->nodeEngine->getQueryManager(),
                                                           this->operatorId,
                                                           this->numSourceLocalBuffersDefault,
                                                           DataSource::GatheringMode::FREQUENCY_MODE,
                                                           {pipeline});
    mDataSource->numBuffersToProcess = 1;
    mDataSource->running = true;
    mDataSource->wasGracefullyStopped = true;
    auto fakeBuf = mDataSource->getRecyclableBuffer();
    ON_CALL(*mDataSource, toString()).WillByDefault(Return("MOCKED SOURCE"));
    ON_CALL(*mDataSource, getType()).WillByDefault(Return(SourceType::CSV_SOURCE));
    ON_CALL(*mDataSource, receiveData()).WillByDefault(Return(fakeBuf));
    ON_CALL(*mDataSource, emitWork(_)).WillByDefault(Return());
    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(this->queryId,
                                                                         this->queryId,
                                                                         {mDataSource},
                                                                         {sink},
                                                                         {pipeline},
                                                                         this->nodeEngine->getQueryManager(),
                                                                         this->nodeEngine->getBufferManager());
    ASSERT_TRUE(this->nodeEngine->registerQueryInNodeEngine(executionPlan));
    ASSERT_TRUE(this->nodeEngine->startQuery(this->queryId));
    ASSERT_EQ(this->nodeEngine->getQueryStatus(this->queryId), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    EXPECT_CALL(*mDataSource, receiveData()).Times(Exactly(1));
    EXPECT_CALL(*mDataSource, emitWork(_)).Times(Exactly(1));
    mDataSource->runningRoutine();
    EXPECT_FALSE(mDataSource->running);
    EXPECT_TRUE(mDataSource->wasGracefullyStopped);
    EXPECT_TRUE(Mock::VerifyAndClearExpectations(mDataSource.get()));
}

TEST_F(SourceTest, testDataSourceIngestionRoutineBufWithValue) {
    // create executable stage
    auto executableStage = std::make_shared<MockedExecutablePipeline>();
    // create sink
    auto sink = createCSVFileSink(this->schema, 0, this->nodeEngine, "source-test-ingest-routine.csv", false);
    // get mocked pipeline to add to source
    auto pipeline = this->createExecutablePipeline(executableStage, sink);
    // mock query manager for passing addEndOfStream
    DataSourceProxyPtr mDataSource = createDataSourceProxy(this->schema,
                                                           this->nodeEngine->getBufferManager(),
                                                           this->nodeEngine->getQueryManager(),
                                                           this->operatorId,
                                                           this->numSourceLocalBuffersDefault,
                                                           DataSource::GatheringMode::INGESTION_RATE_MODE,
                                                           {pipeline});
    mDataSource->numBuffersToProcess = 1;
    mDataSource->running = true;
    mDataSource->wasGracefullyStopped = true;
    mDataSource->gatheringIngestionRate = 1;
    auto fakeBuf = mDataSource->getRecyclableBuffer();
    ON_CALL(*mDataSource, toString()).WillByDefault(Return("MOCKED SOURCE"));
    ON_CALL(*mDataSource, getType()).WillByDefault(Return(SourceType::LAMBDA_SOURCE));
    ON_CALL(*mDataSource, receiveData()).WillByDefault(Return(fakeBuf));
    ON_CALL(*mDataSource, emitWork(_)).WillByDefault(Return());
    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(this->queryId,
                                                                         this->queryId,
                                                                         {mDataSource},
                                                                         {sink},
                                                                         {pipeline},
                                                                         this->nodeEngine->getQueryManager(),
                                                                         this->nodeEngine->getBufferManager());
    ASSERT_TRUE(this->nodeEngine->registerQueryInNodeEngine(executionPlan));
    ASSERT_TRUE(this->nodeEngine->startQuery(this->queryId));
    ASSERT_EQ(this->nodeEngine->getQueryStatus(this->queryId), Runtime::Execution::ExecutableQueryPlanStatus::Running);
    EXPECT_CALL(*mDataSource, receiveData()).Times(Exactly(1));
    EXPECT_CALL(*mDataSource, emitWork(_)).Times(Exactly(1)).WillOnce(InvokeWithoutArgs([&]() {
        mDataSource->running = false;
        return;
    }));
    mDataSource->runningRoutine();
    EXPECT_FALSE(mDataSource->running);
    EXPECT_TRUE(mDataSource->wasGracefullyStopped);
    EXPECT_TRUE(Mock::VerifyAndClearExpectations(mDataSource.get()));
}

TEST_F(SourceTest, testDataSourceOpen) {
    DataSourceProxy mDataSource(this->schema,
                                this->nodeEngine->getBufferManager(),
                                this->nodeEngine->getQueryManager(),
                                this->operatorId,
                                this->numSourceLocalBuffersDefault,
                                DataSource::GatheringMode::INGESTION_RATE_MODE,
                                {});
    // EXPECT_ANY_THROW(mDataSource.bufferManager->getAvailableBuffers()); currently not possible w/ Error: success :)
    mDataSource.open();
    auto size = mDataSource.bufferManager->getAvailableBuffers();
    EXPECT_EQ(size, this->numSourceLocalBuffersDefault);
}

TEST_F(SourceTest, testBinarySourceGetType) {
    BinarySourceProxy bDataSource(this->schema,
                                  this->nodeEngine->getBufferManager(),
                                  this->nodeEngine->getQueryManager(),
                                  this->path_to_bin_file,
                                  this->operatorId,
                                  this->numSourceLocalBuffersDefault,
                                  {});
    ASSERT_EQ(bDataSource.getType(), NES::SourceType::BINARY_SOURCE);
}

TEST_F(SourceTest, testBinarySourceWrongPath) {
    BinarySourceProxy bDataSource(this->schema,
                                  this->nodeEngine->getBufferManager(),
                                  this->nodeEngine->getQueryManager(),
                                  this->wrong_filepath,
                                  this->operatorId,
                                  this->numSourceLocalBuffersDefault,
                                  {});
    ASSERT_FALSE(bDataSource.input.is_open());
}

TEST_F(SourceTest, testBinarySourceCorrectPath) {
    BinarySourceProxy bDataSource(this->schema,
                                  this->nodeEngine->getBufferManager(),
                                  this->nodeEngine->getQueryManager(),
                                  this->path_to_bin_file,
                                  this->operatorId,
                                  this->numSourceLocalBuffersDefault,
                                  {});
    ASSERT_TRUE(bDataSource.input.is_open());
}

TEST_F(SourceTest, testBinarySourceFillBuffer) {
    BinarySourceProxy bDataSource(this->schema,
                                  this->nodeEngine->getBufferManager(),
                                  this->nodeEngine->getQueryManager(),
                                  this->path_to_bin_file,
                                  this->operatorId,
                                  this->numSourceLocalBuffersDefault,
                                  {});
    uint64_t tuple_size = this->schema->getSchemaSizeInBytes();
    uint64_t buffer_size = this->nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;// increased by 1 every fillBuffer()
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);
    auto buf = this->GetEmptyBuffer();
    ASSERT_EQ(bDataSource.getNumberOfGeneratedTuples(), 0u);
    ASSERT_EQ(bDataSource.getNumberOfGeneratedBuffers(), 0u);
    bDataSource.fillBuffer(*buf);
    EXPECT_EQ(bDataSource.getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
    EXPECT_EQ(bDataSource.getNumberOfGeneratedBuffers(), numberOfBuffers);
    EXPECT_STREQ(buf->getBuffer<ysbRecord>()->ad_type, "banner78");
}

TEST_F(SourceTest, testBinarySourceFillBufferRandomTimes) {
    BinarySourceProxy bDataSource(this->schema,
                                  this->nodeEngine->getBufferManager(),
                                  this->nodeEngine->getQueryManager(),
                                  this->path_to_bin_file,
                                  this->operatorId,
                                  this->numSourceLocalBuffersDefault,
                                  {});
    uint64_t tuple_size = this->schema->getSchemaSizeInBytes();
    uint64_t buffer_size = this->nodeEngine->getBufferManager()->getBufferSize();
    uint64_t numberOfBuffers = 1;// increased by 1 every fillBuffer()
    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);
    auto buf = this->GetEmptyBuffer();
    auto iterations = rand() % 5;
    ASSERT_EQ(bDataSource.getNumberOfGeneratedTuples(), 0u);
    ASSERT_EQ(bDataSource.getNumberOfGeneratedBuffers(), 0u);
    for (int i = 0; i < iterations; ++i) {
        bDataSource.fillBuffer(*buf);
        EXPECT_EQ(bDataSource.getNumberOfGeneratedTuples(), (i + 1) * numberOfTuplesToProcess);
        EXPECT_EQ(bDataSource.getNumberOfGeneratedBuffers(), (i + 1) * numberOfBuffers);
    }
    EXPECT_EQ(bDataSource.getNumberOfGeneratedTuples(), iterations * numberOfTuplesToProcess);
    EXPECT_EQ(bDataSource.getNumberOfGeneratedBuffers(), iterations * numberOfBuffers);
}

TEST_F(SourceTest, testBinarySourceFillBufferContents) {
    BinarySourceProxy bDataSource(this->schema,
                                  this->nodeEngine->getBufferManager(),
                                  this->nodeEngine->getQueryManager(),
                                  this->path_to_bin_file,
                                  this->operatorId,
                                  this->numSourceLocalBuffersDefault,
                                  {});
    auto buf = this->GetEmptyBuffer();
    bDataSource.fillBuffer(*buf);
    auto content = buf->getBuffer<ysbRecord>();
    EXPECT_STREQ(content->ad_type, "banner78");
    EXPECT_TRUE((!strcmp(content->event_type, "view") || !strcmp(content->event_type, "click")
                 || !strcmp(content->event_type, "purchase")));
}

TEST_F(SourceTest, testCSVSourceGetType) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();

    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    ASSERT_EQ(csvDataSource.getType(), NES::SourceType::CSV_SOURCE);
}

TEST_F(SourceTest, testCSVSourceWrongFilePath) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->wrong_filepath);
    sourceConfig->setNumberOfBuffersToProduce(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    ASSERT_FALSE(csvDataSource.input.is_open());
}

TEST_F(SourceTest, testCSVSourceCorrectFilePath) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    ASSERT_TRUE(csvDataSource.input.is_open());
}

TEST_F(SourceTest, testCSVSourceFillBufferFileEnded) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    csvDataSource.fileEnded = true;
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    EXPECT_EQ(buf->getNumberOfTuples(), 0u);
}

TEST_F(SourceTest, testCSVSourceFillBufferOnce) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);
    sourceConfig->setSkipHeader(true);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    auto buf = this->GetEmptyBuffer();
    ASSERT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 0u);
    ASSERT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 0u);
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 1u);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 1u);
}

TEST_F(SourceTest, testCSVSourceFillBufferOnceColumnLayout) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);
    sourceConfig->setSkipHeader(true);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    auto buf = this->GetEmptyBuffer();
    ASSERT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 0u);
    ASSERT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 0u);
    std::shared_ptr<Runtime::MemoryLayouts::ColumnLayout> layoutPtr = Runtime::MemoryLayouts::ColumnLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 1u);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 1u);
}

TEST_F(SourceTest, testCSVSourceFillBufferContentsHeaderFailure) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file_head);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);

    // read actual header, get error from casting input to schema
    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    auto buf = this->GetEmptyBuffer();
    try {
        Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
        Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
        csvDataSource.fillBuffer(buffer);
    } catch (std::invalid_argument const& err) {// 1/2 throwables from stoull
        // TODO: is the "overwrite" of the message a good thing?
        // EXPECT_EQ(err.what(),std::string("Invalid argument"));
        EXPECT_EQ(err.what(), std::string("stoull"));// TODO(Dimitrios) this fails on mac as err.what() == "stoull: no conversion"
    } catch (std::out_of_range const& err) {         // 2/2 throwables from stoull
        EXPECT_EQ(err.what(), std::string("Out of range"));
    } catch (...) {
        FAIL() << "Uncaught exception in test for file with headers!" << std::endl;
    }
}

TEST_F(SourceTest, testCSVSourceFillBufferContentsHeaderFailureColumnLayout) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file_head);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);

    // read actual header, get error from casting input to schema
    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    auto buf = this->GetEmptyBuffer();
    try {
        Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
        Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
        csvDataSource.fillBuffer(buffer);
    } catch (std::invalid_argument const& err) {// 1/2 throwables from stoull
        // TODO: is the "overwrite" of the message a good thing?
        // EXPECT_EQ(err.what(),std::string("Invalid argument"));
        EXPECT_EQ(err.what(), std::string("stoull"));
    } catch (std::out_of_range const& err) {// 2/2 throwables from stoull
        EXPECT_EQ(err.what(), std::string("Out of range"));
    } catch (...) {
        FAIL() << "Uncaught exception in test for file with headers!" << std::endl;
    }
}

TEST_F(SourceTest, testCSVSourceFillBufferContentsSkipHeader) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file_head);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);
    sourceConfig->setSkipHeader(true);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    auto content = buf->getBuffer<ysbRecord>();
    EXPECT_STREQ(content->ad_type, "banner78");
    EXPECT_TRUE((!strcmp(content->event_type, "view") || !strcmp(content->event_type, "click")
                 || !strcmp(content->event_type, "purchase")));
}

TEST_F(SourceTest, testCSVSourceFillBufferContentsSkipHeaderColumnLayout) {
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file_head);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);
    sourceConfig->setSkipHeader(true);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    auto content = buf->getBuffer<ysbRecord>();
    EXPECT_STREQ(content->ad_type, "banner78");
    EXPECT_TRUE((!strcmp(content->event_type, "view") || !strcmp(content->event_type, "click")
                 || !strcmp(content->event_type, "purchase")));
}

TEST_F(SourceTest, testCSVSourceFillBufferFullFile) {
    // Full pass: 52 tuples in first buffer, 48 in second
    // expectedNumberOfBuffers in c-tor, no looping
    uint64_t expectedNumberOfTuples = 100;
    uint64_t expectedNumberOfBuffers = 2;
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(expectedNumberOfBuffers);// file is not going to loop
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);
    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    ASSERT_FALSE(csvDataSource.fileEnded);
    ASSERT_FALSE(csvDataSource.loopOnFile);
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    while (csvDataSource.getNumberOfGeneratedBuffers() < expectedNumberOfBuffers) {// relative to file size
        csvDataSource.fillBuffer(buffer);
        EXPECT_NE(buf->getNumberOfTuples(), 0u);
        EXPECT_TRUE(buf.has_value());
        for (uint64_t i = 0; i < buf->getNumberOfTuples(); i++) {
            auto tuple = buf->getBuffer<ysbRecord>();
            EXPECT_STREQ(tuple->ad_type, "banner78");
            EXPECT_TRUE((!strcmp(tuple->event_type, "view") || !strcmp(tuple->event_type, "click")
                         || !strcmp(tuple->event_type, "purchase")));
        }
    }
    EXPECT_TRUE(csvDataSource.fileEnded);
    EXPECT_FALSE(csvDataSource.loopOnFile);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), expectedNumberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceFillBufferFullFileColumnLayout) {
    // Full pass: 52 tuples in first buffer, 48 in second
    // expectedNumberOfBuffers in c-tor, no looping
    uint64_t expectedNumberOfTuples = 100;
    uint64_t expectedNumberOfBuffers = 2;
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(expectedNumberOfBuffers);// file is not going to loop
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);
    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    ASSERT_FALSE(csvDataSource.fileEnded);
    ASSERT_FALSE(csvDataSource.loopOnFile);
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    while (csvDataSource.getNumberOfGeneratedBuffers() < expectedNumberOfBuffers) {// relative to file size
        csvDataSource.fillBuffer(buffer);
        EXPECT_NE(buf->getNumberOfTuples(), 0u);
        EXPECT_TRUE(buf.has_value());
        for (uint64_t i = 0; i < buf->getNumberOfTuples(); i++) {
            auto tuple = buf->getBuffer<ysbRecord>();
            EXPECT_STREQ(tuple->ad_type, "banner78");
            EXPECT_TRUE((!strcmp(tuple->event_type, "view") || !strcmp(tuple->event_type, "click")
                         || !strcmp(tuple->event_type, "purchase")));
        }
    }
    EXPECT_TRUE(csvDataSource.fileEnded);
    EXPECT_FALSE(csvDataSource.loopOnFile);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), expectedNumberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceFillBufferFullFileOnLoop) {
    // Full pass: 52 tuples in a buffer, 2*52 = 104 in total
    // file is 52 + 48 but it loops, so 1st: 52, 2nd: also 52
    // expectedNumberOfBuffers set 0 in c-tor, looping
    uint64_t expectedNumberOfTuples = 104;
    uint64_t expectedNumberOfBuffers = 2;
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(this->path_to_file);
    sourceConfig->setNumberOfBuffersToProduce(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setSourceFrequency(this->frequency);

    CSVSourceProxy csvDataSource(this->schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});
    ASSERT_FALSE(csvDataSource.fileEnded);
    ASSERT_TRUE(csvDataSource.loopOnFile);
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    while (csvDataSource.getNumberOfGeneratedBuffers() < expectedNumberOfBuffers) {
        csvDataSource.fillBuffer(buffer);
    }
    EXPECT_FALSE(csvDataSource.fileEnded);
    EXPECT_TRUE(csvDataSource.loopOnFile);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), expectedNumberOfTuples);
    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), expectedNumberOfBuffers);
}

TEST_F(SourceTest, testCSVSourceIntTypes) {
    // use custom schema and file, read once
    SchemaPtr int_schema = Schema::create()
                               ->addField("uint64", UINT64)
                               ->addField("int64", INT64)
                               ->addField("uint32", UINT32)
                               ->addField("int32", INT32)
                               ->addField("uint16", UINT16)
                               ->addField("int16", INT16)
                               ->addField("uint8", UINT8)
                               ->addField("int8", INT8);

    std::string path_to_int_file = "../tests/test_data/every-int.csv";
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(path_to_int_file);
    sourceConfig->setNumberOfBuffersToProduce(1); // file not looping
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);
    CSVSourceProxy csvDataSource(int_schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});

    std::cout << int_schema->toString() << std::endl;
    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(int_schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    auto content = buf->getBuffer<everyIntTypeRecord>();
    // number is in the expected range
    EXPECT_LE(content->uint64_entry, std::numeric_limits<uint64_t>::max());
    EXPECT_GE(content->uint64_entry, std::numeric_limits<uint64_t>::min());
    // unsigned number is equal to max, no specific numbers in code
    EXPECT_EQ(content->uint64_entry, std::numeric_limits<uint64_t>::max());
    // number is in the expected range
    EXPECT_LE(content->int64_entry, std::numeric_limits<int64_t>::max());
    EXPECT_GE(content->int64_entry, std::numeric_limits<int64_t>::min());
    // checks for min, covers signed case
    EXPECT_EQ(content->int64_entry, std::numeric_limits<int64_t>::min());

    EXPECT_LE(content->uint32_entry, std::numeric_limits<uint32_t>::max());
    EXPECT_GE(content->uint32_entry, std::numeric_limits<uint32_t>::min());
    EXPECT_EQ(content->uint32_entry, std::numeric_limits<uint32_t>::max());

    EXPECT_LE(content->int32_entry, std::numeric_limits<int32_t>::max());
    EXPECT_GE(content->int32_entry, std::numeric_limits<int32_t>::min());
    EXPECT_EQ(content->int32_entry, std::numeric_limits<int32_t>::min());

    EXPECT_LE(content->uint16_entry, std::numeric_limits<uint16_t>::max());
    EXPECT_GE(content->uint16_entry, std::numeric_limits<uint16_t>::min());
    EXPECT_EQ(content->uint16_entry, std::numeric_limits<uint16_t>::max());

    EXPECT_LE(content->int16_entry, std::numeric_limits<int16_t>::max());
    EXPECT_GE(content->int16_entry, std::numeric_limits<int16_t>::min());
    EXPECT_EQ(content->int16_entry, std::numeric_limits<int16_t>::min());

    EXPECT_LE(content->uint8_entry, std::numeric_limits<uint8_t>::max());
    EXPECT_GE(content->uint8_entry, std::numeric_limits<uint8_t>::min());
    EXPECT_EQ(content->uint8_entry, std::numeric_limits<uint8_t>::max());

    EXPECT_LE(content->int8_entry, std::numeric_limits<int8_t>::max());
    EXPECT_GE(content->int8_entry, std::numeric_limits<int8_t>::min());
    EXPECT_EQ(content->int8_entry, std::numeric_limits<int8_t>::min());
}

TEST_F(SourceTest, testCSVSourceFloatTypes) {
    // use custom schema and file, read once
    SchemaPtr float_schema = Schema::create()->addField("float64", FLOAT64)->addField("float32", FLOAT32);
    std::string path_to_float_file = "../tests/test_data/every-float.csv";
    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();
    sourceConfig->setFilePath(path_to_float_file);
    sourceConfig->setNumberOfBuffersToProduce(1); // file is not going to loop
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);
    CSVSourceProxy csvDataSource(float_schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});

    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(float_schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    auto content = buf->getBuffer<everyFloatTypeRecord>();
    EXPECT_LE(content->float64_entry, std::numeric_limits<double>::max());
    EXPECT_GE(content->float64_entry, std::numeric_limits<double>::min());
    EXPECT_DOUBLE_EQ(content->float64_entry, std::numeric_limits<double>::max());

    EXPECT_LE(content->float32_entry, std::numeric_limits<float>::max());
    EXPECT_GE(content->float32_entry, std::numeric_limits<float>::min());
    EXPECT_FLOAT_EQ(content->float32_entry, std::numeric_limits<float>::max());
}

TEST_F(SourceTest, testCSVSourceBooleanTypes) {
    // use custom schema and file, read once
    SchemaPtr bool_schema = Schema::create()
                                ->addField("false", BOOLEAN)
                                ->addField("true", BOOLEAN)
                                ->addField("falsey", BOOLEAN)
                                ->addField("truthy", BOOLEAN);

    std::string path_to_bool_file = "../tests/test_data/every-boolean.csv";

    CSVSourceConfigPtr sourceConfig = CSVSourceConfig::create();

    sourceConfig->setFilePath(path_to_bool_file);
    sourceConfig->setNumberOfBuffersToProduce(1);// file is not going to loop
    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
    sourceConfig->setSourceFrequency(this->frequency);

    CSVSourceProxy csvDataSource(bool_schema,
                                 this->nodeEngine->getBufferManager(),
                                 this->nodeEngine->getQueryManager(),
                                 sourceConfig,
                                 this->operatorId,
                                 this->numSourceLocalBuffersDefault,
                                 {});

    auto buf = this->GetEmptyBuffer();
    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr = Runtime::MemoryLayouts::RowLayout::create(bool_schema, this->nodeEngine->getBufferManager()->getBufferSize());
    Runtime::MemoryLayouts::DynamicTupleBuffer buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layoutPtr, *buf);
    csvDataSource.fillBuffer(buffer);
    auto content = buf->getBuffer<everyBooleanTypeRecord>();
    EXPECT_FALSE(content->false_entry);
    EXPECT_TRUE(content->true_entry);
    EXPECT_FALSE(content->falsey_entry);
    EXPECT_TRUE(content->truthy_entry);
}

TEST_F(SourceTest, testGeneratorSourceGetType) {
    GeneratorSourceProxy genDataSource(this->schema,
                                       this->nodeEngine->getBufferManager(),
                                       this->nodeEngine->getQueryManager(),
                                       1,
                                       this->operatorId,
                                       this->numSourceLocalBuffersDefault,
                                       DataSource::GatheringMode::INGESTION_RATE_MODE,
                                       {});
    ASSERT_EQ(genDataSource.getType(), SourceType::TEST_SOURCE);
}

TEST_F(SourceTest, testDefaultSourceGetType) {
    DefaultSourceProxy defDataSource(this->schema,
                                     this->nodeEngine->getBufferManager(),
                                     this->nodeEngine->getQueryManager(),
                                     1,
                                     1000,
                                     this->operatorId,
                                     this->numSourceLocalBuffersDefault,
                                     {});
    ASSERT_EQ(defDataSource.getType(), SourceType::DEFAULT_SOURCE);
}

TEST_F(SourceTest, testDefaultSourceReceiveData) {
    // call receiveData, get the generated buffer
    // assert it has values and the tuples are as many as the predefined size
    DefaultSourceProxy defDataSource(this->schema,
                                     this->nodeEngine->getBufferManager(),
                                     this->nodeEngine->getQueryManager(),
                                     1,
                                     1000,
                                     this->operatorId,
                                     this->numSourceLocalBuffersDefault,
                                     {});
    // open starts the bufferManager, otherwise receiveData will fail
    defDataSource.open();
    auto buf = defDataSource.receiveData();
    EXPECT_TRUE(buf.has_value());
    EXPECT_EQ(buf->getNumberOfTuples(), 10u);
}

TEST_F(SourceTest, testLambdaSourceInitAndTypeFrequency) {
    uint64_t numBuffers = 2;
    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        uint64_t currentEventType = 0;
        auto* ysbRecords = buffer.getBuffer<IngestionRecord>();
        for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
            ysbRecords[i].userId = i;
            ysbRecords[i].pageId = 0;
            ysbRecords[i].adType = 0;
            ysbRecords[i].campaignId = rand() % 10000;
            ysbRecords[i].eventType = (currentEventType++) % 3;
            ysbRecords[i].currentMs = time(nullptr);
            ysbRecords[i].ip = 0x01020304;
            std::cout << "Write rec i=" << i << " content=" << ysbRecords[i].toString() << " size=" << sizeof(IngestionRecord)
                      << " addr=" << &ysbRecords[i] << std::endl;
        }
    };

    LambdaSourceProxy lambdaDataSource(lambdaSchema,
                                       this->nodeEngine->getBufferManager(),
                                       this->nodeEngine->getQueryManager(),
                                       numBuffers,
                                       0,
                                       func,
                                       this->operatorId,
                                       12,
                                       DataSource::GatheringMode::FREQUENCY_MODE,
                                       {});
    ASSERT_EQ(lambdaDataSource.getType(), SourceType::LAMBDA_SOURCE);
    ASSERT_EQ(lambdaDataSource.getGatheringIntervalCount(), 0u);
    ASSERT_EQ(lambdaDataSource.numberOfTuplesToProduce, 52u);
    lambdaDataSource.open();

    while (lambdaDataSource.getNumberOfGeneratedBuffers() < numBuffers) {
        auto resBuf = lambdaDataSource.receiveData();
        EXPECT_NE(resBuf, std::nullopt);
        EXPECT_TRUE(resBuf.has_value());
        EXPECT_EQ(resBuf->getNumberOfTuples(), lambdaDataSource.numberOfTuplesToProduce);
        auto ysbRecords = resBuf->getBuffer<IngestionRecord>();
        for (uint64_t i = 0; i < lambdaDataSource.numberOfTuplesToProduce; ++i) {
            EXPECT_TRUE(0 <= ysbRecords[i].campaignId && ysbRecords[i].campaignId < 10000);
            EXPECT_TRUE(0 <= ysbRecords[i].eventType && ysbRecords[i].eventType < 3);
        }
    }

    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedBuffers(), numBuffers);
    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedTuples(), numBuffers * lambdaDataSource.numberOfTuplesToProduce);
}

TEST_F(SourceTest, testLambdaSourceInitAndTypeIngestion) {
    uint64_t numBuffers = 2;
    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        uint64_t currentEventType = 0;
        auto* ysbRecords = buffer.getBuffer<IngestionRecord>();
        for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
            ysbRecords[i].userId = i;
            ysbRecords[i].pageId = 0;
            ysbRecords[i].adType = 0;
            ysbRecords[i].campaignId = rand() % 10000;
            ysbRecords[i].eventType = (currentEventType++) % 3;
            ysbRecords[i].currentMs = time(nullptr);
            ysbRecords[i].ip = 0x01020304;
            std::cout << "Write rec i=" << i << " content=" << ysbRecords[i].toString() << " size=" << sizeof(IngestionRecord)
                      << " addr=" << &ysbRecords[i] << std::endl;
        }
    };

    LambdaSourceProxy lambdaDataSource(lambdaSchema,
                                       this->nodeEngine->getBufferManager(),
                                       this->nodeEngine->getQueryManager(),
                                       numBuffers,
                                       1,
                                       func,
                                       this->operatorId,
                                       12,
                                       DataSource::GatheringMode::INGESTION_RATE_MODE,
                                       {});
    ASSERT_EQ(lambdaDataSource.getType(), SourceType::LAMBDA_SOURCE);
    ASSERT_EQ(lambdaDataSource.gatheringIngestionRate, 1u);
    lambdaDataSource.open();

    while (lambdaDataSource.getNumberOfGeneratedBuffers() < numBuffers) {
        auto resBuf = lambdaDataSource.receiveData();
        EXPECT_NE(resBuf, std::nullopt);
        EXPECT_TRUE(resBuf.has_value());
        EXPECT_EQ(resBuf->getNumberOfTuples(), lambdaDataSource.numberOfTuplesToProduce);
        auto ysbRecords = resBuf->getBuffer<IngestionRecord>();
        for (uint64_t i = 0; i < lambdaDataSource.numberOfTuplesToProduce; ++i) {
            EXPECT_TRUE(0 <= ysbRecords[i].campaignId && ysbRecords[i].campaignId < 10000);
            EXPECT_TRUE(0 <= ysbRecords[i].eventType && ysbRecords[i].eventType < 3);
        }
    }

    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedBuffers(), numBuffers);
    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedTuples(), numBuffers * lambdaDataSource.numberOfTuplesToProduce);
}

TEST_F(SourceTest, testIngestionRateFromQuery) {
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setRpcPort(4000);
    crdConf->setRestPort(8081);

    std::cout << "E2EBase: Start coordinator" << std::endl;
    auto crd = std::make_shared<NES::NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    std::cout << "E2EBase: Start worker 1" << std::endl;
    NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setBufferSizeInBytes(72);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    auto wrk1 = std::make_shared<NES::NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    NES_ASSERT(retStart1, "retStart1");

    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();

    auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };
        static int calls = 0;
        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = 1;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = 1;
            records[u].timestamp = calls;
            //            records[u].timestamp = time(0);
        }
        calls++;
    };

    wrk1->registerLogicalStream("input1", testSchemaFileName);

    NES::AbstractPhysicalStreamConfigPtr conf1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "test_stream1", "input1", std::move(func1), 6, 2, "ingestionrate");
    wrk1->registerPhysicalStream(conf1);

    std::string outputFilePath = "testIngestionRateFromQuery.out";
    remove(outputFilePath.c_str());
    string query =
        R"(Query::from("input1").sink(FileSinkDescriptor::create(")" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";

    NES::QueryServicePtr queryService = crd->getQueryService();
    auto queryCatalog = crd->getQueryCatalog();
    auto queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);

    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    auto start = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    string expectedContent = "input1$id:INTEGER,input1$value:INTEGER,input1$timestamp:INTEGER\n"
                             "1,1,0\n"
                             "1,1,0\n"
                             "1,1,0\n"
                             "1,1,1\n"
                             "1,1,1\n"
                             "1,1,1\n"
                             "1,1,2\n"
                             "1,1,2\n"
                             "1,1,2\n"
                             "1,1,3\n"
                             "1,1,3\n"
                             "1,1,3\n"
                             "1,1,4\n"
                             "1,1,4\n"
                             "1,1,4\n"
                             "1,1,5\n"
                             "1,1,5\n"
                             "1,1,5\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 60));
    auto stop = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_DEBUG("start=" << start << " stop=" << stop);
    EXPECT_TRUE(stop - start >= 2);

    NES_INFO("SourceTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::cout << "E2EBase: Stop worker 1" << std::endl;
    bool retStopWrk1 = wrk1->stop(true);
    NES_ASSERT(retStopWrk1, "retStopWrk1");

    std::cout << "E2EBase: Stop Coordinator" << std::endl;
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");
    std::cout << "E2EBase: Test finished" << std::endl;
}

TEST_F(SourceTest, testMonitoringSourceInitAndGetType) {
    // create metrics and plan for MonitoringSource
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    uint64_t numBuffers = 2;
    MonitoringSourceProxy monitoringDataSource(plan,
                                               MetricCatalog::NesMetrics(),
                                               this->nodeEngine->getBufferManager(),
                                               this->nodeEngine->getQueryManager(),
                                               numBuffers,
                                               1,
                                               this->operatorId,
                                               this->numSourceLocalBuffersDefault,
                                               {});
    ASSERT_EQ(monitoringDataSource.getType(), SourceType::MONITORING_SOURCE);
}

TEST_F(SourceTest, testMonitoringSourceReceiveDataOnce) {
    // create metrics and plan for MonitoringSource
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    uint64_t numBuffers = 2;
    MonitoringSourceProxy monitoringDataSource(plan,
                                               MetricCatalog::NesMetrics(),
                                               this->nodeEngine->getBufferManager(),
                                               this->nodeEngine->getQueryManager(),
                                               numBuffers,
                                               1,
                                               this->operatorId,
                                               this->numSourceLocalBuffersDefault,
                                               {});
    // open starts the bufferManager, otherwise receiveData will fail
    monitoringDataSource.open();
    auto buf = monitoringDataSource.receiveData();
    ASSERT_TRUE(buf.has_value());
    ASSERT_EQ(buf->getNumberOfTuples(), 1u);
    ASSERT_EQ(monitoringDataSource.getNumberOfGeneratedTuples(), 1u);
    ASSERT_EQ(monitoringDataSource.getNumberOfGeneratedBuffers(), 1u);
    GroupedMetricValues parsedValues = plan->fromBuffer(monitoringDataSource.getSchema(), buf.value());
    EXPECT_TRUE(parsedValues.cpuMetrics.value()->getTotal().user > 0);
    EXPECT_TRUE(parsedValues.memoryMetrics.value()->FREE_RAM > 0);
    EXPECT_TRUE(parsedValues.diskMetrics.value()->fBavail > 0);
}

TEST_F(SourceTest, testMonitoringSourceReceiveDataMultipleTimes) {
    // create metrics and plan for MonitoringSource
    auto metrics = std::vector<MetricValueType>({CpuMetric, DiskMetric, MemoryMetric, NetworkMetric});
    auto plan = MonitoringPlan::create(metrics);

    uint64_t numBuffers = 2;
    MonitoringSourceProxy monitoringDataSource(plan,
                                               MetricCatalog::NesMetrics(),
                                               this->nodeEngine->getBufferManager(),
                                               this->nodeEngine->getQueryManager(),
                                               numBuffers,
                                               1,
                                               this->operatorId,
                                               this->numSourceLocalBuffersDefault,
                                               {});
    // open starts the bufferManager, otherwise receiveData will fail
    monitoringDataSource.open();
    while (monitoringDataSource.getNumberOfGeneratedBuffers() < numBuffers) {
        auto optBuf = monitoringDataSource.receiveData();
        GroupedMetricValues parsedValues = plan->fromBuffer(monitoringDataSource.getSchema(), optBuf.value());
        EXPECT_TRUE(parsedValues.cpuMetrics.value()->getTotal().user > 0);
        EXPECT_TRUE(parsedValues.memoryMetrics.value()->FREE_RAM > 0);
        EXPECT_TRUE(parsedValues.diskMetrics.value()->fBavail > 0);
    }

    EXPECT_EQ(monitoringDataSource.getNumberOfGeneratedBuffers(), numBuffers);
    EXPECT_EQ(monitoringDataSource.getNumberOfGeneratedTuples(), 2UL);
}

TEST_F(SourceTest, testMemorySource) {
    //TODO: we should test the memory source
}

TEST_F(SourceTest, testTwoLambdaSources) {
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setRpcPort(4000);
    crdConf->setRestPort(8081);

    std::cout << "E2EBase: Start coordinator" << std::endl;
    auto crd = std::make_shared<NES::NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);

    std::cout << "E2EBase: Start worker 1" << std::endl;
    NES::WorkerConfigPtr wrkConf = NES::WorkerConfig::create();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    auto wrk1 = std::make_shared<NES::NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    NES_ASSERT(retStart1, "retStart1");

    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();

    auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        auto* records = buffer.getBuffer<Record>();
        auto ts = time(nullptr);
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = ts;
        }
    };

    auto func2 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        auto* records = buffer.getBuffer<Record>();
        auto ts = time(nullptr);
        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = ts;
        }
    };

    wrk1->registerLogicalStream("input1", testSchemaFileName);
    wrk1->registerLogicalStream("input2", testSchemaFileName);

    NES::AbstractPhysicalStreamConfigPtr conf1 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "test_stream1", "input1", std::move(func1), 3, 0, "frequency");
    wrk1->registerPhysicalStream(conf1);

    NES::AbstractPhysicalStreamConfigPtr conf2 =
        NES::LambdaSourceStreamConfig::create("LambdaSource", "test_stream2", "input2", std::move(func2), 3, 0, "frequency");
    wrk1->registerPhysicalStream(conf2);

    string query =
        R"(Query::from("input1").joinWith(Query::from("input2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))).sink(NullOutputSinkDescriptor::create());)";

    NES::QueryServicePtr queryService = crd->getQueryService();
    auto queryCatalog = crd->getQueryCatalog();
    auto queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    sleep(2);
    std::cout << "E2EBase: Remove query" << std::endl;
    NES_ASSERT(queryService->validateAndQueueStopRequest(queryId), "no vaild stop quest");
    std::cout << "E2EBase: wait for stop" << std::endl;
    bool ret = NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog);
    if (!ret) {
        NES_ERROR("query was not stopped within 30 sec");
    }

    std::cout << "E2EBase: Stop worker 1" << std::endl;
    bool retStopWrk1 = wrk1->stop(true);
    NES_ASSERT(retStopWrk1, "retStopWrk1");

    std::cout << "E2EBase: Stop Coordinator" << std::endl;
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");
    std::cout << "E2EBase: Test finished" << std::endl;
}

TEST_F(SourceTest, testTwoLambdaSourcesMultiThread) {
    NES::CoordinatorConfigPtr crdConf = NES::CoordinatorConfig::create();
    crdConf->setRpcPort(4000);
    crdConf->setRestPort(8081);
    crdConf->setNumWorkerThreads(8);
    crdConf->setNumberOfBuffersInGlobalBufferManager(3000);
    crdConf->setNumberOfBuffersInSourceLocalBufferPool(124);
    crdConf->setNumberOfBuffersPerWorker(124);
    crdConf->setBufferSizeInBytes(524288);

    std::cout << "E2EBase: Start coordinator" << std::endl;

    auto crd = std::make_shared<NES::NesCoordinator>(crdConf);
    crd->startCoordinator(/**blocking**/ false);

    std::string input =
        R"(Schema::create()->addField(createField("id", UINT64))->addField(createField("value", UINT64))->addField(createField("timestamp", UINT64));)";
    std::string testSchemaFileName = "input.hpp";
    std::ofstream out(testSchemaFileName);
    out << input;
    out.close();
    crd->getNesWorker()->registerLogicalStream("input", testSchemaFileName);

    for (int64_t i = 0; i < 4; i++) {
        auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
            struct Record {
                uint64_t id;
                uint64_t value;
                uint64_t timestamp;
            };

            auto* records = buffer.getBuffer<Record>();
            auto ts = time(nullptr);
            for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
                records[u].id = u;
                //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
                records[u].value = u % 10;
                records[u].timestamp = ts;
            }
        };

        NES::AbstractPhysicalStreamConfigPtr conf1 = NES::LambdaSourceStreamConfig::create("LambdaSource",
                                                                                           "test_stream" + std::to_string(i),
                                                                                           "input",
                                                                                           std::move(func1),
                                                                                           3000000,
                                                                                           0,
                                                                                           "frequency");
        crd->getNesWorker()->registerPhysicalStream(conf1);
    }

    string query = R"(Query::from("input").filter(Attribute("value") > 5).sink(NullOutputSinkDescriptor::create());)";

    NES::QueryServicePtr queryService = crd->getQueryService();
    auto queryCatalog = crd->getQueryCatalog();
    auto queryId = queryService->validateAndQueueAddRequest(query, "BottomUp", FaultToleranceType::NONE, LineageType::IN_MEMORY);
    NES_ASSERT(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog), "failed start wait");

    sleep(2);
    std::cout << "E2EBase: Remove query" << std::endl;
    NES_ASSERT(queryService->validateAndQueueStopRequest(queryId), "no valid stop quest");
    std::cout << "E2EBase: wait for stop" << std::endl;
    bool ret = NES::TestUtils::checkStoppedOrTimeout(queryId, queryCatalog);
    if (!ret) {
        NES_ERROR("query was not stopped within 30 sec");
    }

    std::cout << "E2EBase: Stop Coordinator" << std::endl;
    bool retStopCord = crd->stopCoordinator(true);
    NES_ASSERT(retStopCord, "retStopCord");
    std::cout << "E2EBase: Test finished" << std::endl;
}

}// namespace NES