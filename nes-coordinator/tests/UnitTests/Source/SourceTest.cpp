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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Monitoring/MetricCollectors/CpuCollector.hpp>
#include <Monitoring/MetricCollectors/DiskCollector.hpp>
#include <Monitoring/MonitoringPlan.hpp>
#include <Monitoring/ResourcesReader/SystemResourcesReaderFactory.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/ExecutableQueryPlan.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/FixedSizeBufferPool.hpp>
#include <Runtime/MemoryLayout/ColumnLayout.hpp>
#include <Runtime/MemoryLayout/MemoryLayout.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryManager.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/BinarySource.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/DefaultSource.hpp>
#include <Sources/LambdaSource.hpp>
#include <Sources/MonitoringSource.hpp>
#include <Sources/SourceCreator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/MetricValidator.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtils.hpp>
#include <cstring>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <limits>
#include <string>

namespace NES {

//struct __attribute__((packed)) ysbRecord {
//    char user_id[16]{};
//    char page_id[16]{};
//    char campaign_id[16]{};
//    char ad_type[9]{};
//    char event_type[9]{};
//    int64_t current_ms;
//    uint32_t ip;
//
//    ysbRecord() {
//        event_type[0] = '-';// invalid record
//        current_ms = 0;
//        ip = 0;
//    }
//
//    ysbRecord(const ysbRecord& rhs) {
//        memcpy(&user_id, &rhs.user_id, 16);
//        memcpy(&page_id, &rhs.page_id, 16);
//        memcpy(&campaign_id, &rhs.campaign_id, 16);
//        memcpy(&ad_type, &rhs.ad_type, 9);
//        memcpy(&event_type, &rhs.event_type, 9);
//        current_ms = rhs.current_ms;
//        ip = rhs.ip;
//    }
//};
//// size 78 bytes
//
//struct __attribute__((packed)) everyIntTypeRecord {
//    uint64_t uint64_entry;
//    int64_t int64_entry;
//    uint32_t uint32_entry;
//    int32_t int32_entry;
//    uint16_t uint16_entry;
//    int16_t int16_entry;
//    uint8_t uint8_entry;
//    int8_t int8_entry;
//};
//
//struct __attribute__((packed)) everyFloatTypeRecord {
//    double float64_entry;
//    float float32_entry;
//};
//
//struct __attribute__((packed)) everyBooleanTypeRecord {
//    bool false_entry;
//    bool true_entry;
//    bool falsey_entry;
//    bool truthy_entry;
//};
//
//struct __attribute__((packed)) IngestionRecord {
//    IngestionRecord(uint64_t userId,
//                    uint64_t pageId,
//                    uint64_t campaignId,
//                    uint64_t adType,
//                    uint64_t eventType,
//                    uint64_t currentMs,
//                    uint64_t ip)
//        : userId(userId), pageId(pageId), campaignId(campaignId), adType(adType), eventType(eventType), currentMs(currentMs),
//          ip(ip) {}
//
//    uint64_t userId;
//    uint64_t pageId;
//    uint64_t campaignId;
//    uint64_t adType;
//    uint64_t eventType;
//    uint64_t currentMs;
//    uint64_t ip;
//
//    // placeholder to reach 78 bytes
//    uint64_t dummy1{0};
//    uint64_t dummy2{0};
//    uint32_t dummy3{0};
//    uint16_t dummy4{0};
//
//    IngestionRecord(const IngestionRecord& rhs) {
//        userId = rhs.userId;
//        pageId = rhs.pageId;
//        campaignId = rhs.campaignId;
//        adType = rhs.adType;
//        eventType = rhs.eventType;
//        currentMs = rhs.currentMs;
//        ip = rhs.ip;
//    }
//
//    [[nodiscard]] std::string toString() const {
//        return "Record(userId=" + std::to_string(userId) + ", pageId=" + std::to_string(pageId) + ", campaignId="
//            + std::to_string(campaignId) + ", adType=" + std::to_string(adType) + ", eventType=" + std::to_string(eventType)
//            + ", currentMs=" + std::to_string(currentMs) + ", ip=" + std::to_string(ip);
//    }
//};
//
//struct __attribute__((packed)) decimalsRecord {
//    float positive_with_decimal;
//    float negative_with_decimal;
//    float longer_precision_decimal;
//};
//
//using ::testing::_;
//using ::testing::AnyNumber;
//using ::testing::Exactly;
//using ::testing::InvokeWithoutArgs;
//using ::testing::Mock;
//using ::testing::Return;
//
//// testing w/ original running routine
//class MockDataSource : public DataSource {
//  public:
//    MockDataSource(const SchemaPtr& schema,
//                   Runtime::BufferManagerPtr bufferManager,
//                   Runtime::QueryManagerPtr queryManager,
//                   OperatorId operatorId,
//                   size_t numSourceLocalBuffers,
//                   GatheringMode gatheringMode,
//                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
//        : DataSource(schema,
//                     bufferManager,
//                     queryManager,
//                     operatorId,
//                     INITIAL_ORIGIN_ID,
//                     INVALID_STATISTIC_ID,
//                     numSourceLocalBuffers,
//                     gatheringMode,
//                     "defaultPhysicalStreamName",
//                     false,
//                     executableSuccessors){
//            // nop
//        };
//
//    static auto create(const SchemaPtr& schema,
//                       Runtime::BufferManagerPtr bufferManager,
//                       Runtime::QueryManagerPtr queryManager,
//                       OperatorId operatorId,
//                       size_t numSourceLocalBuffers,
//                       GatheringMode gatheringMode,
//                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors) {
//        return std::make_shared<MockDataSource>(schema,
//                                                bufferManager,
//                                                queryManager,
//                                                operatorId,
//                                                numSourceLocalBuffers,
//                                                gatheringMode,
//                                                executableSuccessors);
//    }
//
//    MOCK_METHOD(void, runningRoutineWithGatheringInterval, ());
//    MOCK_METHOD(void, runningRoutineWithIngestionRate, ());
//    MOCK_METHOD(void, runningRoutineAdaptiveGatheringInterval, ());
//    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
//    MOCK_METHOD(std::string, toString, (), (const));
//    MOCK_METHOD(SourceType, getType, (), (const));
//};
//
//// testing w/ mocked running routine (no need for actual routines)
//class MockDataSourceWithRunningRoutine : public DataSource {
//  public:
//    MockDataSourceWithRunningRoutine(const SchemaPtr& schema,
//                                     Runtime::BufferManagerPtr bufferManager,
//                                     Runtime::QueryManagerPtr queryManager,
//                                     OperatorId operatorId,
//                                     size_t numSourceLocalBuffers,
//                                     GatheringMode gatheringMode,
//                                     std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
//        : DataSource(schema,
//                     bufferManager,
//                     queryManager,
//                     operatorId,
//                     INVALID_ORIGIN_ID,
//                     INVALID_STATISTIC_ID,
//                     numSourceLocalBuffers,
//                     gatheringMode,
//                     "defaultPhysicalStreamName",
//                     false,
//                     executableSuccessors) {
//        ON_CALL(*this, runningRoutine()).WillByDefault(InvokeWithoutArgs([&]() {
//            completedPromise.set_value(true);
//            return;
//        }));
//    };
//    MOCK_METHOD(void, runningRoutine, ());
//    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
//    MOCK_METHOD(std::string, toString, (), (const));
//    MOCK_METHOD(SourceType, getType, (), (const));
//
//  private:
//    FRIEND_TEST(SourceTest, testDataSourceHardStopSideEffect);
//    FRIEND_TEST(SourceTest, testDataSourceGracefulStopSideEffect);
//};
//
//// proxy friendly to test class, exposing protected members
//class DataSourceProxy : public DataSource, public Runtime::BufferRecycler {
//  public:
//    DataSourceProxy(const SchemaPtr& schema,
//                    Runtime::BufferManagerPtr bufferManager,
//                    Runtime::QueryManagerPtr queryManager,
//                    OperatorId operatorId,
//                    size_t numSourceLocalBuffers,
//                    GatheringMode gatheringMode,
//                    std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors)
//        : DataSource(schema,
//                     bufferManager,
//                     queryManager,
//                     operatorId,
//                     INVALID_ORIGIN_ID,
//                     INVALID_STATISTIC_ID,
//                     numSourceLocalBuffers,
//                     gatheringMode,
//                     "defaultPhysicalStreamName",
//                     false,
//                     executableSuccessors){};
//
//    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
//    MOCK_METHOD(std::string, toString, (), (const));
//    MOCK_METHOD(SourceType, getType, (), (const));
//    MOCK_METHOD(void, emitWork, (Runtime::TupleBuffer & buffer, bool addBufferMetaData));
//    MOCK_METHOD(void, recycleUnpooledBuffer, (Runtime::detail::MemorySegment * buffer));
//
//    Runtime::TupleBuffer getRecyclableBuffer() {
//        auto* p = new uint8_t[5];
////        auto fakeBuffer = Runtime::TupleBuffer::wrapMemory(p, sizeof(uint8_t) * 5, this);
////        return fakeBuffer;
//    }
//
//    void recyclePooledBuffer(Runtime::detail::MemorySegment* buffer) { delete buffer->getPointer(); }
//    virtual ~DataSourceProxy() = default;
//
//  private:
//    FRIEND_TEST(SourceTest, testDataSourceGatheringIntervalRoutineBufWithValue);
//    FRIEND_TEST(SourceTest, testDataSourceIngestionRoutineBufWithValue);
//    FRIEND_TEST(SourceTest, testDataSourceIngestionRoutineBufWithValueWithTooSmallIngestionRate);
//    FRIEND_TEST(SourceTest, testDataSourceKFRoutineBufWithValue);
//    FRIEND_TEST(SourceTest, testDataSourceKFRoutineBufWithValueZeroIntervalUpdate);
//    FRIEND_TEST(SourceTest, testDataSourceKFRoutineBufWithValueIntervalUpdateNonZeroInitialInterval);
//    FRIEND_TEST(SourceTest, testDataSourceOpen);
//};
//using DataSourceProxyPtr = std::shared_ptr<DataSourceProxy>;
//
//class BinarySourceProxy : public BinarySource {
//  public:
//    BinarySourceProxy(const SchemaPtr& schema,
//                      Runtime::BufferManagerPtr bufferManager,
//                      Runtime::QueryManagerPtr queryManager,
//                      const std::string& file_path,
//                      OperatorId operatorId,
//                      size_t numSourceLocalBuffers,
//                      std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
//        : BinarySource(schema,
//                       bufferManager,
//                       queryManager,
//                       file_path,
//                       operatorId,
//                       INVALID_ORIGIN_ID,
//                       INVALID_STATISTIC_ID,
//                       numSourceLocalBuffers,
//                       GatheringMode::INTERVAL_MODE,
//                       "defaultPhysicalStreamName",
//                       successors){};
//
//  private:
//    FRIEND_TEST(SourceTest, testBinarySourceGetType);
//    FRIEND_TEST(SourceTest, testBinarySourceWrongPath);
//    FRIEND_TEST(SourceTest, testBinarySourceCorrectPath);
//    FRIEND_TEST(SourceTest, testBinarySourceFillBuffer);
//    FRIEND_TEST(SourceTest, testBinarySourceFillBufferRandomTimes);
//    FRIEND_TEST(SourceTest, testBinarySourceFillBufferContents);
//};
//
//class CSVSourceProxy : public CSVSource {
//  public:
//    CSVSourceProxy(SchemaPtr schema,
//                   Runtime::BufferManagerPtr bufferManager,
//                   Runtime::QueryManagerPtr queryManager,
//                   CSVSourceTypePtr sourceConfig,
//                   OperatorId operatorId,
//                   size_t numSourceLocalBuffers,
//                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
//        : CSVSource(schema,
//                    bufferManager,
//                    queryManager,
//                    sourceConfig,
//                    operatorId,
//                    INVALID_ORIGIN_ID,
//                    INVALID_STATISTIC_ID,
//                    numSourceLocalBuffers,
//                    GatheringMode::INTERVAL_MODE,
//                    "defaultPhysicalStreamName",
//                    successors){};
//
//  private:
//    FRIEND_TEST(SourceTest, testCSVSourceGetType);
//    FRIEND_TEST(SourceTest, testCSVSourceWrongFilePath);
//    FRIEND_TEST(SourceTest, testCSVSourceCorrectFilePath);
//    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFileEnded);
//    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFullFile);
//    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFullFileColumnLayout);
//    FRIEND_TEST(SourceTest, testCSVSourceFillBufferFullFileOnLoop);
//};
//
//class TCPSourceProxy : public TCPSource {
//  public:
//    TCPSourceProxy(SchemaPtr schema,
//                   Runtime::BufferManagerPtr bufferManager,
//                   Runtime::QueryManagerPtr queryManager,
//                   TCPSourceTypePtr sourceConfig,
//                   OperatorId operatorId,
//                   size_t numSourceLocalBuffers,
//                   std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
//        : TCPSource(schema,
//                    bufferManager,
//                    queryManager,
//                    sourceConfig,
//                    operatorId,
//                    INVALID_ORIGIN_ID,
//                    INVALID_STATISTIC_ID,
//                    numSourceLocalBuffers,
//                    GatheringMode::INTERVAL_MODE,
//                    "defaultPhysicalStreamName",
//                    successors){};
//
//  private:
//    FRIEND_TEST(SourceTest, TCPSourceInit);
//    FRIEND_TEST(SourceTest, TCPSourceReadCSVData);
//    FRIEND_TEST(SourceTest, TCPSourceReadJSONData);
//};
//
//class GeneratorSourceProxy : public GeneratorSource {
//  public:
//    GeneratorSourceProxy(SchemaPtr schema,
//                         Runtime::BufferManagerPtr bufferManager,
//                         Runtime::QueryManagerPtr queryManager,
//                         uint64_t numbersOfBufferToProduce,
//                         OperatorId operatorId,
//                         size_t numSourceLocalBuffers,
//                         GatheringMode gatheringMode,
//                         std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
//        : GeneratorSource(schema,
//                          bufferManager,
//                          queryManager,
//                          numbersOfBufferToProduce,
//                          operatorId,
//                          INVALID_ORIGIN_ID,
//                          INVALID_STATISTIC_ID,
//                          numSourceLocalBuffers,
//                          gatheringMode,
//                          successors,
//                          "defaultPhysicalStreamName"){};
//    MOCK_METHOD(std::optional<Runtime::TupleBuffer>, receiveData, ());
//};
//
//class DefaultSourceProxy : public DefaultSource {
//  public:
//    DefaultSourceProxy(SchemaPtr schema,
//                       Runtime::BufferManagerPtr bufferManager,
//                       Runtime::QueryManagerPtr queryManager,
//                       const uint64_t numbersOfBufferToProduce,
//                       uint64_t gatheringInterval,
//                       OperatorId operatorId,
//                       size_t numSourceLocalBuffers,
//                       std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
//        : DefaultSource(schema,
//                        bufferManager,
//                        queryManager,
//                        numbersOfBufferToProduce,
//                        gatheringInterval,
//                        operatorId,
//                        INVALID_ORIGIN_ID,
//                        INVALID_STATISTIC_ID,
//                        numSourceLocalBuffers,
//                        successors,
//                        "defaultPhysicalStreamName"){};
//};
//
//class LambdaSourceProxy : public LambdaSource {
//  public:
//    LambdaSourceProxy(
//        SchemaPtr schema,
//        Runtime::BufferManagerPtr bufferManager,
//        Runtime::QueryManagerPtr queryManager,
//        uint64_t numbersOfBufferToProduce,
//        uint64_t gatheringValue,
//        std::function<void(NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce)>&& generationFunction,
//        OperatorId operatorId,
//        size_t numSourceLocalBuffers,
//        GatheringMode gatheringMode,
//        std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors)
//        : LambdaSource(schema,
//                       bufferManager,
//                       queryManager,
//                       numbersOfBufferToProduce,
//                       gatheringValue,
//                       std::move(generationFunction),
//                       operatorId,
//                       INVALID_ORIGIN_ID,
//                       INVALID_STATISTIC_ID,
//                       numSourceLocalBuffers,
//                       gatheringMode,
//                       0,
//                       0,
//                       "defaultPhysicalStreamName",
//                       successors){};
//
//  private:
//    FRIEND_TEST(SourceTest, testLambdaSourceInitAndTypeInterval);
//    FRIEND_TEST(SourceTest, testLambdaSourceInitAndTypeIngestion);
//};
//
//class MonitoringSourceProxy : public MonitoringSource {
//  public:
//    MonitoringSourceProxy(const Monitoring::MetricCollectorPtr& metricCollector,
//                          std::chrono::milliseconds waitTime,
//                          Runtime::BufferManagerPtr bufferManager,
//                          Runtime::QueryManagerPtr queryManager,
//                          OperatorId operatorId,
//                          size_t numSourceLocalBuffers,
//                          std::vector<Runtime::Execution::SuccessorExecutablePipeline> successors = {})
//        : MonitoringSource(metricCollector,
//                           waitTime,
//                           bufferManager,
//                           queryManager,
//                           operatorId,
//                           INVALID_ORIGIN_ID,
//                           INVALID_STATISTIC_ID,
//                           numSourceLocalBuffers,
//                           "defaultPhysicalStreamName",
//                           successors){};
//};
//
//class MockedPipelineExecutionContext : public Runtime::Execution::PipelineExecutionContext {
//  public:
//    MockedPipelineExecutionContext(Runtime::QueryManagerPtr queryManager, DataSinkPtr sink)
//        : PipelineExecutionContext(
//            INVALID_PIPELINE_ID,             // mock pipeline id
//            INVALID_DECOMPOSED_QUERY_PLAN_ID,// mock query id
//            queryManager->getBufferManager(),
//            queryManager->getNumberOfWorkerThreads(),
//            [sink](Runtime::TupleBuffer& buffer, Runtime::WorkerContextRef worker) {
//                sink->writeData(buffer, worker);
//            },
//            [sink](Runtime::TupleBuffer&) {
//            },
//            std::vector<Runtime::Execution::OperatorHandlerPtr>()){};
//};
//
//class MockedExecutablePipeline : public Runtime::Execution::ExecutablePipelineStage {
//  public:
//    std::atomic<uint64_t> count = 0;
//    std::promise<bool> completedPromise;
//
//    ExecutionResult execute(Runtime::TupleBuffer& inputTupleBuffer,
//                            Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
//                            Runtime::WorkerContext& wctx) override {
//        count += inputTupleBuffer.getNumberOfTuples();
//
//        Runtime::TupleBuffer outputBuffer = wctx.allocateTupleBuffer();
//        auto arr = outputBuffer.getBuffer<uint32_t>();
//        arr[0] = static_cast<uint32_t>(count.load());
//        outputBuffer.setNumberOfTuples(count);
//        pipelineExecutionContext.emitBuffer(outputBuffer, wctx);
//        completedPromise.set_value(true);
//        return ExecutionResult::Ok;
//    }
//};
//
//class SourceTest : public Testing::BaseIntegrationTest {
//  public:
//    void SetUp() override {
//        Testing::BaseIntegrationTest::SetUp();
//        auto sourceConf = CSVSourceType::create("x", "x1");
//        auto workerConfigurations = WorkerConfiguration::create();
//        workerConfigurations->physicalSourceTypes.add(sourceConf);
//        this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
//                               .setQueryStatusListener(std::make_shared<DummyQueryListener>())
//                               .build();
//        this->path_to_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "ysb-tuples-100-campaign-100.csv";
//        this->path_to_file_head = std::filesystem::path(TEST_DATA_DIRECTORY) / "ysb-tuples-100-campaign-100-head.csv";
//        this->path_to_bin_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "ysb-tuples-100-campaign-100.bin";
//        this->path_to_decimals_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "decimals.csv";
//        this->schema = Schema::create()
//                           ->addField("user_id", DataTypeFactory::createFixedChar(16))
//                           ->addField("page_id", DataTypeFactory::createFixedChar(16))
//                           ->addField("campaign_id", DataTypeFactory::createFixedChar(16))
//                           ->addField("ad_type", DataTypeFactory::createFixedChar(9))
//                           ->addField("event_type", DataTypeFactory::createFixedChar(9))
//                           ->addField("current_ms", BasicType::UINT64)
//                           ->addField("ip", BasicType::INT32);
//        this->lambdaSchema = Schema::create()
//                                 ->addField("user_id", BasicType::UINT64)
//                                 ->addField("page_id", BasicType::UINT64)
//                                 ->addField("campaign_id", BasicType::UINT64)
//                                 ->addField("ad_type", BasicType::UINT64)
//                                 ->addField("event_type", BasicType::UINT64)
//                                 ->addField("current_ms", BasicType::UINT64)
//                                 ->addField("ip", BasicType::UINT64)
//                                 ->addField("d1", BasicType::UINT64)
//                                 ->addField("d2", BasicType::UINT64)
//                                 ->addField("d3", BasicType::UINT32)
//                                 ->addField("d4", BasicType::UINT16);
//        this->decimalsSchema = Schema::create()
//                                   ->addField("positive_with_decimal", BasicType::FLOAT32)
//                                   ->addField("negative_with_decimal", BasicType::FLOAT32)
//                                   ->addField("longer_precision_decimal", BasicType::FLOAT32);
//        this->tuple_size = this->schema->getSchemaSizeInBytes();
//        this->buffer_size = this->nodeEngine->getBufferManager()->getBufferSize();
//        this->numberOfBuffers = 1;
//        this->numberOfTuplesToProcess = this->numberOfBuffers * (this->buffer_size / this->tuple_size);
//        this->operatorId = 1;
//        this->originId = 1;
//        this->numSourceLocalBuffersDefault = 12;
//        this->gatheringInterval = 1000;
//        this->wrong_filepath = "this_doesnt_exist";
//        this->queryId = 1;
//        this->bufferAreaSize = this->numberOfBuffers * this->buffer_size;
//        void* tmp = nullptr;
//        ASSERT_TRUE(posix_memalign(&tmp, 64, bufferAreaSize) == 0);
//        this->singleMemoryArea = static_cast<uint8_t*>(tmp);
//        this->sourceAffinity = std::numeric_limits<uint64_t>::max();
//    }
//
//    static void TearDownTestCase() { NES_INFO("Tear down SourceTest test class."); }
//
//    static void SetUpTestCase() {
//        NES::Logger::setupLogging("SourceTest.log", NES::LogLevel::LOG_TRACE);
//        NES_INFO("Setup SourceTest test class.");
//    }
//
//    void TearDown() override {
//        free(singleMemoryArea);
//        ASSERT_TRUE(nodeEngine->stop());
//        Testing::BaseIntegrationTest::TearDown();
//    }
//
//    std::optional<Runtime::TupleBuffer> GetEmptyBuffer() { return this->nodeEngine->getBufferManager()->getBufferBlocking(); }
//
//    DataSourceProxyPtr createDataSourceProxy(const SchemaPtr& schema,
//                                             Runtime::BufferManagerPtr bufferManager,
//                                             Runtime::QueryManagerPtr queryManager,
//                                             OperatorId operatorId,
//                                             size_t numSourceLocalBuffers,
//                                             GatheringMode gatheringMode,
//                                             std::vector<Runtime::Execution::SuccessorExecutablePipeline> executableSuccessors) {
//        return std::make_shared<DataSourceProxy>(schema,
//                                                 bufferManager,
//                                                 queryManager,
//                                                 operatorId,
//                                                 numSourceLocalBuffers,
//                                                 gatheringMode,
//                                                 executableSuccessors);
//    }
//
//    std::shared_ptr<Runtime::Execution::ExecutablePipeline>
//    createExecutablePipeline(std::shared_ptr<MockedExecutablePipeline> executableStage, std::shared_ptr<SinkMedium> sink) {
//        auto context = std::make_shared<MockedPipelineExecutionContext>(this->nodeEngine->getQueryManager(), sink);
//        return Runtime::Execution::ExecutablePipeline::create(INVALID_PIPELINE_ID,
//                                                              SharedQueryId(queryId),
//                                                              DecomposedQueryId(queryId),
//                                                              DecomposedQueryPlanVersion(0),
//                                                              this->nodeEngine->getQueryManager(),
//                                                              context,
//                                                              executableStage,
//                                                              1,
//                                                              {sink});
//    }
//
//    Runtime::NodeEnginePtr nodeEngine{nullptr};
//    std::string path_to_file, path_to_bin_file, wrong_filepath, path_to_file_head, path_to_decimals_file;
//    SchemaPtr schema, lambdaSchema, decimalsSchema;
//    uint8_t* singleMemoryArea;
//    uint64_t tuple_size, buffer_size, numberOfBuffers, numberOfTuplesToProcess, operatorId, originId, statisticId,
//        numSourceLocalBuffersDefault, gatheringInterval, queryId, sourceAffinity;
//    size_t bufferAreaSize;
//    std::shared_ptr<uint8_t> singleBufferMemoryArea;
//};

//TEST_F(SourceTest, testDataSourceGetOperatorId) {
//    const DataSourcePtr source =
//        createDefaultDataSourceWithSchemaForOneBuffer(this->schema,
//                                                      this->nodeEngine->getBufferManager(),
//                                                      this->nodeEngine->getQueryManager(),
//                                                      OperatorId(operatorId),
//                                                      OriginId(originId),
//                                                      this->statisticId,
//                                                      this->numSourceLocalBuffersDefault,
//                                                      "defaultPhysicalSourceName",
//                                                      {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                        1,
//                                                                                        SharedQueryId(1),
//                                                                                        DecomposedQueryId(1),
//                                                                                        DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(source->getOperatorId(), OperatorId(operatorId));
//}
//
//TEST_F(SourceTest, DISABLED_testDataSourceEmptySuccessors) {
//    try {
//        const DataSourcePtr source = createDefaultDataSourceWithSchemaForOneBuffer(this->schema,
//                                                                                   this->nodeEngine->getBufferManager(),
//                                                                                   this->nodeEngine->getQueryManager(),
//                                                                                   OperatorId(operatorId),
//                                                                                   OriginId(originId),
//                                                                                   this->statisticId,
//                                                                                   this->numSourceLocalBuffersDefault,
//                                                                                   "defaultPhysicalSourceName",
//                                                                                   {});
//    } catch (Exceptions::RuntimeException const& ex) {
//        SUCCEED();
//        return;
//    }
//    FAIL();
//}
//
//TEST_F(SourceTest, testDataSourceGetSchema) {
//    const DataSourcePtr source =
//        createDefaultDataSourceWithSchemaForOneBuffer(this->schema,
//                                                      this->nodeEngine->getBufferManager(),
//                                                      this->nodeEngine->getQueryManager(),
//                                                      OperatorId(operatorId),
//                                                      OriginId(originId),
//                                                      this->statisticId,
//                                                      this->numSourceLocalBuffersDefault,
//                                                      "defaultPhysicalSourceName",
//                                                      {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                        1,
//                                                                                        SharedQueryId(1),
//                                                                                        DecomposedQueryId(1),
//                                                                                        DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(source->getSchema(), this->schema);
//}
//
//TEST_F(SourceTest, testDataSourceRunningImmediately) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ASSERT_FALSE(mDataSource.isRunning());
//}
//
//TEST_F(SourceTest, testDataSourceStartSideEffectRunningTrue) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
//    EXPECT_TRUE(mDataSource.start());
//    EXPECT_TRUE(mDataSource.isRunning());// the publicly visible side-effect
//    EXPECT_TRUE(mDataSource.stop(Runtime::QueryTerminationType::HardStop));
//}
//
//TEST_F(SourceTest, testDataSourceStartTwiceNoSideEffect) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
//    EXPECT_TRUE(mDataSource.start());
//    EXPECT_FALSE(mDataSource.start());
//    EXPECT_TRUE(mDataSource.isRunning());// the publicly visible side-effect
//    EXPECT_TRUE(mDataSource.stop(Runtime::QueryTerminationType::HardStop));
//}
//
//TEST_F(SourceTest, testDataSourceStopImmediately) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ASSERT_TRUE(mDataSource.stop(Runtime::QueryTerminationType::HardStop));
//}
//
//TEST_F(SourceTest, testDataSourceStopSideEffect) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
//    EXPECT_TRUE(mDataSource.start());
//    EXPECT_TRUE(mDataSource.isRunning());
//    EXPECT_TRUE(mDataSource.stop(Runtime::QueryTerminationType::HardStop));
//    EXPECT_FALSE(mDataSource.isRunning());// the publicly visible side-effect
//}
//
//TEST_F(SourceTest, testDataSourceHardStopSideEffect) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
//    EXPECT_TRUE(mDataSource.start());
//    EXPECT_TRUE(mDataSource.isRunning());
//    EXPECT_EQ(mDataSource.wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);
//    EXPECT_TRUE(mDataSource.stop(Runtime::QueryTerminationType::Graceful));
//    EXPECT_FALSE(mDataSource.isRunning());
//    EXPECT_EQ(mDataSource.wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);// private side-effect, use FRIEND_TEST
//}
//
//TEST_F(SourceTest, testDataSourceGracefulStopSideEffect) {
//    MockDataSourceWithRunningRoutine mDataSource(this->schema,
//                                                 this->nodeEngine->getBufferManager(),
//                                                 this->nodeEngine->getQueryManager(),
//                                                 OperatorId(operatorId),
//                                                 this->numSourceLocalBuffersDefault,
//                                                 GatheringMode::INTERVAL_MODE,
//                                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                   1,
//                                                                                   SharedQueryId(1),
//                                                                                   DecomposedQueryId(1),
//                                                                                   DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, getType()).WillByDefault(Return(SourceType::DEFAULT_SOURCE));
//    EXPECT_TRUE(mDataSource.start());
//    EXPECT_TRUE(mDataSource.isRunning());
//    EXPECT_EQ(mDataSource.wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);
//    EXPECT_TRUE(mDataSource.stop(Runtime::QueryTerminationType::HardStop));
//    EXPECT_FALSE(mDataSource.isRunning());
//    EXPECT_EQ(mDataSource.wasGracefullyStopped, Runtime::QueryTerminationType::HardStop);// private side-effect, use FRIEND_TEST
//}
//
//TEST_F(SourceTest, testDataSourceRunningRoutineGatheringInterval) {
//    auto mDataSource = MockDataSource::create(this->schema,
//                                              this->nodeEngine->getBufferManager(),
//                                              this->nodeEngine->getQueryManager(),
//                                              OperatorId(operatorId),
//                                              this->numSourceLocalBuffersDefault,
//                                              GatheringMode::INTERVAL_MODE,
//                                              {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                1,
//                                                                                SharedQueryId(1),
//                                                                                DecomposedQueryId(1),
//                                                                                DecomposedQueryPlanVersion(0))});
//    ON_CALL(*mDataSource, runningRoutineWithGatheringInterval()).WillByDefault(Return());
//    EXPECT_CALL(*mDataSource, runningRoutineWithGatheringInterval()).Times(Exactly(1));
//    mDataSource->runningRoutine();
//}
//
//TEST_F(SourceTest, testDataSourceRunningRoutineIngestion) {
//    MockDataSource mDataSource(this->schema,
//                               this->nodeEngine->getBufferManager(),
//                               this->nodeEngine->getQueryManager(),
//                               OperatorId(operatorId),
//                               this->numSourceLocalBuffersDefault,
//                               GatheringMode::INGESTION_RATE_MODE,
//                               {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                 1,
//                                                                 SharedQueryId(1),
//                                                                 DecomposedQueryId(1),
//                                                                 DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, runningRoutineWithIngestionRate()).WillByDefault(Return());
//    EXPECT_CALL(mDataSource, runningRoutineWithIngestionRate()).Times(Exactly(1));
//    mDataSource.runningRoutine();
//}
//
//// Disabled: ADAPTIVE is not currently supported
//TEST_F(SourceTest, DISABLED_testDataSourceRunningRoutineKalmanFilter) {
//    MockDataSource mDataSource(this->schema,
//                               this->nodeEngine->getBufferManager(),
//                               this->nodeEngine->getQueryManager(),
//                               OperatorId(operatorId),
//                               this->numSourceLocalBuffersDefault,
//                               GatheringMode::ADAPTIVE_MODE,
//                               {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                 1,
//                                                                 SharedQueryId(1),
//                                                                 DecomposedQueryId(1),
//                                                                 DecomposedQueryPlanVersion(0))});
//    ON_CALL(mDataSource, runningRoutineAdaptiveGatheringInterval()).WillByDefault(Return());
//    EXPECT_CALL(mDataSource, runningRoutineAdaptiveGatheringInterval()).Times(Exactly(1));
//    mDataSource.runningRoutine();
//}
//
//TEST_F(SourceTest, testDataSourceGatheringIntervalRoutineBufWithValue) {
//    // create executable stage
//    auto executableStage = std::make_shared<MockedExecutablePipeline>();
//    // create sink
//    auto sink = createCSVFileSink(this->schema,
//                                  SharedQueryId(this->queryId),
//                                  DecomposedQueryId(this->queryId),
//                                  DecomposedQueryPlanVersion(0),
//                                  this->nodeEngine,
//                                  1,
//                                  "source-test-freq-routine.csv",
//                                  false);
//    // get mocked pipeline to add to source
//    auto pipeline = this->createExecutablePipeline(executableStage, sink);
//    // mock query manager for passing addEndOfStream
//    DataSourceProxyPtr mDataSource = createDataSourceProxy(this->schema,
//                                                           this->nodeEngine->getBufferManager(),
//                                                           this->nodeEngine->getQueryManager(),
//                                                           OperatorId(operatorId),
//                                                           this->numSourceLocalBuffersDefault,
//                                                           GatheringMode::INTERVAL_MODE,
//                                                           {pipeline});
//    mDataSource->numberOfBuffersToProduce = 1;
//    mDataSource->running = true;
//    mDataSource->wasGracefullyStopped = Runtime::QueryTerminationType::Graceful;
//    auto fakeBuf = mDataSource->getRecyclableBuffer();
//    ON_CALL(*mDataSource, toString()).WillByDefault(Return("MOCKED SOURCE"));
//    ON_CALL(*mDataSource, getType()).WillByDefault(Return(SourceType::CSV_SOURCE));
//    ON_CALL(*mDataSource, receiveData()).WillByDefault(Return(fakeBuf));
//    ON_CALL(*mDataSource, emitWork(_, _)).WillByDefault(Return());
//    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(SharedQueryId(this->queryId),
//                                                                         DecomposedQueryId(this->queryId),
//                                                                         DecomposedQueryPlanVersion(0),
//                                                                         {mDataSource},
//                                                                         {sink},
//                                                                         {pipeline},
//                                                                         this->nodeEngine->getQueryManager(),
//                                                                         this->nodeEngine->getBufferManager());
//    ASSERT_TRUE(this->nodeEngine->registerExecutableQueryPlan(executionPlan, false));
//    ASSERT_TRUE(
//        this->nodeEngine->startDecomposedQueryPlan(executionPlan->getSharedQueryId(), executionPlan->getDecomposedQueryId()));
//    ASSERT_EQ(this->nodeEngine->getQueryStatus(SharedQueryId(this->queryId)),
//              Runtime::Execution::ExecutableQueryPlanStatus::Running);
//    EXPECT_CALL(*mDataSource, receiveData()).Times(Exactly(1));
//    EXPECT_CALL(*mDataSource, emitWork(_, _)).Times(Exactly(1));
//    mDataSource->runningRoutine();
//    EXPECT_FALSE(mDataSource->running);
//    EXPECT_EQ(mDataSource->wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);
//    EXPECT_TRUE(Mock::VerifyAndClearExpectations(mDataSource.get()));
//}
//
//TEST_F(SourceTest, testDataSourceIngestionRoutineBufWithValue) {
//    // create executable stage
//    auto executableStage = std::make_shared<MockedExecutablePipeline>();
//    // create sink
//    auto sink = createCSVFileSink(this->schema,
//                                  SharedQueryId(this->queryId),
//                                  DecomposedQueryId(this->queryId),
//                                  DecomposedQueryPlanVersion(0),
//                                  this->nodeEngine,
//                                  1,
//                                  "source-test-ingest-routine.csv",
//                                  false);
//    // get mocked pipeline to add to source
//    auto pipeline = this->createExecutablePipeline(executableStage, sink);
//    // mock query manager for passing addEndOfStream
//    DataSourceProxyPtr mDataSource = createDataSourceProxy(this->schema,
//                                                           this->nodeEngine->getBufferManager(),
//                                                           this->nodeEngine->getQueryManager(),
//                                                           OperatorId(operatorId),
//                                                           this->numSourceLocalBuffersDefault,
//                                                           GatheringMode::INGESTION_RATE_MODE,
//                                                           {pipeline});
//    mDataSource->numberOfBuffersToProduce = 1;
//    mDataSource->running = true;
//    mDataSource->wasGracefullyStopped = Runtime::QueryTerminationType::Graceful;
//    mDataSource->gatheringIngestionRate = 11;
//    auto fakeBuf = mDataSource->getRecyclableBuffer();
//    ON_CALL(*mDataSource, toString()).WillByDefault(Return("MOCKED SOURCE"));
//    ON_CALL(*mDataSource, getType()).WillByDefault(Return(SourceType::LAMBDA_SOURCE));
//    ON_CALL(*mDataSource, receiveData()).WillByDefault(Return(fakeBuf));
//    ON_CALL(*mDataSource, emitWork(_, _)).WillByDefault(Return());
//    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(SharedQueryId(this->queryId),
//                                                                         DecomposedQueryId(this->queryId),
//                                                                         DecomposedQueryPlanVersion(0),
//                                                                         {mDataSource},
//                                                                         {sink},
//                                                                         {pipeline},
//                                                                         this->nodeEngine->getQueryManager(),
//                                                                         this->nodeEngine->getBufferManager());
//    ASSERT_TRUE(this->nodeEngine->registerExecutableQueryPlan(executionPlan, false));
//    ASSERT_TRUE(
//        this->nodeEngine->startDecomposedQueryPlan(executionPlan->getSharedQueryId(), executionPlan->getDecomposedQueryId()));
//    ASSERT_EQ(this->nodeEngine->getQueryStatus(SharedQueryId(this->queryId)),
//              Runtime::Execution::ExecutableQueryPlanStatus::Running);
//    EXPECT_CALL(*mDataSource, receiveData()).Times(Exactly(1));
//    EXPECT_CALL(*mDataSource, emitWork(_, _)).Times(Exactly(1)).WillOnce(InvokeWithoutArgs([&]() {
//        mDataSource->running = false;
//        return;
//    }));
//    mDataSource->runningRoutine();
//    EXPECT_FALSE(mDataSource->running);
//    EXPECT_EQ(mDataSource->wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);
//    EXPECT_TRUE(Mock::VerifyAndClearExpectations(mDataSource.get()));
//}
//
//// Disabled: ADAPTIVE is not currently supported
///*
//TEST_F(SourceTest, DISABLED_testDataSourceKFRoutineBufWithValue) {
//    // create executable stage
//    auto executableStage = std::make_shared<MockedExecutablePipeline>();
//    // create sink
//    auto sink =
//        createCSVFileSink(this->schema, this->queryId, this->queryId, this->nodeEngine, 1, "source-test-kf-routine.csv", false);
//    // get mocked pipeline to add to source
//    auto pipeline = this->createExecutablePipeline(executableStage, sink);
//    // mock query manager for passing addEndOfStream
//    DataSourceProxyPtr mDataSource = createDataSourceProxy(this->schema,
//                                                           this->nodeEngine->getBufferManager(),
//                                                           this->nodeEngine->getQueryManager(),
//                                                           operatorId,
//                                                           this->numSourceLocalBuffersDefault,
//                                                           GatheringMode::ADAPTIVE_MODE,
//                                                           {pipeline});
//    mDataSource->numberOfBuffersToProduce = 1;
//    mDataSource->running = true;
//    mDataSource->wasGracefullyStopped = Runtime::QueryTerminationType::Graceful;
//    mDataSource->gatheringIngestionRate = 1;
//    auto fakeBuf = mDataSource->getRecyclableBuffer();
//    ON_CALL(*mDataSource, toString()).WillByDefault(Return("MOCKED SOURCE"));
//    ON_CALL(*mDataSource, getType()).WillByDefault(Return(SourceType::LAMBDA_SOURCE));
//    ON_CALL(*mDataSource, receiveData()).WillByDefault(Return(fakeBuf));
//    ON_CALL(*mDataSource, emitWork(_, _)).WillByDefault(Return());
//    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(this->queryId,
//                                                                         this->queryId,
//                                                                         DecomposedQueryPlanVersion(0),
//                                                                         {mDataSource},
//                                                                         {sink},
//                                                                         {pipeline},
//                                                                         this->nodeEngine->getQueryManager(),
//                                                                         this->nodeEngine->getBufferManager());
//    ASSERT_TRUE(this->nodeEngine->registerExecutableQueryPlan(executionPlan));
//    ASSERT_TRUE(this->nodeEngine->startQuery(this->queryId));
//    ASSERT_EQ(this->nodeEngine->getQueryStatus(this->queryId), Runtime::Execution::ExecutableQueryPlanStatus::Running);
//    EXPECT_CALL(*mDataSource, receiveData()).Times(Exactly(1));
//    EXPECT_CALL(*mDataSource, emitWork(_, _)).Times(Exactly(1)).WillOnce(InvokeWithoutArgs([&]() {
//        mDataSource->running = false;
//        return;
//    }));
//    mDataSource->runningRoutine();
//    EXPECT_FALSE(mDataSource->running);
//    EXPECT_EQ(mDataSource->wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);
//    EXPECT_TRUE(Mock::VerifyAndClearExpectations(mDataSource.get()));
//    mDataSource.reset();
//} */
//
//// Disabled: ADAPTIVE is not currently supported
///*
//TEST_F(SourceTest, testDataSourceKFRoutineBufWithValueZeroIntervalUpdate) {
//    // create executable stage
//    auto executableStage = std::make_shared<MockedExecutablePipeline>();
//    // create sink
//    auto sink =
//        createCSVFileSink(this->schema, this->queryId, this->queryId, this->nodeEngine, 1, "source-test-kf-routine.csv", false);
//    // get mocked pipeline to add to source
//    auto pipeline = this->createExecutablePipeline(executableStage, sink);
//    // mock query manager for passing addEndOfStream
//    DataSourceProxyPtr mDataSource = createDataSourceProxy(this->schema,
//                                                           this->nodeEngine->getBufferManager(),
//                                                           this->nodeEngine->getQueryManager(),
//                                                           operatorId,
//                                                           this->numSourceLocalBuffersDefault,
//                                                           GatheringMode::ADAPTIVE_MODE,
//                                                           {pipeline});
//    mDataSource->numberOfBuffersToProduce = 1;
//    mDataSource->running = true;
//    mDataSource->wasGracefullyStopped = Runtime::QueryTerminationType::Graceful;
//    auto fakeBuf = mDataSource->getRecyclableBuffer();
//    ON_CALL(*mDataSource, toString()).WillByDefault(Return("MOCKED SOURCE"));
//    ON_CALL(*mDataSource, getType()).WillByDefault(Return(SourceType::LAMBDA_SOURCE));
//    ON_CALL(*mDataSource, receiveData()).WillByDefault(Return(fakeBuf));
//    ON_CALL(*mDataSource, emitWork(_, _)).WillByDefault(Return());
//    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(this->queryId,
//                                                                         this->queryId,
//                                                                         DecomposedQueryPlanVersion(0),
//                                                                         {mDataSource},
//                                                                         {sink},
//                                                                         {pipeline},
//                                                                         this->nodeEngine->getQueryManager(),
//                                                                         this->nodeEngine->getBufferManager());
//    ASSERT_TRUE(this->nodeEngine->registerExecutableQueryPlan(executionPlan));
//    ASSERT_TRUE(this->nodeEngine->startQuery(this->queryId));
//    ASSERT_EQ(this->nodeEngine->getQueryStatus(this->queryId), Runtime::Execution::ExecutableQueryPlanStatus::Running);
//    auto oldInterval = mDataSource->gatheringInterval;
//    EXPECT_CALL(*mDataSource, receiveData()).Times(Exactly(1));
//    EXPECT_CALL(*mDataSource, emitWork(_, _)).Times(Exactly(1)).WillOnce(InvokeWithoutArgs([&]() {
//        mDataSource->running = false;
//        return;
//    }));
//    mDataSource->runningRoutine();
//    EXPECT_EQ(oldInterval.count(), mDataSource->gatheringInterval.count());
//    EXPECT_EQ(oldInterval.count(), 0);
//    EXPECT_FALSE(mDataSource->running);
//    EXPECT_EQ(mDataSource->wasGracefullyStopped, Runtime::QueryTerminationType::Graceful);
//    EXPECT_TRUE(Mock::VerifyAndClearExpectations(mDataSource.get()));
//} */
//
//TEST_F(SourceTest, testDataSourceOpen) {
//    DataSourceProxy mDataSource(this->schema,
//                                this->nodeEngine->getBufferManager(),
//                                this->nodeEngine->getQueryManager(),
//                                OperatorId(operatorId),
//                                this->numSourceLocalBuffersDefault,
//                                GatheringMode::INGESTION_RATE_MODE,
//                                {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                  1,
//                                                                  SharedQueryId(1),
//                                                                  DecomposedQueryId(1),
//                                                                  DecomposedQueryPlanVersion(0))});
//    // EXPECT_ANY_THROW(mDataSource.bufferManager->getAvailableBuffers()); currently not possible w/ Error: success :)
//    mDataSource.open();
//    auto size = mDataSource.bufferManager->getAvailableBuffers();
//    EXPECT_EQ(size, this->numSourceLocalBuffersDefault);
//}
//
//TEST_F(SourceTest, testBinarySourceGetType) {
//    BinarySourceProxy bDataSource(this->schema,
//                                  this->nodeEngine->getBufferManager(),
//                                  this->nodeEngine->getQueryManager(),
//                                  this->path_to_bin_file,
//                                  OperatorId(operatorId),
//                                  this->numSourceLocalBuffersDefault,
//                                  {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                    1,
//                                                                    SharedQueryId(1),
//                                                                    DecomposedQueryId(1),
//                                                                    DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(bDataSource.getType(), NES::SourceType::BINARY_SOURCE);
//}
//
//TEST_F(SourceTest, testBinarySourceWrongPath) {
//    ASSERT_THROW(
//        try {
//            BinarySourceProxy bDataSource(this->schema,
//                                          this->nodeEngine->getBufferManager(),
//                                          this->nodeEngine->getQueryManager(),
//                                          this->wrong_filepath,
//                                          OperatorId(operatorId),
//                                          this->numSourceLocalBuffersDefault,
//                                          {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                            1,
//                                                                            SharedQueryId(1),
//                                                                            DecomposedQueryId(1),
//                                                                            DecomposedQueryPlanVersion(0))});
//            ASSERT_FALSE(bDataSource.input.is_open());
//        } catch (std::exception const& exception) {
//            auto msg = std::string(exception.what());
//            ASSERT_NE(msg.find(std::string("Binary input file is not valid")), std::string::npos);
//            throw;
//        },
//        Exceptions::RuntimeException);
//}
//
//TEST_F(SourceTest, testBinarySourceCorrectPath) {
//    BinarySourceProxy bDataSource(this->schema,
//                                  this->nodeEngine->getBufferManager(),
//                                  this->nodeEngine->getQueryManager(),
//                                  this->path_to_bin_file,
//                                  OperatorId(operatorId),
//                                  this->numSourceLocalBuffersDefault,
//                                  {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                    1,
//                                                                    SharedQueryId(1),
//                                                                    DecomposedQueryId(1),
//                                                                    DecomposedQueryPlanVersion(0))});
//    ASSERT_TRUE(bDataSource.input.is_open());
//}
//
//TEST_F(SourceTest, testBinarySourceFillBuffer) {
//    BinarySourceProxy bDataSource(this->schema,
//                                  this->nodeEngine->getBufferManager(),
//                                  this->nodeEngine->getQueryManager(),
//                                  this->path_to_bin_file,
//                                  OperatorId(operatorId),
//                                  this->numSourceLocalBuffersDefault,
//                                  {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                    1,
//                                                                    SharedQueryId(1),
//                                                                    DecomposedQueryId(1),
//                                                                    DecomposedQueryPlanVersion(0))});
//    uint64_t tuple_size = this->schema->getSchemaSizeInBytes();
//    uint64_t buffer_size = this->nodeEngine->getBufferManager()->getBufferSize();
//    uint64_t numberOfBuffers = 1;// increased by 1 every fillBuffer()
//    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);
//    auto buf = this->GetEmptyBuffer();
//    ASSERT_EQ(bDataSource.getNumberOfGeneratedTuples(), 0u);
//    ASSERT_EQ(bDataSource.getNumberOfGeneratedBuffers(), 0u);
//    bDataSource.fillBuffer(*buf);
//    EXPECT_EQ(bDataSource.getNumberOfGeneratedTuples(), numberOfTuplesToProcess);
//    EXPECT_EQ(bDataSource.getNumberOfGeneratedBuffers(), numberOfBuffers);
//    EXPECT_STREQ(buf->getBuffer<ysbRecord>()->ad_type, "banner78");
//}
//
//TEST_F(SourceTest, testBinarySourceFillBufferRandomTimes) {
//    BinarySourceProxy bDataSource(this->schema,
//                                  this->nodeEngine->getBufferManager(),
//                                  this->nodeEngine->getQueryManager(),
//                                  this->path_to_bin_file,
//                                  OperatorId(operatorId),
//                                  this->numSourceLocalBuffersDefault,
//                                  {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                    1,
//                                                                    SharedQueryId(1),
//                                                                    DecomposedQueryId(1),
//                                                                    DecomposedQueryPlanVersion(0))});
//    uint64_t tuple_size = this->schema->getSchemaSizeInBytes();
//    uint64_t buffer_size = this->nodeEngine->getBufferManager()->getBufferSize();
//    uint64_t numberOfBuffers = 1;// increased by 1 every fillBuffer()
//    uint64_t numberOfTuplesToProcess = numberOfBuffers * (buffer_size / tuple_size);
//    auto buf = this->GetEmptyBuffer();
//    auto iterations = rand() % 5;
//    ASSERT_EQ(bDataSource.getNumberOfGeneratedTuples(), 0u);
//    ASSERT_EQ(bDataSource.getNumberOfGeneratedBuffers(), 0u);
//    for (int i = 0; i < iterations; ++i) {
//        bDataSource.fillBuffer(*buf);
//        EXPECT_EQ(bDataSource.getNumberOfGeneratedTuples(), (i + 1) * numberOfTuplesToProcess);
//        EXPECT_EQ(bDataSource.getNumberOfGeneratedBuffers(), (i + 1) * numberOfBuffers);
//    }
//    EXPECT_EQ(bDataSource.getNumberOfGeneratedTuples(), iterations * numberOfTuplesToProcess);
//    EXPECT_EQ(bDataSource.getNumberOfGeneratedBuffers(), iterations * numberOfBuffers);
//}
//
//TEST_F(SourceTest, testBinarySourceFillBufferContents) {
//    BinarySourceProxy bDataSource(this->schema,
//                                  this->nodeEngine->getBufferManager(),
//                                  this->nodeEngine->getQueryManager(),
//                                  this->path_to_bin_file,
//                                  OperatorId(operatorId),
//                                  this->numSourceLocalBuffersDefault,
//                                  {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                    1,
//                                                                    SharedQueryId(1),
//                                                                    DecomposedQueryId(1),
//                                                                    DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    bDataSource.fillBuffer(*buf);
//    auto content = buf->getBuffer<ysbRecord>();
//    EXPECT_STREQ(content->ad_type, "banner78");
//    EXPECT_TRUE((!strcmp(content->event_type, "view") || !strcmp(content->event_type, "click")
//                 || !strcmp(content->event_type, "purchase")));
//}
//
//TEST_F(SourceTest, testCSVSourceGetType) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file);
//    sourceConfig->setNumberOfBuffersToProduce(0);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(csvDataSource.getType(), NES::SourceType::CSV_SOURCE);
//}
//
//TEST_F(SourceTest, testCSVSourceWrongFilePath) {
//    auto csvSourceType = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    csvSourceType->setFilePath(this->wrong_filepath);
//    csvSourceType->setNumberOfBuffersToProduce(0);
//    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
//    csvSourceType->setGatheringInterval(this->gatheringInterval);
//
//    try {
//        CSVSourceProxy csvDataSource(this->schema,
//                                     this->nodeEngine->getBufferManager(),
//                                     this->nodeEngine->getQueryManager(),
//                                     csvSourceType,
//                                     OperatorId(operatorId),
//                                     this->numSourceLocalBuffersDefault,
//                                     {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                       1,
//                                                                       SharedQueryId(1),
//                                                                       DecomposedQueryId(1),
//                                                                       DecomposedQueryPlanVersion(0))});
//        ASSERT_FALSE(csvDataSource.input.is_open());
//    } catch (std::exception const& err) {
//        std::string msg = err.what();
//        EXPECT_TRUE(msg.find(std::string("Could not determine absolute pathname")) != std::string::npos);
//    }
//}
//
//TEST_F(SourceTest, testCSVSourceCorrectFilePath) {
//    CSVSourceTypePtr csvSourceType = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    csvSourceType->setFilePath(this->path_to_file);
//    csvSourceType->setNumberOfBuffersToProduce(0);
//    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
//    csvSourceType->setGatheringInterval(this->gatheringInterval);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 csvSourceType,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    ASSERT_TRUE(csvDataSource.input.is_open());
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferFileEnded) {
//    CSVSourceTypePtr csvSourceType = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    csvSourceType->setFilePath(this->path_to_file);
//    csvSourceType->setNumberOfBuffersToProduce(0);
//    csvSourceType->setNumberOfTuplesToProducePerBuffer(0);
//    csvSourceType->setGatheringInterval(this->gatheringInterval);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 csvSourceType,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    csvDataSource.fileEnded = true;
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    EXPECT_EQ(buf->getNumberOfTuples(), 0u);
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferOnce) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file);
//    sourceConfig->setNumberOfBuffersToProduce(1);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    sourceConfig->setSkipHeader(true);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    ASSERT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 0u);
//    ASSERT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 0u);
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 1u);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 1u);
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferOnceColumnLayout) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file);
//    sourceConfig->setNumberOfBuffersToProduce(1);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    sourceConfig->setSkipHeader(true);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    ASSERT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 0u);
//    ASSERT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 0u);
//    std::shared_ptr<Runtime::MemoryLayouts::ColumnLayout> layoutPtr =
//        Runtime::MemoryLayouts::ColumnLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), 1u);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), 1u);
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferContentsHeaderFailure) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file_head);
//    sourceConfig->setNumberOfBuffersToProduce(1);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//
//    // read actual header, get error from casting input to schema
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    try {
//        Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//            Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//        Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//        csvDataSource.fillBuffer(buffer);
//    } catch (std::invalid_argument const& err) {// 1/2 throwables from stoull
//        // TODO: is the "overwrite" of the message a good thing?
//        // EXPECT_EQ(err.what(),std::string("Invalid argument"));
//        std::string msg = err.what();
//        EXPECT_TRUE(msg.find(std::string("stoull")) != std::string::npos);
//    } catch (std::out_of_range const& err) {// 2/2 throwables from stoull
//        EXPECT_EQ(err.what(), std::string("Out of range"));
//    } catch (...) {
//        FAIL() << "Uncaught exception in test for file with headers!" << std::endl;
//    }
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferContentsHeaderFailureColumnLayout) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file_head);
//    sourceConfig->setNumberOfBuffersToProduce(1);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//
//    // read actual header, get error from casting input to schema
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    try {
//        Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//            Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//        Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//        csvDataSource.fillBuffer(buffer);
//    } catch (std::invalid_argument const& err) {// 1/2 throwables from stoull
//        // TODO: is the "overwrite" of the message a good thing?
//        // EXPECT_EQ(err.what(),std::string("Invalid argument"));
//        std::string msg = err.what();
//        EXPECT_TRUE(msg.find(std::string("stoull")) != std::string::npos);
//    } catch (std::out_of_range const& err) {// 2/2 throwables from stoull
//        EXPECT_EQ(err.what(), std::string("Out of range"));
//    } catch (...) {
//        FAIL() << "Uncaught exception in test for file with headers!" << std::endl;
//    }
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferContentsSkipHeader) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file_head);
//    sourceConfig->setNumberOfBuffersToProduce(1);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    sourceConfig->setSkipHeader(true);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    auto content = buf->getBuffer<ysbRecord>();
//    EXPECT_STREQ(content->ad_type, "banner78");
//    EXPECT_TRUE((!strcmp(content->event_type, "view") || !strcmp(content->event_type, "click")
//                 || !strcmp(content->event_type, "purchase")));
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferContentsSkipHeaderColumnLayout) {
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file_head);
//    sourceConfig->setNumberOfBuffersToProduce(1);
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    sourceConfig->setSkipHeader(true);
//
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    auto content = buf->getBuffer<ysbRecord>();
//    EXPECT_STREQ(content->ad_type, "banner78");
//    EXPECT_TRUE((!strcmp(content->event_type, "view") || !strcmp(content->event_type, "click")
//                 || !strcmp(content->event_type, "purchase")));
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferFullFileColumnLayout) {
//    // Full pass: 52 tuples in first buffer, 48 in second
//    // expectedNumberOfBuffers in c-tor, no looping
//    uint64_t expectedNumberOfTuples = 100;
//    uint64_t expectedNumberOfBuffers = 2;
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file);
//    sourceConfig->setNumberOfBuffersToProduce(expectedNumberOfBuffers);// file is not going to loop
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    ASSERT_FALSE(csvDataSource.fileEnded);
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    while (csvDataSource.getNumberOfGeneratedBuffers() < expectedNumberOfBuffers) {// relative to file size
//        csvDataSource.fillBuffer(buffer);
//        EXPECT_NE(buf->getNumberOfTuples(), 0u);
//        EXPECT_TRUE(buf.has_value());
//        for (uint64_t i = 0; i < buf->getNumberOfTuples(); i++) {
//            auto tuple = buf->getBuffer<ysbRecord>();
//            EXPECT_STREQ(tuple->ad_type, "banner78");
//            EXPECT_TRUE((!strcmp(tuple->event_type, "view") || !strcmp(tuple->event_type, "click")
//                         || !strcmp(tuple->event_type, "purchase")));
//        }
//    }
//    EXPECT_TRUE(csvDataSource.fileEnded);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), expectedNumberOfTuples);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), expectedNumberOfBuffers);
//}
//
//TEST_F(SourceTest, testCSVSourceFillBufferFullFile) {
//    // Full pass: 52 tuples in first buffer, 48 in second
//    // expectedNumberOfBuffers in c-tor, no looping
//    uint64_t expectedNumberOfTuples = 100;
//    uint64_t expectedNumberOfBuffers = 2;
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(this->path_to_file);
//    sourceConfig->setNumberOfBuffersToProduce(expectedNumberOfBuffers);// file is not going to loop
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    CSVSourceProxy csvDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    ASSERT_FALSE(csvDataSource.fileEnded);
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    while (csvDataSource.getNumberOfGeneratedBuffers() < expectedNumberOfBuffers) {// relative to file size
//        csvDataSource.fillBuffer(buffer);
//        EXPECT_NE(buf->getNumberOfTuples(), 0u);
//        EXPECT_TRUE(buf.has_value());
//        for (uint64_t i = 0; i < buf->getNumberOfTuples(); i++) {
//            auto tuple = buf->getBuffer<ysbRecord>();
//            EXPECT_STREQ(tuple->ad_type, "banner78");
//            EXPECT_TRUE((!strcmp(tuple->event_type, "view") || !strcmp(tuple->event_type, "click")
//                         || !strcmp(tuple->event_type, "purchase")));
//        }
//    }
//    EXPECT_TRUE(csvDataSource.fileEnded);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedTuples(), expectedNumberOfTuples);
//    EXPECT_EQ(csvDataSource.getNumberOfGeneratedBuffers(), expectedNumberOfBuffers);
//}
//
//TEST_F(SourceTest, testCSVSourceIntTypes) {
//    // use custom schema and file, read once
//    SchemaPtr int_schema = Schema::create()
//                               ->addField("uint64", BasicType::UINT64)
//                               ->addField("int64", BasicType::INT64)
//                               ->addField("uint32", BasicType::UINT32)
//                               ->addField("int32", BasicType::INT32)
//                               ->addField("uint16", BasicType::UINT16)
//                               ->addField("int16", BasicType::INT16)
//                               ->addField("uint8", BasicType::UINT8)
//                               ->addField("int8", BasicType::INT8);
//
//    std::string path_to_int_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "every-int.csv";
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(path_to_int_file);
//    sourceConfig->setNumberOfBuffersToProduce(1);// file not looping
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    CSVSourceProxy csvDataSource(int_schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//
//    NES_DEBUG("{}", int_schema->toString());
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(int_schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    auto content = buf->getBuffer<everyIntTypeRecord>();
//    // number is in the expected range
//    EXPECT_LE(content->uint64_entry, std::numeric_limits<uint64_t>::max());
//    EXPECT_GE(content->uint64_entry, std::numeric_limits<uint64_t>::min());
//    // unsigned number is equal to max, no specific numbers in code
//    EXPECT_EQ(content->uint64_entry, std::numeric_limits<uint64_t>::max());
//    // number is in the expected range
//    EXPECT_LE(content->int64_entry, std::numeric_limits<int64_t>::max());
//    EXPECT_GE(content->int64_entry, std::numeric_limits<int64_t>::min());
//    // checks for min, covers signed case
//    EXPECT_EQ(content->int64_entry, std::numeric_limits<int64_t>::min());
//
//    EXPECT_LE(content->uint32_entry, std::numeric_limits<uint32_t>::max());
//    EXPECT_GE(content->uint32_entry, std::numeric_limits<uint32_t>::min());
//    EXPECT_EQ(content->uint32_entry, std::numeric_limits<uint32_t>::max());
//
//    EXPECT_LE(content->int32_entry, std::numeric_limits<int32_t>::max());
//    EXPECT_GE(content->int32_entry, std::numeric_limits<int32_t>::min());
//    EXPECT_EQ(content->int32_entry, std::numeric_limits<int32_t>::min());
//
//    EXPECT_LE(content->uint16_entry, std::numeric_limits<uint16_t>::max());
//    EXPECT_GE(content->uint16_entry, std::numeric_limits<uint16_t>::min());
//    EXPECT_EQ(content->uint16_entry, std::numeric_limits<uint16_t>::max());
//
//    EXPECT_LE(content->int16_entry, std::numeric_limits<int16_t>::max());
//    EXPECT_GE(content->int16_entry, std::numeric_limits<int16_t>::min());
//    EXPECT_EQ(content->int16_entry, std::numeric_limits<int16_t>::min());
//
//    EXPECT_LE(content->uint8_entry, std::numeric_limits<uint8_t>::max());
//    EXPECT_GE(content->uint8_entry, std::numeric_limits<uint8_t>::min());
//    EXPECT_EQ(content->uint8_entry, std::numeric_limits<uint8_t>::max());
//
//    EXPECT_LE(content->int8_entry, std::numeric_limits<int8_t>::max());
//    EXPECT_GE(content->int8_entry, std::numeric_limits<int8_t>::min());
//    EXPECT_EQ(content->int8_entry, std::numeric_limits<int8_t>::min());
//}
//
//TEST_F(SourceTest, testCSVSourceFloatTypes) {
//    // use custom schema and file, read once
//    SchemaPtr float_schema = Schema::create()->addField("float64", BasicType::FLOAT64)->addField("float32", BasicType::FLOAT32);
//    std::string path_to_float_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "every-float.csv";
//    CSVSourceTypePtr sourceConfig = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    sourceConfig->setFilePath(path_to_float_file);
//    sourceConfig->setNumberOfBuffersToProduce(1);// file is not going to loop
//    sourceConfig->setNumberOfTuplesToProducePerBuffer(1);
//    sourceConfig->setGatheringInterval(this->gatheringInterval);
//    CSVSourceProxy csvDataSource(float_schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(float_schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    auto content = buf->getBuffer<everyFloatTypeRecord>();
//    EXPECT_LE(content->float64_entry, std::numeric_limits<double>::max());
//    EXPECT_GE(content->float64_entry, std::numeric_limits<double>::min());
//    EXPECT_DOUBLE_EQ(content->float64_entry, std::numeric_limits<double>::max());
//
//    EXPECT_LE(content->float32_entry, std::numeric_limits<float>::max());
//    EXPECT_GE(content->float32_entry, std::numeric_limits<float>::min());
//    EXPECT_FLOAT_EQ(content->float32_entry, std::numeric_limits<float>::max());
//}
//
//TEST_F(SourceTest, testCSVSourceBooleanTypes) {
//    // use custom schema and file, read once
//    SchemaPtr bool_schema = Schema::create()
//                                ->addField("false", BasicType::BOOLEAN)
//                                ->addField("true", BasicType::BOOLEAN)
//                                ->addField("falsey", BasicType::BOOLEAN)
//                                ->addField("truthy", BasicType::BOOLEAN);
//
//    std::string path_to_bool_file = std::filesystem::path(TEST_DATA_DIRECTORY) / "every-boolean.csv";
//
//    CSVSourceTypePtr csvSourceType = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    csvSourceType->setFilePath(path_to_bool_file);
//    csvSourceType->setNumberOfBuffersToProduce(1);// file is not going to loop
//    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
//    csvSourceType->setGatheringInterval(this->gatheringInterval);
//
//    CSVSourceProxy csvDataSource(bool_schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 csvSourceType,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(bool_schema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    auto content = buf->getBuffer<everyBooleanTypeRecord>();
//    EXPECT_FALSE(content->false_entry);
//    EXPECT_TRUE(content->true_entry);
//    EXPECT_FALSE(content->falsey_entry);
//    EXPECT_TRUE(content->truthy_entry);
//}
//
//TEST_F(SourceTest, testCSVSourceCommaFloatingPoint) {
//    CSVSourceTypePtr csvSourceType = CSVSourceType::create("LogicalSourceName", "PhysicalSourceName");
//    csvSourceType->setFilePath(this->path_to_decimals_file);
//    csvSourceType->setDelimiter("*");
//    csvSourceType->setSkipHeader(true);
//    csvSourceType->setNumberOfBuffersToProduce(1);
//    csvSourceType->setNumberOfTuplesToProducePerBuffer(1);
//    csvSourceType->setGatheringInterval(this->gatheringInterval);
//
//    CSVSourceProxy csvDataSource(this->decimalsSchema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 csvSourceType,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//    auto buf = this->GetEmptyBuffer();
//    Runtime::MemoryLayouts::RowLayoutPtr layoutPtr =
//        Runtime::MemoryLayouts::RowLayout::create(this->decimalsSchema, this->nodeEngine->getBufferManager()->getBufferSize());
//    Runtime::MemoryLayouts::TestTupleBuffer buffer = Runtime::MemoryLayouts::TestTupleBuffer(layoutPtr, *buf);
//    csvDataSource.fillBuffer(buffer);
//    auto content = buf->getBuffer<decimalsRecord>();
//    ASSERT_NEAR(content->positive_with_decimal, 9.09, 0.01);
//    ASSERT_NEAR(content->negative_with_decimal, -0.5, 0.01);
//    ASSERT_NEAR(content->longer_precision_decimal, 13.1608002, 0.01);
//}
//
///**
// * Tests basic set up of TCP source
// */
//TEST_F(SourceTest, TCPSourceInit) {
//
//    TCPSourceTypePtr sourceConfig = TCPSourceType::create("LogicalSourceName", "PhysicalSourceName");
//
//    TCPSourceProxy tcpDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//
//    SUCCEED();
//}
//
///**
// * Test if schema and TCP source information are the same
// */
//TEST_F(SourceTest, TCPSourcePrint) {
//    TCPSourceTypePtr sourceConfig = TCPSourceType::create("LogicalSourceName", "PhysicalSourceName");
//
//    sourceConfig->setSocketHost("127.0.0.1");
//    sourceConfig->setSocketPort(5000);
//    sourceConfig->setSocketDomainViaString("AF_INET");
//    sourceConfig->setSocketTypeViaString("SOCK_STREAM");
//
//    TCPSourceProxy tcpDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//
//    std::string expected =
//        "TCPSOURCE(SCHEMA(user_id:ArrayType page_id:ArrayType campaign_id:ArrayType ad_type:ArrayType event_type:ArrayType "
//        "current_ms:INTEGER(64 bits) ip:INTEGER(32 bits)), TCPSourceType => {\nsocketHost: 127.0.0.1\nsocketPort: "
//        "5000\nsocketDomain: "
//        "2\nsocketType: 1\nflushIntervalMS: -1\ninputFormat: CSV\ndecideMessageSize: TUPLE_SEPARATOR\ntupleSeparator: "
//        "\n\nsocketBufferSize: "
//        "0\nbytesUsedForSocketBufferSizeTransfer: 0\n"
//        "persistentTcpSource: 0\n}";
//
//    EXPECT_EQ(tcpDataSource.toString(), expected);
//}
//
///**
// * Test if schema and TCP source information are the same
// */
//TEST_F(SourceTest, TCPSourcePrintWithChangedValues) {
//    TCPSourceTypePtr sourceConfig = TCPSourceType::create("LogicalSourceName", "PhysicalSourceName");
//
//    sourceConfig->setSocketHost("127.0.0.1");
//    sourceConfig->setSocketPort(5000);
//    sourceConfig->setSocketDomainViaString("AF_INET6");
//    sourceConfig->setSocketTypeViaString("SOCK_SEQPACKET");
//    sourceConfig->setFlushIntervalMS(100);
//    sourceConfig->setInputFormat(Configurations::InputFormat::CSV);
//    sourceConfig->setDecideMessageSize(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR);
//    sourceConfig->setTupleSeparator('\n');
//
//    EXPECT_EQ(SOCK_SEQPACKET, sourceConfig->getSocketType()->getValue());
//    EXPECT_EQ(AF_INET6, sourceConfig->getSocketDomain()->getValue());
//
//    TCPSourceProxy tcpDataSource(this->schema,
//                                 this->nodeEngine->getBufferManager(),
//                                 this->nodeEngine->getQueryManager(),
//                                 sourceConfig,
//                                 OperatorId(operatorId),
//                                 this->numSourceLocalBuffersDefault,
//                                 {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                   1,
//                                                                   SharedQueryId(1),
//                                                                   DecomposedQueryId(1),
//                                                                   DecomposedQueryPlanVersion(0))});
//
//    std::string expected =
//        "TCPSOURCE(SCHEMA(user_id:ArrayType page_id:ArrayType campaign_id:ArrayType ad_type:ArrayType event_type:ArrayType "
//        "current_ms:INTEGER(64 bits) ip:INTEGER(32 bits)), TCPSourceType => {\nsocketHost: 127.0.0.1\nsocketPort: "
//        "5000\nsocketDomain: "
//        "10\nsocketType: 5\nflushIntervalMS: 100\ninputFormat: CSV\ndecideMessageSize: TUPLE_SEPARATOR\ntupleSeparator: "
//        "\n\nsocketBufferSize: "
//        "0\nbytesUsedForSocketBufferSizeTransfer: 0\n"
//        "persistentTcpSource: 0\n}";
//
//    EXPECT_EQ(tcpDataSource.toString(), expected);
//}
//
//TEST_F(SourceTest, testGeneratorSourceGetType) {
//    GeneratorSourceProxy genDataSource(this->schema,
//                                       this->nodeEngine->getBufferManager(),
//                                       this->nodeEngine->getQueryManager(),
//                                       1,
//                                       OperatorId(operatorId),
//                                       this->numSourceLocalBuffersDefault,
//                                       GatheringMode::INGESTION_RATE_MODE,
//                                       {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                         1,
//                                                                         SharedQueryId(1),
//                                                                         DecomposedQueryId(1),
//                                                                         DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(genDataSource.getType(), SourceType::TEST_SOURCE);
//}
//
//TEST_F(SourceTest, testDefaultSourceGetType) {
//    DefaultSourceProxy defDataSource(this->schema,
//                                     this->nodeEngine->getBufferManager(),
//                                     this->nodeEngine->getQueryManager(),
//                                     1,
//                                     1000,
//                                     OperatorId(operatorId),
//                                     this->numSourceLocalBuffersDefault,
//                                     {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                       1,
//                                                                       SharedQueryId(1),
//                                                                       DecomposedQueryId(1),
//                                                                       DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(defDataSource.getType(), SourceType::DEFAULT_SOURCE);
//}
//
//TEST_F(SourceTest, testDefaultSourceReceiveData) {
//    // call receiveData, get the generated buffer
//    // assert it has values and the tuples are as many as the predefined size
//    DefaultSourceProxy defDataSource(this->schema,
//                                     this->nodeEngine->getBufferManager(),
//                                     this->nodeEngine->getQueryManager(),
//                                     1,
//                                     1000,
//                                     OperatorId(operatorId),
//                                     this->numSourceLocalBuffersDefault,
//                                     {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                       1,
//                                                                       SharedQueryId(1),
//                                                                       DecomposedQueryId(1),
//                                                                       DecomposedQueryPlanVersion(0))});
//    // open starts the bufferManager, otherwise receiveData will fail
//    defDataSource.open();
//    auto buf = defDataSource.receiveData();
//    EXPECT_TRUE(buf.has_value());
//    EXPECT_EQ(buf->getNumberOfTuples(), 10u);
//}
//
//TEST_F(SourceTest, testLambdaSourceInitAndTypeInterval) {
//    uint64_t numBuffers = 2;
//    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
//        uint64_t currentEventType = 0;
//        auto* ysbRecords = buffer.getBuffer<IngestionRecord>();
//        for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
//            ysbRecords[i].userId = i;
//            ysbRecords[i].pageId = 0;
//            ysbRecords[i].adType = 0;
//            ysbRecords[i].campaignId = rand() % 10000;
//            ysbRecords[i].eventType = (currentEventType++) % 3;
//            ysbRecords[i].currentMs = time(nullptr);
//            ysbRecords[i].ip = 0x01020304;
//            std::cout << "Write rec i=" << i << " content=" << ysbRecords[i].toString() << " size=" << sizeof(IngestionRecord)
//                      << " addr=" << &ysbRecords[i] << std::endl;
//        }
//    };
//
//    LambdaSourceProxy lambdaDataSource(lambdaSchema,
//                                       this->nodeEngine->getBufferManager(),
//                                       this->nodeEngine->getQueryManager(),
//                                       numBuffers,
//                                       0,
//                                       func,
//                                       OperatorId(operatorId),
//                                       12,
//                                       GatheringMode::INTERVAL_MODE,
//                                       {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                         1,
//                                                                         SharedQueryId(1),
//                                                                         DecomposedQueryId(1),
//                                                                         DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(lambdaDataSource.getType(), SourceType::LAMBDA_SOURCE);
//    ASSERT_EQ(lambdaDataSource.getGatheringIntervalCount(), 0u);
//    ASSERT_EQ(lambdaDataSource.numberOfTuplesToProduce, 52u);
//    lambdaDataSource.open();
//
//    while (lambdaDataSource.getNumberOfGeneratedBuffers() < numBuffers) {
//        auto resBuf = lambdaDataSource.receiveData();
//        EXPECT_NE(resBuf, std::nullopt);
//        EXPECT_TRUE(resBuf.has_value());
//        EXPECT_EQ(resBuf->getNumberOfTuples(), lambdaDataSource.numberOfTuplesToProduce);
//        auto ysbRecords = resBuf->getBuffer<IngestionRecord>();
//        for (uint64_t i = 0; i < lambdaDataSource.numberOfTuplesToProduce; ++i) {
//            EXPECT_TRUE(0 <= ysbRecords[i].campaignId && ysbRecords[i].campaignId < 10000);
//            EXPECT_TRUE(0 <= ysbRecords[i].eventType && ysbRecords[i].eventType < 3);
//        }
//    }
//
//    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedBuffers(), numBuffers);
//    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedTuples(), numBuffers * lambdaDataSource.numberOfTuplesToProduce);
//}
//
//TEST_F(SourceTest, testLambdaSourceInitAndTypeIngestion) {
//    uint64_t numBuffers = 2;
//    auto func = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
//        uint64_t currentEventType = 0;
//        auto* ysbRecords = buffer.getBuffer<IngestionRecord>();
//        for (uint64_t i = 0; i < numberOfTuplesToProduce; i++) {
//            ysbRecords[i].userId = i;
//            ysbRecords[i].pageId = 0;
//            ysbRecords[i].adType = 0;
//            ysbRecords[i].campaignId = rand() % 10000;
//            ysbRecords[i].eventType = (currentEventType++) % 3;
//            ysbRecords[i].currentMs = time(nullptr);
//            ysbRecords[i].ip = 0x01020304;
//            std::cout << "Write rec i=" << i << " content=" << ysbRecords[i].toString() << " size=" << sizeof(IngestionRecord)
//                      << " addr=" << &ysbRecords[i] << std::endl;
//        }
//    };
//
//    LambdaSourceProxy lambdaDataSource(lambdaSchema,
//                                       this->nodeEngine->getBufferManager(),
//                                       this->nodeEngine->getQueryManager(),
//                                       numBuffers,
//                                       1,
//                                       func,
//                                       OperatorId(operatorId),
//                                       12,
//                                       GatheringMode::INGESTION_RATE_MODE,
//                                       {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                         1,
//                                                                         SharedQueryId(1),
//                                                                         DecomposedQueryId(1),
//                                                                         DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(lambdaDataSource.getType(), SourceType::LAMBDA_SOURCE);
//    ASSERT_EQ(lambdaDataSource.gatheringIngestionRate, 1u);
//    lambdaDataSource.open();
//
//    while (lambdaDataSource.getNumberOfGeneratedBuffers() < numBuffers) {
//        auto resBuf = lambdaDataSource.receiveData();
//        EXPECT_NE(resBuf, std::nullopt);
//        EXPECT_TRUE(resBuf.has_value());
//        EXPECT_EQ(resBuf->getNumberOfTuples(), lambdaDataSource.numberOfTuplesToProduce);
//        auto ysbRecords = resBuf->getBuffer<IngestionRecord>();
//        for (uint64_t i = 0; i < lambdaDataSource.numberOfTuplesToProduce; ++i) {
//            EXPECT_TRUE(0 <= ysbRecords[i].campaignId && ysbRecords[i].campaignId < 10000);
//            EXPECT_TRUE(0 <= ysbRecords[i].eventType && ysbRecords[i].eventType < 3);
//        }
//    }
//
//    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedBuffers(), numBuffers);
//    EXPECT_EQ(lambdaDataSource.getNumberOfGeneratedTuples(), numBuffers * lambdaDataSource.numberOfTuplesToProduce);
//}
//
//TEST_F(SourceTest, testIngestionRateFromQuery) {
//    NES::CoordinatorConfigurationPtr coordinatorConfig = NES::CoordinatorConfiguration::createDefault();
//    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
//    coordinatorConfig->restPort = *restPort;
//
//    NES_DEBUG("E2EBase: Start coordinator");
//    auto crd = std::make_shared<NES::NesCoordinator>(coordinatorConfig);
//    uint64_t port = crd->startCoordinator(/**blocking**/ false);
//    auto input = TestSchemas::getSchemaTemplate("id_val_time_u64");
//    crd->getSourceCatalog()->addLogicalSource("input1", input);
//
//    NES_DEBUG("E2EBase: Start worker 1");
//    NES::WorkerConfigurationPtr wrkConf = NES::WorkerConfiguration::create();
//    wrkConf->coordinatorPort = port;
//    wrkConf->bufferSizeInBytes = (72);
//
//    auto func1 = [](NES::Runtime::TupleBuffer& buffer, uint64_t numberOfTuplesToProduce) {
//        struct Record {
//            uint64_t id;
//            uint64_t value;
//            uint64_t timestamp;
//        };
//        static int calls = 0;
//        auto* records = buffer.getBuffer<Record>();
//        for (auto u = 0u; u < numberOfTuplesToProduce; ++u) {
//            records[u].id = 1;
//            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
//            records[u].value = 1;
//            records[u].timestamp = calls;
//            //            records[u].timestamp = time(0);
//        }
//        calls++;
//    };
//
//    auto lambdaSourceType =
//        LambdaSourceType::create("input1", "test_stream1", std::move(func1), 22, 11, GatheringMode::INTERVAL_MODE);
//    wrkConf->physicalSourceTypes.add(lambdaSourceType);
//    auto wrk1 = std::make_shared<NES::NesWorker>(std::move(wrkConf));
//    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
//    ASSERT_TRUE(retStart1);
//
//    std::string outputFilePath = getTestResourceFolder() / "testIngestionRateFromQuery.out";
//    remove(outputFilePath.c_str());
//    string query =
//        R"(Query::from("input1").sink(FileSinkDescriptor::create(")" + outputFilePath + R"(", "CSV_FORMAT", "APPEND"));)";
//
//    NES::RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
//    auto queryCatalog = crd->getQueryCatalog();
//    auto queryId = requestHandlerService->validateAndQueueAddQueryRequest(query, Optimizer::PlacementStrategy::BottomUp);
//
//    ASSERT_TRUE(NES::TestUtils::waitForQueryToStart(queryId, queryCatalog));
//
//    auto start = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
//    string expectedContent = "input1$id:INTEGER(64 bits),input1$value:INTEGER(64 bits),input1$timestamp:INTEGER(64 bits)\n"
//                             "1,1,0\n"
//                             "1,1,0\n"
//                             "1,1,0\n"
//                             "1,1,1\n"
//                             "1,1,1\n"
//                             "1,1,1\n"
//                             "1,1,2\n"
//                             "1,1,2\n"
//                             "1,1,2\n"
//                             "1,1,3\n"
//                             "1,1,3\n"
//                             "1,1,3\n"
//                             "1,1,4\n"
//                             "1,1,4\n"
//                             "1,1,4\n"
//                             "1,1,5\n"
//                             "1,1,5\n"
//                             "1,1,5\n"
//                             "1,1,6\n"
//                             "1,1,6\n"
//                             "1,1,6\n"
//                             "1,1,7\n"
//                             "1,1,7\n"
//                             "1,1,7\n"
//                             "1,1,8\n"
//                             "1,1,8\n"
//                             "1,1,8\n"
//                             "1,1,9\n"
//                             "1,1,9\n"
//                             "1,1,9\n"
//                             "1,1,10\n"
//                             "1,1,10\n"
//                             "1,1,10\n"
//                             "1,1,11\n"
//                             "1,1,11\n"
//                             "1,1,11\n"
//                             "1,1,12\n"
//                             "1,1,12\n"
//                             "1,1,12\n"
//                             "1,1,13\n"
//                             "1,1,13\n"
//                             "1,1,13\n"
//                             "1,1,14\n"
//                             "1,1,14\n"
//                             "1,1,14\n"
//                             "1,1,15\n"
//                             "1,1,15\n"
//                             "1,1,15\n"
//                             "1,1,16\n"
//                             "1,1,16\n"
//                             "1,1,16\n"
//                             "1,1,17\n"
//                             "1,1,17\n"
//                             "1,1,17\n"
//                             "1,1,18\n"
//                             "1,1,18\n"
//                             "1,1,18\n"
//                             "1,1,19\n"
//                             "1,1,19\n"
//                             "1,1,19\n"
//                             "1,1,20\n"
//                             "1,1,20\n"
//                             "1,1,20\n"
//                             "1,1,21\n"
//                             "1,1,21\n"
//                             "1,1,21\n";
//
//    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath, 60));
//    auto stop = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
//    NES_DEBUG("start={} stop={}", start, stop);
//
//    NES_INFO("SourceTest: Remove query");
//    requestHandlerService->validateAndQueueStopQueryRequest(queryId);
//    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));
//
//    NES_DEBUG("E2EBase: Stop worker 1");
//    bool retStopWrk1 = wrk1->stop(true);
//    ASSERT_TRUE(retStopWrk1);
//
//    NES_DEBUG("E2EBase: Stop Coordinator");
//    bool retStopCord = crd->stopCoordinator(true);
//    ASSERT_TRUE(retStopCord);
//    NES_DEBUG("E2EBase: Test finished");
//}
//
//TEST_F(SourceTest, testMonitoringSourceInitAndGetType) {
//    // create metrics and plan for MonitoringSource
//    auto metrics = std::set<Monitoring::MetricType>({Monitoring::MetricType::CpuMetric,
//                                                     Monitoring::MetricType::DiskMetric,
//                                                     Monitoring::MetricType::MemoryMetric,
//                                                     Monitoring::MetricType::NetworkMetric});
//    auto plan = Monitoring::MonitoringPlan::create(metrics);
//    auto testCollector = std::make_shared<Monitoring::DiskCollector>();
//
//    uint64_t numBuffers = 2;
//    MonitoringSourceProxy monitoringDataSource(testCollector,
//                                               MonitoringSource::DEFAULT_WAIT_TIME,
//                                               this->nodeEngine->getBufferManager(),
//                                               this->nodeEngine->getQueryManager(),
//                                               OperatorId(operatorId),
//                                               this->numSourceLocalBuffersDefault,
//                                               {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                 1,
//                                                                                 SharedQueryId(1),
//                                                                                 DecomposedQueryId(1),
//                                                                                 DecomposedQueryPlanVersion(0))});
//    ASSERT_EQ(monitoringDataSource.getType(), SourceType::MONITORING_SOURCE);
//}
//
//TEST_F(SourceTest, testMonitoringSourceReceiveDataOnce) {
//    // create metrics and plan for MonitoringSource
//    auto metrics = std::set<Monitoring::MetricType>({Monitoring::MetricType::CpuMetric,
//                                                     Monitoring::MetricType::DiskMetric,
//                                                     Monitoring::MetricType::MemoryMetric,
//                                                     Monitoring::MetricType::NetworkMetric});
//    auto plan = Monitoring::MonitoringPlan::create(metrics);
//    auto testCollector = std::make_shared<Monitoring::DiskCollector>();
//
//    uint64_t numBuffers = 2;
//    MonitoringSourceProxy monitoringDataSource(testCollector,
//                                               MonitoringSource::DEFAULT_WAIT_TIME,
//                                               this->nodeEngine->getBufferManager(),
//                                               this->nodeEngine->getQueryManager(),
//                                               OperatorId(operatorId),
//                                               this->numSourceLocalBuffersDefault,
//                                               {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                 1,
//                                                                                 SharedQueryId(1),
//                                                                                 DecomposedQueryId(1),
//                                                                                 DecomposedQueryPlanVersion(0))});
//
//    // open starts the bufferManager, otherwise receiveData will fail
//    monitoringDataSource.open();
//    auto buf = monitoringDataSource.receiveData();
//    ASSERT_TRUE(buf.has_value());
//    ASSERT_EQ(buf->getNumberOfTuples(), 1u);
//    ASSERT_EQ(monitoringDataSource.getNumberOfGeneratedTuples(), 1u);
//    ASSERT_EQ(monitoringDataSource.getNumberOfGeneratedBuffers(), 1u);
//
//    Monitoring::DiskMetrics parsedValues{};
//    parsedValues.readFromBuffer(buf.value(), 0);
//    ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), parsedValues));
//}
//
//TEST_F(SourceTest, testMonitoringSourceReceiveDataMultipleTimes) {
//    // create metrics and plan for MonitoringSource
//    auto metrics = std::set<Monitoring::MetricType>({Monitoring::MetricType::CpuMetric,
//                                                     Monitoring::MetricType::DiskMetric,
//                                                     Monitoring::MetricType::MemoryMetric,
//                                                     Monitoring::MetricType::NetworkMetric});
//    auto plan = Monitoring::MonitoringPlan::create(metrics);
//    auto testCollector = std::make_shared<Monitoring::DiskCollector>();
//
//    uint64_t numBuffers = 2;
//    MonitoringSourceProxy monitoringDataSource(testCollector,
//                                               MonitoringSource::DEFAULT_WAIT_TIME,
//                                               this->nodeEngine->getBufferManager(),
//                                               this->nodeEngine->getQueryManager(),
//                                               OperatorId(operatorId),
//                                               this->numSourceLocalBuffersDefault,
//                                               {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                 1,
//                                                                                 SharedQueryId(1),
//                                                                                 DecomposedQueryId(1),
//                                                                                 DecomposedQueryPlanVersion(0))});
//
//    // open starts the bufferManager, otherwise receiveData will fail
//    monitoringDataSource.open();
//    while (monitoringDataSource.getNumberOfGeneratedBuffers() < numBuffers) {
//        auto optBuf = monitoringDataSource.receiveData();
//        Monitoring::DiskMetrics parsedValues{};
//        parsedValues.readFromBuffer(optBuf.value(), 0);
//        ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), parsedValues));
//    }
//
//    EXPECT_EQ(monitoringDataSource.getNumberOfGeneratedBuffers(), numBuffers);
//    EXPECT_EQ(monitoringDataSource.getNumberOfGeneratedTuples(), 2UL);
//}
//
////TODO: Addressed in issue #4188
//TEST_F(SourceTest, DISABLED_testMonitoringSourceBufferSmallerThanMetric) {
//    // create metrics and plan for MonitoringSource
//    auto testCollector = std::make_shared<Monitoring::CpuCollector>();
//    auto cpuMetrics = testCollector->readMetric()->getValue<Monitoring::CpuMetricsWrapper>();
//    uint64_t numCpuMetrics = cpuMetrics.size();
//    ASSERT_TRUE(numCpuMetrics > 0);
//
//    auto schema = Monitoring::CpuMetrics::getSchema("");
//    auto bufferSize = (numCpuMetrics - 1) * schema->getSchemaSizeInBytes();
//
//    Runtime::BufferManagerPtr bufferManager = std::make_shared<Runtime::BufferManager>(bufferSize, 12);
//    auto tupleBuffer = bufferManager->getUnpooledBuffer(bufferSize).value();// MetricCollectorTest.cpp l. 80
//
//    MonitoringSourceProxy monitoringDataSource(testCollector,
//                                               MonitoringSource::DEFAULT_WAIT_TIME,
//                                               bufferManager,
//                                               this->nodeEngine->getQueryManager(),
//                                               OperatorId(operatorId),
//                                               this->numSourceLocalBuffersDefault,
//                                               {std::make_shared<NullOutputSink>(this->nodeEngine,
//                                                                                 1,
//                                                                                 SharedQueryId(1),
//                                                                                 DecomposedQueryId(1),
//                                                                                 DecomposedQueryPlanVersion(0))});
//
//    // open starts the bufferManager, otherwise receiveData will fail
//    monitoringDataSource.open();
//    auto buf = monitoringDataSource.receiveData();
//
//    Monitoring::CpuMetricsWrapper parsedValues{};
//    parsedValues.readFromBuffer(buf.value(), 0);
//    ASSERT_TRUE(MetricValidator::isValid(Monitoring::SystemResourcesReaderFactory::getSystemResourcesReader(), parsedValues));
//    ASSERT_EQ(parsedValues.size(), numCpuMetrics - 1);
//}

}// namespace NES
