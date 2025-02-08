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
#include <API/TimeUnit.hpp>
#include <BaseIntegrationTest.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Listeners/QueryStatusListener.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/Formats/CsvFormat.hpp>
#include <Sinks/Mediums/RawBufferSink.hpp>
#include <iostream>
#include <random>

namespace NES::Runtime::Execution {

auto constexpr DEFAULT_WINDOW_SIZE = 1000;
auto constexpr DEFAULT_OP_HANDLER_IDX = 0;
auto constexpr DEFAULT_LEFT_PAGE_SIZE = 1024;
auto constexpr DEFAULT_RIGHT_PAGE_SIZE = 256;

class NLJBuildPipelineExecutionContext : public PipelineExecutionContext {
  public:
    NLJBuildPipelineExecutionContext(OperatorHandlerPtr nljOperatorHandler, BufferManagerPtr bm)
        : PipelineExecutionContext(
            INVALID_PIPELINE_ID,             // mock pipeline id
            INVALID_DECOMPOSED_QUERY_PLAN_ID,// mock query id
            bm,
            1,
            [](TupleBuffer&, Runtime::WorkerContextRef) {
            },
            [](TupleBuffer&) {
            },
            {nljOperatorHandler}) {}
};

class DummyQueryListener : public AbstractQueryStatusListener {
  public:
    virtual ~DummyQueryListener() {}

    bool canTriggerEndOfStream(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
        return true;
    }
    bool notifySourceTermination(SharedQueryId, DecomposedQueryId, OperatorId, Runtime::QueryTerminationType) override {
        return true;
    }
    bool notifyQueryFailure(SharedQueryId, DecomposedQueryId, std::string) override { return true; }
    bool notifyQueryStatusChange(SharedQueryId, DecomposedQueryId, Runtime::Execution::ExecutableQueryPlanStatus) override {
        return true;
    }
    bool propagateNeighbourInformation(std::vector<std::pair<WorkerId, std::string>>) override {
        return true;
    }
    void offloadCheckpoint(uint64_t, std::vector<char>) override {
        return;
    }
    void rpcTrimCheckpoint(uint64_t, uint64_t) override {
        return;
    }
    bool requestSubQueryRemoval(SharedQueryId, DecomposedQueryId) override {
        return true;
    }
};

class ComparableNLJOperatorHandlerSlicing;
using ComparableNLJOperatorHandlerSlicingPtr = std::shared_ptr<ComparableNLJOperatorHandlerSlicing>;

/**
* @brief Wrapper to be able to get windowToSlices map from operator handler
*/
class ComparableNLJOperatorHandlerSlicing : public Operators::NLJOperatorHandlerSlicing {
  public:
    // constructor is the same as in NLJOperatorHandlerSlicing
    ComparableNLJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                                        const OriginId outputOriginId,
                                        const uint64_t windowSize,
                                        const uint64_t windowSlide,
                                        const SchemaPtr& leftSchema,
                                        const SchemaPtr& rightSchema,
                                        const uint64_t pageSizeLeft,
                                        const uint64_t pageSizeRight,
                                        Operators::TimeFunctionPtr leftTimeFunctionPtr,
                                        Operators::TimeFunctionPtr rightTimeFunctionPtr)
        : StreamJoinOperatorHandler(inputOrigins,
                                    outputOriginId,
                                    windowSize,
                                    windowSlide,
                                    leftSchema,
                                    rightSchema,
                                    std::move(leftTimeFunctionPtr),
                                    std::move(rightTimeFunctionPtr),
                                    std::map<QueryId, uint64_t>{{INVALID_QUERY_ID, DEFAULT_JOIN_DEPLOYMENT_TIME}}),
          NLJOperatorHandlerSlicing(inputOrigins,
                                    outputOriginId,
                                    windowSize,
                                    windowSlide,
                                    leftSchema,
                                    rightSchema,
                                    pageSizeLeft,
                                    pageSizeRight,
                                    std::move(leftTimeFunctionPtr),
                                    std::move(rightTimeFunctionPtr),
                                    std::map<QueryId, uint64_t>{{INVALID_QUERY_ID, DEFAULT_JOIN_DEPLOYMENT_TIME}}){

          };
    // create function is the same as in NLJOperatorHandlerSlicing
    static ComparableNLJOperatorHandlerSlicingPtr create(const std::vector<OriginId>& inputOrigins,
                                                         const OriginId outputOriginId,
                                                         const uint64_t windowSize,
                                                         const uint64_t windowSlide,
                                                         const SchemaPtr& leftSchema,
                                                         const SchemaPtr& rightSchema,
                                                         const uint64_t pageSizeLeft,
                                                         const uint64_t pageSizeRight,
                                                         Operators::TimeFunctionPtr leftTimeFunctionPtr,
                                                         Operators::TimeFunctionPtr rightTimeFunctionPtr) {
        return std::make_shared<ComparableNLJOperatorHandlerSlicing>(inputOrigins,
                                                                     outputOriginId,
                                                                     windowSize,
                                                                     windowSlide,
                                                                     leftSchema,
                                                                     rightSchema,
                                                                     pageSizeLeft,
                                                                     pageSizeRight,
                                                                     std::move(leftTimeFunctionPtr),
                                                                     std::move(rightTimeFunctionPtr));
    }

    /**
     * @brief get windowToSlices map from operator handler
     */
    std::map<WindowInfo, SlicesAndState> getWindowInfoToCompare() {
        auto windowToSlicesLocked = windowToSlices.rlock();
        return *windowToSlicesLocked;
    }
};

class NLJSliceSerializationTest : public Testing::BaseUnitTest {
  public:
    ComparableNLJOperatorHandlerSlicingPtr nljOperatorHandler;
    ComparableNLJOperatorHandlerSlicingPtr recreatedOperatorHandler;
    std::shared_ptr<Runtime::BufferManager> bm;
    Runtime::NodeEnginePtr nodeEngine{nullptr};
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
    std::string timestampFieldNameLeft;
    std::string timestampFieldNameRight;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    std::string stateFilePath;
    std::string windowInfoFilePath;
    std::string recreationFilePath;
    uint64_t windowSize = DEFAULT_WINDOW_SIZE;
    const uint64_t handlerIndex = DEFAULT_OP_HANDLER_IDX;
    const uint64_t leftPageSize = DEFAULT_LEFT_PAGE_SIZE;
    const uint64_t rightPageSize = DEFAULT_RIGHT_PAGE_SIZE;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NLJSliceSerializationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NLJSliceSerializationTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup NLJSliceSerializationTest test case.");

        leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
        rightSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test2");
        timestampFieldNameLeft = leftSchema->get(2)->getName();
        timestampFieldNameRight = rightSchema->get(2)->getName();
        stateFilePath = "test-serialization-state.bin";
        windowInfoFilePath = "test-serialization-windowInfo.bin";
        recreationFilePath = "test-recreation.bin";

        bm = std::make_shared<BufferManager>(1024, 5000);

        auto readTsFieldLeftSetup = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldNameLeft);
        auto readTsFieldRightSetup = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldNameRight);
        nljOperatorHandler = ComparableNLJOperatorHandlerSlicing::create(
            {INVALID_ORIGIN_ID},
            OriginId(1),
            windowSize,
            windowSize,
            leftSchema,
            rightSchema,
            leftPageSize,
            rightPageSize,
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeftSetup,
                                                                               Windowing::TimeUnit::Milliseconds()),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRightSetup,
                                                                               Windowing::TimeUnit::Milliseconds()));
        recreatedOperatorHandler = ComparableNLJOperatorHandlerSlicing::create(
            {INVALID_ORIGIN_ID},
            OriginId(1),
            windowSize,
            windowSize,
            leftSchema,
            rightSchema,
            leftPageSize,
            rightPageSize,
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeftSetup,
                                                                               Windowing::TimeUnit::Milliseconds()),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRightSetup,
                                                                               Windowing::TimeUnit::Milliseconds()));
        nljOperatorHandler->setBufferManager(bm);
        recreatedOperatorHandler->setBufferManager(bm);

        std::remove(stateFilePath.c_str());
        std::remove(windowInfoFilePath.c_str());
        std::remove(recreationFilePath.c_str());
        std::remove(transformFileName(stateFilePath).c_str());
        std::remove(transformFileName(windowInfoFilePath).c_str());
        std::remove(transformFileName(recreationFilePath).c_str());
    }

    /**
     * @brief completed file name
     */
    std::string transformFileName(std::string filePath) {
        auto dotPosition = filePath.find_last_of('.');
        return filePath.substr(0, dotPosition) + "-completed" + filePath.substr(dotPosition);
    }

    /**
     * @brief use raw buffer sink to write tuples to the file
     * @param state - vector of tuple buffers to sink
     */
    void sinkToTheRawBufferSink(std::vector<Runtime::TupleBuffer> state, std::string fileName) {
        // create mocked configuration for sink
        SinkFormatPtr format = std::make_shared<CsvFormat>(leftSchema, bm, false);
        auto workerConfigurations = Configurations::WorkerConfiguration::create();
        this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                               .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                               .build();
        // create sink, which is used to write buffers to the file
        auto rawBufferSink =
            RawBufferSink(nodeEngine, 1, fileName, false, INVALID_SHARED_QUERY_ID, INVALID_DECOMPOSED_QUERY_PLAN_ID);
        rawBufferSink.setup();

        // mocked context
        auto context = WorkerContext(INITIAL<WorkerThreadId>, bm, 100);
        for (auto buffer : state) {
            rawBufferSink.writeData(buffer, context);
        }
        rawBufferSink.shutdown();
    }

    /**
     * @brief add metadata info to buffer and return it
     * @param numberOfStateBuffers size of state buffers
     * @param numberOfWindowInfoBuffers size of window info buffers
     * @return new buffer with metadata info
     */
    Runtime::TupleBuffer addStateAndWindowInfoMetadata(size_t numberOfStateBuffers, size_t numberOfWindowInfoBuffers) {
        auto metadata = bm->getBufferBlocking();
        auto metadataPtr = metadata.getBuffer<uint64_t>();

        metadataPtr[0] = numberOfStateBuffers;
        metadataPtr[1] = numberOfWindowInfoBuffers;

        return metadata;
    }

    /**
     * @brief use raw buffer sink to write tuples to the file
     * @param state vector of state tuple buffers to sink
     * @param windowInfo vector of window info buffers to sink
     */
    void sinkToTheRawBufferSink(std::vector<Runtime::TupleBuffer> state,
                                std::vector<Runtime::TupleBuffer> windowInfo,
                                std::string fileName) {

        // create mocked configuration for sink
        SinkFormatPtr format = std::make_shared<CsvFormat>(leftSchema, bm, false);
        auto workerConfigurations = Configurations::WorkerConfiguration::create();
        this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                               .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                               .build();
        // create sink, which is used to write buffers to the file
        auto rawBufferSink =
            RawBufferSink(nodeEngine, 1, fileName, false, INVALID_SHARED_QUERY_ID, INVALID_DECOMPOSED_QUERY_PLAN_ID, 1);
        rawBufferSink.setup();

        auto metadata = addStateAndWindowInfoMetadata(state.size(), windowInfo.size());

        // mocked context
        auto context = WorkerContext(INITIAL<WorkerThreadId>, bm, 100);
        // 1. write state and window info metadata
        rawBufferSink.writeData(metadata, context);
        // 2. write state buffers
        for (auto buffer : state) {
            rawBufferSink.writeData(buffer, context);
        }
        // 3. write window info buffers
        for (auto buffer : windowInfo) {
            rawBufferSink.writeData(buffer, context);
        }
        rawBufferSink.shutdown();
    }

    /**
     * @brief recreate state from the file to recreated operator handler
     */
    void recreateStateFromTheFile(std::string fileName) {
        // file to read the data from
        std::ifstream deserFile(fileName, std::ios::binary | std::ios::in);

        if (!deserFile.is_open()) {
            NES_ERROR("Error: Failed to open file with the state for reading. Path is wrong.");
        }

        std::vector<TupleBuffer> recreatedBuffers = {};
        while (!deserFile.eof()) {
            // read size and number of tuples in every tuple buffer from the file
            uint64_t size = 0, numberOfTuples = 0;
            deserFile.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
            deserFile.read(reinterpret_cast<char*>(&numberOfTuples), sizeof(uint64_t));
            NES_DEBUG("size {} tuples{}", size, numberOfTuples);
            if (size > bm->getBufferSize()) {
                NES_NOT_IMPLEMENTED();
            }
            if (size > 0) {
                // 5. recreate vector of tuple buffers from the file
                auto newBuffer = bm->getUnpooledBuffer(size);
                if (newBuffer.has_value()) {
                    deserFile.read(reinterpret_cast<char*>(newBuffer.value().getBuffer()), size);
                    newBuffer.value().setNumberOfTuples(numberOfTuples);
                    NES_DEBUG("buffer size {}", newBuffer.value().getNumberOfTuples());
                    recreatedBuffers.insert(recreatedBuffers.end(), newBuffer.value());
                } else {
                    NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
                }
            }
        }

        deserFile.close();
        recreatedOperatorHandler->restoreState(recreatedBuffers);
    }

    /**
     * @brief recreate windowInfo from the file to recreated operator handler
     */
    void recreateWindowInfoFromTheFile(std::string fileName) {
        // file to read the data from
        std::ifstream deserFile(fileName, std::ios::binary | std::ios::in);

        if (!deserFile.is_open()) {
            NES_ERROR("Error: Failed to open file for reading.");
        }

        std::vector<TupleBuffer> recreatedBuffers = {};
        while (!deserFile.eof()) {
            // read size and number of tuples in evert tuple buffer from the file
            uint64_t size = 0, numberOfTuples = 0;
            deserFile.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
            deserFile.read(reinterpret_cast<char*>(&numberOfTuples), sizeof(uint64_t));
            NES_DEBUG("size {} tuples{}", size, numberOfTuples);
            if (size > bm->getBufferSize()) {
                NES_NOT_IMPLEMENTED();
            }
            if (size > 0) {
                // 5. recreate vector of tuple buffers from the file
                auto newBuffer = bm->getUnpooledBuffer(size);
                if (newBuffer.has_value()) {
                    deserFile.read(reinterpret_cast<char*>(newBuffer.value().getBuffer()), size);
                    newBuffer.value().setNumberOfTuples(numberOfTuples);
                    NES_DEBUG("buffer size {}", newBuffer.value().getNumberOfTuples());
                    recreatedBuffers.insert(recreatedBuffers.end(), newBuffer.value());
                } else {
                    NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
                }
            }
        }

        deserFile.close();

        recreatedOperatorHandler->restoreWindowInfo(recreatedBuffers);
    }

    /**
     * @brief generates left and right records, inserts into build slicing operator and runs to fill StreamJoinOperatorHandler in.
     * @param numberOfRecordsLeft
     * @param numberOfRecordsRight
     * @return max timestamp of generated records
     */
    uint64_t generateAndInsertRecordsToBuilds(uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight) {
        auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldNameLeft);
        auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldNameRight);

        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, QueryCompilation::JoinBuildSideType::Left);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, QueryCompilation::JoinBuildSideType::Right);

        auto nljBuildLeft = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            leftSchema,
            QueryCompilation::JoinBuildSideType::Left,
            leftSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft,
                                                                               Windowing::TimeUnit::Milliseconds()),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);
        auto nljBuildRight = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            rightSchema,
            QueryCompilation::JoinBuildSideType::Right,
            rightSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight,
                                                                               Windowing::TimeUnit::Milliseconds()),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);

        NLJBuildPipelineExecutionContext pipelineContext(nljOperatorHandler, bm);
        WorkerContextPtr workerContext = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bm, 100);
        auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                                 Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

        nljBuildLeft->setup(executionContext);
        nljBuildRight->setup(executionContext);
        RecordBuffer recordBuffer(Value<MemRef>((int8_t*) nullptr));
        nljBuildLeft->open(executionContext, recordBuffer);
        nljBuildRight->open(executionContext, recordBuffer);

        uint64_t maxTimestamp = 2;
        for (auto& leftRecord : allLeftRecords) {
            maxTimestamp =
                std::max(leftRecord.read(timestampFieldNameLeft).getValue().staticCast<UInt64>().getValue(), maxTimestamp);
            nljBuildLeft->execute(executionContext, leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            maxTimestamp =
                std::max(rightRecord.read(timestampFieldNameRight).getValue().staticCast<UInt64>().getValue(), maxTimestamp);
            nljBuildRight->execute(executionContext, rightRecord);
        }

        return maxTimestamp;
    }

    /**
     * @brief creates numberOfRecords many tuples for either left or right schema
     * @param numberOfRecords
     * @param joinBuildSide
     * @param minValue default is 0
     * @param maxValue default is 1000
     * @param randomSeed default is 42
     * @return the vector of records
     */
    std::vector<Record> createRandomRecords(uint64_t numberOfRecords,
                                            QueryCompilation::JoinBuildSideType joinBuildSide,
                                            uint64_t minValue = 0,
                                            uint64_t maxValue = 1000,
                                            uint64_t randomSeed = 42) {
        std::vector<Record> retVector;
        std::mt19937 generator(randomSeed);
        std::uniform_int_distribution<uint64_t> distribution(minValue, maxValue);
        auto schema = joinBuildSide == QueryCompilation::JoinBuildSideType::Left ? leftSchema : rightSchema;

        auto firstSchemaField = schema->get(0)->getName();
        auto secondSchemaField = schema->get(1)->getName();
        auto thirdSchemaField = schema->get(2)->getName();

        for (auto i = 0_u64; i < numberOfRecords; ++i) {
            retVector.emplace_back(Record({{firstSchemaField, Value<UInt64>(i)},
                                           {secondSchemaField, Value<UInt64>(distribution(generator))},
                                           {thirdSchemaField, Value<UInt64>(i)}}));
        }

        return retVector;
    }

    /**
     * @brief Compares state inside recreatedOperatorHandler and expected nljOperatorHandler
     * @param maxTimestamp of generated records to iterate over slices
     */
    void compareExpectedOperatorHandlerAndRecreatedState(uint64_t maxTimestamp) {
        // compare number of slices in both operator handlers
        NES_DEBUG("Checking number of slices = {}", recreatedOperatorHandler->getNumberOfSlices());
        EXPECT_EQ(nljOperatorHandler->getNumberOfSlices(), recreatedOperatorHandler->getNumberOfSlices());

        // go over slices and compare them
        auto maxSliceId = std::ceil((double) maxTimestamp / windowSize);
        for (uint64_t sliceIdentifier = 1; sliceIdentifier <= maxSliceId; sliceIdentifier += 1) {
            NES_DEBUG("Check slice={}", sliceIdentifier);
            NES_DEBUG("Checking number of left tuples in expected and recreated operator handler");
            EXPECT_EQ(
                nljOperatorHandler->getNumberOfTuplesInSlice(sliceIdentifier, QueryCompilation::JoinBuildSideType::Left),
                recreatedOperatorHandler->getNumberOfTuplesInSlice(sliceIdentifier, QueryCompilation::JoinBuildSideType::Left));
            NES_DEBUG("Checking number of right tuples in expected and recreated operator handler");
            EXPECT_EQ(
                nljOperatorHandler->getNumberOfTuplesInSlice(sliceIdentifier, QueryCompilation::JoinBuildSideType::Right),
                recreatedOperatorHandler->getNumberOfTuplesInSlice(sliceIdentifier, QueryCompilation::JoinBuildSideType::Right));

            // get slice by identifier from expected and recreated operator handler
            auto expectedNLJSlice =
                std::dynamic_pointer_cast<NLJSlice>(nljOperatorHandler->getSliceBySliceIdentifier(sliceIdentifier).value());
            auto recreatedNLJSlice =
                std::dynamic_pointer_cast<NLJSlice>(recreatedOperatorHandler->getSliceBySliceIdentifier(sliceIdentifier).value());

            // getting left paged vector to compare
            auto expectedLeftPagedVectorRef = Nautilus::Value<Nautilus::MemRef>(
                static_cast<int8_t*>(expectedNLJSlice->getPagedVectorRefLeft(INITIAL<WorkerThreadId>)));

            auto recreatedLeftPagedVectorRef = Nautilus::Value<Nautilus::MemRef>(
                static_cast<int8_t*>(recreatedNLJSlice->getPagedVectorRefLeft(INITIAL<WorkerThreadId>)));

            NES_DEBUG("Checking left tuples one by one");
            // compare records inside left paged vector
            checkRecordsInBuild(expectedLeftPagedVectorRef,
                                recreatedLeftPagedVectorRef,
                                expectedNLJSlice->getNumberOfTuplesLeft(),
                                leftSchema);

            NES_DEBUG("Checking right tuples one by one");
            // getting right paged vector to compare
            auto expectedRightPagedVectorRef = Nautilus::Value<Nautilus::MemRef>(
                static_cast<int8_t*>(expectedNLJSlice->getPagedVectorRefRight(INITIAL<WorkerThreadId>)));
            auto recreatedRightPagedVectorRef = Nautilus::Value<Nautilus::MemRef>(
                static_cast<int8_t*>(recreatedNLJSlice->getPagedVectorRefRight(INITIAL<WorkerThreadId>)));
            // compare records inside right paged vector
            checkRecordsInBuild(expectedRightPagedVectorRef,
                                recreatedRightPagedVectorRef,
                                expectedNLJSlice->getNumberOfTuplesRight(),
                                rightSchema);
        }
    }

    /**
     * @brief Compares window info inside recreatedOperatorHandler and expected nljOperatorHandler
     */
    void compareExpectedOperatorHandlerAndRecreatedWindowsInfo() {
        // Get recreated and expected window info to compare
        auto expectedWindowsInfo = nljOperatorHandler->getWindowInfoToCompare();
        auto recreatedWindowsInfo = recreatedOperatorHandler->getWindowInfoToCompare();

        // check that size of windows info maps is equal
        EXPECT_EQ(expectedWindowsInfo.size(), recreatedWindowsInfo.size());

        // Go over data tuple buffers with window info and compare them
        for (auto expectedIt = expectedWindowsInfo.cbegin(), recreatedIt = recreatedWindowsInfo.cbegin();
             expectedIt != expectedWindowsInfo.cend() && recreatedIt != recreatedWindowsInfo.cend();
             expectedIt++, recreatedIt++) {
            // check that window info is equal
            EXPECT_EQ(expectedIt->first.windowStart, recreatedIt->first.windowStart);
            EXPECT_EQ(expectedIt->first.windowEnd, recreatedIt->first.windowEnd);
            EXPECT_EQ(expectedIt->first.windowId, recreatedIt->first.windowId);

            // check window status and slices
            EXPECT_EQ(expectedIt->second.windowState, recreatedIt->second.windowState);
            EXPECT_EQ(expectedIt->second.slices.size(), recreatedIt->second.slices.size());

            for (auto expectedSliceIt = expectedIt->second.slices.cbegin(),
                      recreatedSliceIt = recreatedIt->second.slices.cbegin();
                 expectedSliceIt != expectedIt->second.slices.cend() && recreatedSliceIt != recreatedIt->second.slices.cend();
                 expectedSliceIt++, recreatedSliceIt++) {
                EXPECT_EQ(expectedSliceIt->get()->getSliceIdentifier(), recreatedSliceIt->get()->getSliceIdentifier());
                EXPECT_EQ(expectedSliceIt->get()->getSliceStart(), recreatedSliceIt->get()->getSliceStart());
                EXPECT_EQ(expectedSliceIt->get()->getSliceEnd(), recreatedSliceIt->get()->getSliceEnd());
                EXPECT_EQ(expectedSliceIt->get()->getNumberOfTuplesLeft(), recreatedSliceIt->get()->getNumberOfTuplesLeft());
                EXPECT_EQ(expectedSliceIt->get()->getNumberOfTuplesRight(), recreatedSliceIt->get()->getNumberOfTuplesRight());
            }
        }
    }

    /**
     * @brief Compares state inside expected and recreated paged vector
     * @param expectedPagedVectorRef - expected content of paged vector
     * @param recreatedPagedVectorRef - recreated content
     * @param numberOfTuples to check
     * @param schema of tuples
     */
    void checkRecordsInBuild(Value<MemRef>& expectedPagedVectorRef,
                             Value<MemRef>& recreatedPagedVectorRef,
                             uint64_t numberOfTuples,
                             SchemaPtr& schema) {

        Nautilus::Interface::PagedVectorVarSizedRef expectedPagedVector(expectedPagedVectorRef, schema);
        Nautilus::Interface::PagedVectorVarSizedRef recreatedPagedVector(recreatedPagedVectorRef, schema);

        // check records one by one
        for (uint64_t pos = 0; pos < numberOfTuples; ++pos) {
            auto record = *(recreatedPagedVector.at(pos));
            auto expectedRecord = *expectedPagedVector.at(pos);
            NES_DEBUG("readRecord {} record{}", record.toString(), expectedRecord.toString());

            for (auto& field : schema->fields) {
                EXPECT_EQ(record.read(field->getName()), expectedRecord.read(field->getName()));
            }
        }
    }
};

/**
 * Test behaviour of serialization and deserialization methods of StreamJoinOperatorHandler and NLJSlice, by getting, writing to the file, reading and deserializing the state.
 */
TEST_F(NLJSliceSerializationTest, nljSliceSerializationAndDeserialization) {
    const auto numberOfRecordsLeft = 5000;
    const auto numberOfRecordsRight = 5000;

    // 1. generate and insert records to the operator handler
    auto maxTimestamp = generateAndInsertRecordsToBuilds(numberOfRecordsLeft, numberOfRecordsRight);

    // 2. get the state from old operator handler
    auto dataToMigrate = nljOperatorHandler->getStateToMigrate(0, 5000);

    // 3. write the state to the file using raWBufferSink
    sinkToTheRawBufferSink(dataToMigrate, stateFilePath);

    // 4. recreate state from the file
    recreateStateFromTheFile(transformFileName(stateFilePath));

    // 5. compare state inside old and new operator handler
    compareExpectedOperatorHandlerAndRecreatedState(maxTimestamp);
}

/**
 * Test recreation function in stream join operator handler
 */
TEST_F(NLJSliceSerializationTest, nljSliceSerializationAndRecreationFromFile) {
    const auto numberOfRecordsLeft = 5000;
    const auto numberOfRecordsRight = 5000;

    // 1. generate and insert records to the operator handler
    auto maxTimestamp = generateAndInsertRecordsToBuilds(numberOfRecordsLeft, numberOfRecordsRight);

    // 2. get the state from old operator handler
    auto dataToMigrate = nljOperatorHandler->getStateToMigrate(0, 5000);

    // 3. write the state to the file using rawBufferSink
    sinkToTheRawBufferSink(dataToMigrate, stateFilePath);

    // 4. create stream
    std::ifstream file(transformFileName(stateFilePath), std::ios::binary | std::ios::in);

    if (!file.is_open()) {
        NES_ERROR("Error: Failed to open file with the state for reading. Path is wrong.");
    }

    // 5. recreate state from the file
    recreatedOperatorHandler->restoreStateFromFile(file);

    // 6. compare state inside old and new operator handler
    compareExpectedOperatorHandlerAndRecreatedState(maxTimestamp);
}

/**
 * Test serialization of windows information
 */
TEST_F(NLJSliceSerializationTest, nljWindowsSerialization) {
    const auto numberOfRecordsLeft = 10000;
    const auto numberOfRecordsRight = 10000;

    // 1. Generate and insert records to the operator handler
    auto maxTs = generateAndInsertRecordsToBuilds(numberOfRecordsLeft, numberOfRecordsRight);

    // migrate the state
    auto dataToMigrate = nljOperatorHandler->getStateToMigrate(0, maxTs);
    sinkToTheRawBufferSink(dataToMigrate, stateFilePath);
    std::ifstream file(transformFileName(stateFilePath), std::ios::binary | std::ios::in);
    if (!file.is_open()) {
        NES_ERROR("Error: Failed to open file with the state for reading. Path is wrong.");
    }
    recreatedOperatorHandler->restoreStateFromFile(file);

    // 2. Get windows info from old operator handler
    auto windowsToMigrate = nljOperatorHandler->getWindowInfoToMigrate();

    // 3. Write windows info to the file using rawBufferSink
    sinkToTheRawBufferSink(windowsToMigrate, windowInfoFilePath);

    // 4. Recreate window info from the file in operator handler
    recreateWindowInfoFromTheFile(transformFileName(windowInfoFilePath));

    // 5. Compare old and recreated operator handlers
    compareExpectedOperatorHandlerAndRecreatedWindowsInfo();
}

/**
 * Test operator handler recreation from the file
 */
TEST_F(NLJSliceSerializationTest, nljFileRecreation) {
    const auto numberOfRecordsLeft = 10000;
    const auto numberOfRecordsRight = 10000;

    // 1. Generate and insert records to the operator handler
    auto maxTs = generateAndInsertRecordsToBuilds(numberOfRecordsLeft, numberOfRecordsRight);

    // 2. Get state and windows info from old operator handler
    auto dataToMigrate = nljOperatorHandler->getStateToMigrate(0, maxTs);
    auto windowsToMigrate = nljOperatorHandler->getWindowInfoToMigrate();

    // 3. Write buffers to the file using rawBufferSink in other thread
    std::thread thread([this,
                        dataToMigrate = dataToMigrate,
                        windowsToMigrate = windowsToMigrate,
                        recreationFilePath = recreationFilePath]() mutable {
        sinkToTheRawBufferSink(dataToMigrate, windowsToMigrate, recreationFilePath);
    });
    thread.detach();

    // 4. Start recreation earlier to check waiting on the new operator handler
    recreatedOperatorHandler->setRecreationFileName(transformFileName(recreationFilePath));
    recreatedOperatorHandler->recreateOperatorHandlerFromFile();

    // 5. Compare state and window info of old and recreated operator handler
    compareExpectedOperatorHandlerAndRecreatedState(maxTs);
    compareExpectedOperatorHandlerAndRecreatedWindowsInfo();
}
}// namespace NES::Runtime::Execution
