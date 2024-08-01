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
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Listeners/QueryStatusListener.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
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

    bool canTriggerEndOfStream(SharedQueryId, DecomposedQueryPlanId, OperatorId, Runtime::QueryTerminationType) override {
        return true;
    }
    bool notifySourceTermination(SharedQueryId, DecomposedQueryPlanId, OperatorId, Runtime::QueryTerminationType) override {
        return true;
    }
    bool notifyQueryFailure(SharedQueryId, DecomposedQueryPlanId, std::string) override { return true; }
    bool notifyQueryStatusChange(SharedQueryId, DecomposedQueryPlanId, Runtime::Execution::ExecutableQueryPlanStatus) override {
        return true;
    }
    bool notifyEpochTermination(uint64_t, uint64_t) override { return false; }
};

class NLJSliceSerializationTest : public Testing::BaseUnitTest {
  public:
    Operators::NLJOperatorHandlerPtr nljOperatorHandler;
    std::shared_ptr<Runtime::BufferManager> bm;
    Runtime::NodeEnginePtr nodeEngine{nullptr};
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
    std::string timestampFieldNameLeft;
    std::string timestampFieldNameRight;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    std::string filePath;
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
        filePath = "test-serialization.bin";

        bm = std::make_shared<BufferManager>(1024, 5000);
        nljOperatorHandler = Operators::NLJOperatorHandlerSlicing::create({INVALID_ORIGIN_ID},
                                                                          OriginId(1),
                                                                          windowSize,
                                                                          windowSize,
                                                                          leftSchema,
                                                                          rightSchema,
                                                                          leftPageSize,
                                                                          rightPageSize);
        nljOperatorHandler->setBufferManager(bm);
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
            joinFieldNameLeft,
            QueryCompilation::JoinBuildSideType::Left,
            leftSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft,
                                                                               Windowing::TimeUnit::Milliseconds()),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);
        auto nljBuildRight = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            rightSchema,
            joinFieldNameRight,
            QueryCompilation::JoinBuildSideType::Right,
            rightSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight,
                                                                               Windowing::TimeUnit::Milliseconds()),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN);

        NLJBuildPipelineExecutionContext pipelineContext(nljOperatorHandler, bm);
        WorkerContextPtr workerContext = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bm, 100);
        auto executionContext = ExecutionContext(Nautilus::MemRef((int8_t*) workerContext.get()),
                                                 Nautilus::MemRef((int8_t*) (&pipelineContext)));

        nljBuildLeft->setup(executionContext);
        nljBuildRight->setup(executionContext);
        RecordBuffer recordBuffer(MemRef((int8_t*) nullptr));
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
            retVector.emplace_back(Record({{firstSchemaField, UInt64(i)},
                                           {secondSchemaField, UInt64(distribution(generator))},
                                           {thirdSchemaField, UInt64(i)}}));
        }

        return retVector;
    }

    /**
     * @brief Compares state inside recreatedOperatorHandler and expected nljOperatorHandler
     * @param recreatedOperatorHandler - recreated NLJOperatorHandler
     * @param maxTimestamp of generated records to iterate over slices
     */
    void compareExpectedOperatorHandlerWith(std::shared_ptr<Operators::NLJOperatorHandler> recreatedOperatorHandler,
                                            uint64_t maxTimestamp) {
        // compare number of slices in both operator handlers
        NES_DEBUG("Checking number of slices = {}", recreatedOperatorHandler->getNumberOfSlices());
        EXPECT_EQ(nljOperatorHandler->getNumberOfSlices(), recreatedOperatorHandler->getNumberOfSlices());

        // go over slices and compare them
        auto maxSliceId = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto sliceIdentifier = 1000; sliceIdentifier <= maxSliceId; sliceIdentifier += windowSize) {
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
            auto expectedLeftPagedVectorRef = Nautilus::MemRef(
                static_cast<int8_t*>(expectedNLJSlice->getPagedVectorRefLeft(INITIAL<WorkerThreadId>)));

            auto recreatedLeftPagedVectorRef = Nautilus::MemRef(
                static_cast<int8_t*>(recreatedNLJSlice->getPagedVectorRefLeft(INITIAL<WorkerThreadId>)));

            NES_DEBUG("Checking left tuples one by one");
            // compare records inside left paged vector
            checkRecordsInBuild(expectedLeftPagedVectorRef,
                                recreatedLeftPagedVectorRef,
                                expectedNLJSlice->getNumberOfTuplesLeft(),
                                leftSchema);

            NES_DEBUG("Checking right tuples one by one");
            // getting right paged vector to compare
            auto expectedRightPagedVectorRef = Nautilus::MemRef(
                static_cast<int8_t*>(expectedNLJSlice->getPagedVectorRefRight(INITIAL<WorkerThreadId>)));
            auto recreatedRightPagedVectorRef = Nautilus::MemRef(
                static_cast<int8_t*>(recreatedNLJSlice->getPagedVectorRefRight(INITIAL<WorkerThreadId>)));
            // compare records inside right paged vector
            checkRecordsInBuild(expectedRightPagedVectorRef,
                                recreatedRightPagedVectorRef,
                                expectedNLJSlice->getNumberOfTuplesRight(),
                                rightSchema);
        }
    }

    /**
     * @brief Compares state inside expected and recreated paged vector
     * @param expectedPagedVectorRef - expected content of paged vector
     * @param recreatedPagedVectorRef - recreated content
     * @param numberOfTuples to check
     * @param schema of tuples
     */
    void checkRecordsInBuild(MemRef& expectedPagedVectorRef,
                             MemRef& recreatedPagedVectorRef,
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
TEST_F(NLJSliceSerializationTest, nljSliceSerialization) {
    const auto numberOfRecordsLeft = 5000;
    const auto numberOfRecordsRight = 5000;

    // 1. generate and insert records to the operator handler
    auto maxTimestamp = generateAndInsertRecordsToBuilds(numberOfRecordsLeft, numberOfRecordsRight);

    // create mocked configuration for sink
    SinkFormatPtr format = std::make_shared<CsvFormat>(leftSchema, bm, false);
    auto workerConfigurations = Configurations::WorkerConfiguration::create();
    this->nodeEngine = Runtime::NodeEngineBuilder::create(workerConfigurations)
                           .setQueryStatusListener(std::make_shared<DummyQueryListener>())
                           .build();
    // create sink, which is used to write buffers to the file
    auto fileSink = RawBufferSink(nodeEngine, 1, filePath, false, INVALID_SHARED_QUERY_ID, INVALID_DECOMPOSED_QUERY_PLAN_ID, 1);
    fileSink.setup();

    // 2. get the state from old operator handler
    auto dataToMigrate = nljOperatorHandler->getStateToMigrate(0, 5000);

    // mocked context
    auto context = WorkerContext(INITIAL<WorkerThreadId>, bm, 100);
    // 3. write the state to the file using sink
    for (auto buffer : dataToMigrate) {
        fileSink.writeData(buffer, context);
    }

    // file to read the data from
    std::ifstream deser_file(filePath, std::ios::binary | std::ios::in);

    if (!deser_file.is_open()) {
        NES_ERROR("Error: Failed to open file for reading.");
    }

    // 4. create new operator handler to recreate the state in
    auto newNLJOperatorHandler = Operators::NLJOperatorHandlerSlicing::create({INVALID_ORIGIN_ID},
                                                                              OriginId(1),
                                                                              windowSize,
                                                                              windowSize,
                                                                              leftSchema,
                                                                              rightSchema,
                                                                              leftPageSize,
                                                                              rightPageSize);

    newNLJOperatorHandler->setBufferManager(bm);

    std::vector<TupleBuffer> recreatedBuffers = {};
    while (!deser_file.eof()) {
        // read size and number of tuples in evert tuple buffer from the file
        uint64_t size = 0, numberOfTuples = 0;
        deser_file.read(reinterpret_cast<char*>(&size), sizeof(uint64_t));
        deser_file.read(reinterpret_cast<char*>(&numberOfTuples), sizeof(uint64_t));
        NES_DEBUG("size {} tuples{}", size, numberOfTuples);
        if (size > bm->getBufferSize()) {
            NES_NOT_IMPLEMENTED();
        }
        if (size > 0) {
            // 5. recreate vector of tuple buffers from the file
            auto newBuffer = bm->getUnpooledBuffer(size);
            if (newBuffer.has_value()) {
                deser_file.read(reinterpret_cast<char*>(newBuffer.value().getBuffer()), size);
                newBuffer.value().setNumberOfTuples(numberOfTuples);
                NES_DEBUG("buffer size {}", newBuffer.value().getNumberOfTuples());
                recreatedBuffers.insert(recreatedBuffers.end(), newBuffer.value());
            } else {
                NES_THROW_RUNTIME_ERROR("No unpooled TupleBuffer available!");
            }
        }
    }

    deser_file.close();

    // 6. restore the state inside new operator handler
    newNLJOperatorHandler->restoreState(recreatedBuffers);

    // 7. compare state inside old and new operator handler
    compareExpectedOperatorHandlerWith(newNLJOperatorHandler, maxTimestamp);
}
}// namespace NES::Runtime::Execution
