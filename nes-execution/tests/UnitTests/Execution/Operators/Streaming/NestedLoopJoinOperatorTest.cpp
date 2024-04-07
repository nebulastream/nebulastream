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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJBuildSlicing.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Common.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <random>
#include <API/TestSchemas.hpp>

namespace NES::Runtime::Execution {

auto constexpr DEFAULT_WINDOW_SIZE = 1000;
auto constexpr DEFAULT_OP_HANDLER_IDX = 0;
auto constexpr DEFAULT_LEFT_PAGE_SIZE = 1024;
auto constexpr DEFAULT_RIGHT_PAGE_SIZE = 256;

class NLJBuildPipelineExecutionContext : public PipelineExecutionContext {
  public:
    NLJBuildPipelineExecutionContext(OperatorHandlerPtr nljOperatorHandler, BufferManagerPtr bm)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            bm,
            1,
            [](TupleBuffer&, Runtime::WorkerContextRef) {
            },
            [](TupleBuffer&) {
            },
            {nljOperatorHandler}) {}
};

class NLJProbePipelineExecutionContext : public PipelineExecutionContext {
  public:
    std::vector<TupleBuffer> emittedBuffers;
    NLJProbePipelineExecutionContext(OperatorHandlerPtr nljOperatorHandler, BufferManagerPtr bm)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            bm,
            1,
            [](TupleBuffer&, Runtime::WorkerContextRef) {
                //                emittedBuffers.emplace_back(std::move(buffer));
            },
            [](TupleBuffer&) {
                //                emittedBuffers.emplace_back(std::move(buffer));
            },
            {nljOperatorHandler}) {}
};

class NestedLoopJoinOperatorTest : public Testing::BaseUnitTest {
  public:
    Operators::NLJOperatorHandlerPtr nljOperatorHandler;
    std::shared_ptr<Runtime::BufferManager> bm;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    std::string joinFieldNameLeft;
    std::string joinFieldNameRight;
    std::string timestampFieldNameLeft;
    std::string timestampFieldNameRight;
    uint64_t windowSize = DEFAULT_WINDOW_SIZE;
    const uint64_t handlerIndex = DEFAULT_OP_HANDLER_IDX;
    const uint64_t leftPageSize = DEFAULT_LEFT_PAGE_SIZE;
    const uint64_t rightPageSize = DEFAULT_RIGHT_PAGE_SIZE;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NestedLoopJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NestedLoopJoinOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup NestedLoopJoinOperatorTest test case.");

        leftSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")->updateSourceName("test1");
        rightSchema = TestSchemas::getSchemaTemplate("id_val_time_u64")
                          ->updateSourceName("test2");

        joinFieldNameLeft = leftSchema->get(1)->getName();
        joinFieldNameRight = rightSchema->get(1)->getName();
        timestampFieldNameLeft = leftSchema->get(2)->getName();
        timestampFieldNameRight = rightSchema->get(2)->getName();

        nljOperatorHandler = Operators::NLJOperatorHandlerSlicing::create({0},
                                                                          1,
                                                                          windowSize,
                                                                          windowSize,
                                                                          leftSchema,
                                                                          rightSchema,
                                                                          leftPageSize,
                                                                          rightPageSize);
        bm = std::make_shared<BufferManager>(8196, 5000);
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
     * @brief this method returns the number of tuples in the given window, presuming that all windows
     * except for the last one contains windowSize many tuples
     * @param totalTuples
     * @param windowIdentifier
     * @return
     */
    uint64_t calculateExpNoTuplesInWindow(uint64_t totalTuples, uint64_t windowIdentifier) {
        std::map<uint64_t, uint64_t> windowIdToTuples;
        auto curWindowId = windowSize;
        while (totalTuples > windowSize) {
            windowIdToTuples[curWindowId] = windowSize;
            totalTuples -= windowSize;
            curWindowId += windowSize;
        }
        windowIdToTuples[curWindowId] = totalTuples;
        return windowIdToTuples[windowIdentifier];
    }

    /**
     * @brief checks for every record in the given window that each field entry equals the corresponding entry in allRecords
     * @param windowIdentifier
     * @param pagedVectorRef
     * @param entrySize
     * @param expectedNumberOfTuplesInWindow
     * @param allRecords
     * @param memoryProvider
     * @param schema
     */
    void checkRecordsInBuild(uint64_t windowIdentifier,
                             Value<MemRef>& pagedVectorRef,
                             uint64_t expectedNumberOfTuplesInWindow,
                             std::vector<Record>& allRecords,
                             SchemaPtr& schema) {

        Nautilus::Value<UInt64> zeroVal(0_u64);
        Nautilus::Interface::PagedVectorVarSizedRef pagedVector(pagedVectorRef, schema);
        auto windowStartPos = windowIdentifier - windowSize;
        auto windowEndPos = windowStartPos + expectedNumberOfTuplesInWindow;
        uint64_t posInWindow = 0;

        for (auto pos = windowStartPos; pos < windowEndPos; ++pos, ++posInWindow) {
            auto record = *pagedVector.at(pos);
            auto& expectedRecord = allRecords[pos];
            NES_TRACE("readRecord {} record{}", record.toString(), expectedRecord.toString());

            for (auto& field : schema->fields) {
                EXPECT_EQ(record.read(field->getName()), expectedRecord.read(field->getName()));
            }
        }
    }

    /**
     * @brief checks for every window that it contains the expected number of tuples and calls checkRecordsInBuild()
     * @param maxTimestamp
     * @param allLeftRecords
     * @param allRightRecords
     */
    void checkWindowsInBuild(uint64_t maxTimestamp, std::vector<Record>& allLeftRecords, std::vector<Record>& allRightRecords) {
        auto numberOfRecordsLeft = allLeftRecords.size();
        auto numberOfRecordsRight = allRightRecords.size();

        auto memoryProviderLeft =
            MemoryProvider::MemoryProvider::createMemoryProvider(leftSchema->getSchemaSizeInBytes(), leftSchema);
        auto memoryProviderRight =
            MemoryProvider::MemoryProvider::createMemoryProvider(rightSchema->getSchemaSizeInBytes(), rightSchema);

        auto maxWindowIdentifier = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);
            auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(numberOfRecordsRight, windowIdentifier);

            NES_DEBUG("Check window={}", windowIdentifier);
            ASSERT_EQ(nljOperatorHandler->getNumberOfTuplesInSlice(windowIdentifier, QueryCompilation::JoinBuildSideType::Left),
                      expectedNumberOfTuplesInWindowLeft);
            ASSERT_EQ(nljOperatorHandler->getNumberOfTuplesInSlice(windowIdentifier, QueryCompilation::JoinBuildSideType::Right),
                      expectedNumberOfTuplesInWindowRight);

            auto nljWindow =
                std::dynamic_pointer_cast<NLJSlice>(nljOperatorHandler->getSliceBySliceIdentifier(windowIdentifier).value());
            Nautilus::Value<UInt64> zeroVal((uint64_t) 0);

            auto leftPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefLeft(/*workerId*/ 0));
            checkRecordsInBuild(windowIdentifier,
                                leftPagedVectorRef,
                                expectedNumberOfTuplesInWindowLeft,
                                allLeftRecords,
                                leftSchema);

            auto rightPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefRight(/*workerId*/ 0));
            checkRecordsInBuild(windowIdentifier,
                                rightPagedVectorRef,
                                expectedNumberOfTuplesInWindowRight,
                                allRightRecords,
                                rightSchema);
        }
    }

    /**
     * @brief sets up and opens NLJBuild for left and right side, executes it for every record and calls checkWindowsInBuild()
     * @param numberOfRecordsLeft
     * @param numberOfRecordsRight
     */
    void insertRecordsIntoBuild(uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight) {
        auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldNameLeft);
        auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldNameRight);

        auto nljBuildLeft = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            leftSchema,
            joinFieldNameLeft,
            QueryCompilation::JoinBuildSideType::Left,
            leftSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
            QueryCompilation::WindowingStrategy::SLICING);
        auto nljBuildRight = std::make_shared<Operators::NLJBuildSlicing>(
            handlerIndex,
            rightSchema,
            joinFieldNameRight,
            QueryCompilation::JoinBuildSideType::Right,
            rightSchema->getSchemaSizeInBytes(),
            std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight),
            QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
            QueryCompilation::WindowingStrategy::SLICING);

        NLJBuildPipelineExecutionContext pipelineContext(nljOperatorHandler, bm);
        WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
        auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                                 Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

        nljBuildLeft->setup(executionContext);
        nljBuildRight->setup(executionContext);

        // We do not care for the record buffer in the current NLJBuild::open() implementation
        RecordBuffer recordBuffer(Value<MemRef>((int8_t*) nullptr));
        nljBuildLeft->open(executionContext, recordBuffer);
        nljBuildRight->open(executionContext, recordBuffer);

        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, QueryCompilation::JoinBuildSideType::Left);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, QueryCompilation::JoinBuildSideType::Right);
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

        checkWindowsInBuild(maxTimestamp, allLeftRecords, allRightRecords);
    }

    /**
     * @brief checks for every record in the given window that each joined record is in the emitted records
     * @param allLeftRecords
     * @param allRightRecords
     * @param windowIdentifier
     * @param joinSchema
     * @param collector
     */
    void checkRecordsInProbe(std::vector<Record>& allLeftRecords,
                             std::vector<Record>& allRightRecords,
                             uint64_t windowIdentifier,
                             const SchemaPtr& joinSchema,
                             Operators::CollectOperatorPtr& collector) {

        NES_DEBUG("Checking records in Probe now for left {} right {}...", allLeftRecords.size(), allRightRecords.size());
        for (auto& leftRecord : allLeftRecords) {
            for (auto& rightRecord : allRightRecords) {

                auto windowStart = windowIdentifier - windowSize;
                auto windowEnd = windowIdentifier;
                auto leftKey = leftRecord.read(joinFieldNameLeft);
                auto rightKey = rightRecord.read(joinFieldNameRight);
                auto timestampLeftVal = leftRecord.read(timestampFieldNameLeft).getValue().staticCast<UInt64>().getValue();
                auto timestampRightVal = rightRecord.read(timestampFieldNameRight).getValue().staticCast<UInt64>().getValue();

                if (windowStart <= timestampLeftVal && timestampLeftVal < windowEnd && windowStart <= timestampRightVal
                    && timestampRightVal < windowEnd && leftKey == rightKey) {
                    Record joinedRecord;
                    Nautilus::Value<Any> windowStartVal(windowStart);
                    Nautilus::Value<Any> windowEndVal(windowEnd);
                    joinedRecord.write(joinSchema->get(0)->getName(), windowStartVal);
                    joinedRecord.write(joinSchema->get(1)->getName(), windowEndVal);
                    joinedRecord.write(joinSchema->get(2)->getName(), leftRecord.read(joinFieldNameLeft));

                    // Writing the leftSchema fields
                    for (auto& field : leftSchema->fields) {
                        joinedRecord.write(field->getName(), leftRecord.read(field->getName()));
                    }

                    // Writing the rightSchema fields
                    for (auto& field : rightSchema->fields) {
                        joinedRecord.write(field->getName(), rightRecord.read(field->getName()));
                    }

                    // Check if this joinedRecord is in the emitted records
                    auto it = std::find(collector->records.begin(), collector->records.end(), joinedRecord);
                    if (it == collector->records.end()) {
                        NES_ERROR("Could not find joinedRecord {} in the emitted records!", joinedRecord.toString());
                        ASSERT_TRUE(false);
                    }
                    collector->records.erase(it);
                    if (collector->records.empty()) {
                        NES_DEBUG("No more records in the collector!!!");
                    }
                }
            }
        }
    }

    /**
     * @brief checks for every window that it contains the expected number of tuples and calls checkRecordsInProbe()
     * @param maxTimestamp
     * @param allLeftRecords
     * @param allRightRecords
     */
    void checkWindowsInProbe(uint64_t maxTimestamp, std::vector<Record>& allLeftRecords, std::vector<Record>& allRightRecords) {
        Operators::JoinSchema joinSchema(leftSchema,
                                         rightSchema,
                                         Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft));
        Operators::WindowMetaData windowMetaData(joinSchema.joinSchema->get(0)->getName(),
                                                 joinSchema.joinSchema->get(1)->getName(),
                                                 joinSchema.joinSchema->get(2)->getName());

        auto nljProbe = std::make_shared<Operators::NLJProbe>(handlerIndex,
                                                              joinSchema,
                                                              joinFieldNameLeft,
                                                              joinFieldNameRight,
                                                              windowMetaData,
                                                              leftSchema,
                                                              rightSchema,
                                                              QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN,
                                                              QueryCompilation::WindowingStrategy::SLICING);

        NLJProbePipelineExecutionContext pipelineContext(nljOperatorHandler, bm);
        WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
        auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                                 Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

        auto collector = std::make_shared<Operators::CollectOperator>();
        nljProbe->setChild(collector);

        auto numberOfRecordsLeft = allLeftRecords.size();
        auto numberOfRecordsRight = allRightRecords.size();

        uint64_t maxWindowIdentifier = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize; windowIdentifier <= maxWindowIdentifier; windowIdentifier += windowSize) {
            auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);
            auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(numberOfRecordsRight, windowIdentifier);

            auto nljOpHandler = std::dynamic_pointer_cast<Operators::NLJOperatorHandlerSlicing>(nljOperatorHandler);
            NES_DEBUG("Check window={}", windowIdentifier);
            ASSERT_EQ(nljOpHandler->getNumberOfTuplesInSlice(windowIdentifier, QueryCompilation::JoinBuildSideType::Left),
                      expectedNumberOfTuplesInWindowLeft);
            ASSERT_EQ(nljOpHandler->getNumberOfTuplesInSlice(windowIdentifier, QueryCompilation::JoinBuildSideType::Right),
                      expectedNumberOfTuplesInWindowRight);

            {
                auto tupleBuffer = bm->getBufferBlocking();
                auto bufferMemory = tupleBuffer.getBuffer<Operators::EmittedNLJWindowTriggerTask>();
                bufferMemory->leftSliceIdentifier = windowIdentifier;
                bufferMemory->rightSliceIdentifier = windowIdentifier;
                bufferMemory->windowInfo = WindowInfo(windowIdentifier - windowSize, windowIdentifier);
                tupleBuffer.setNumberOfTuples(1);

                RecordBuffer recordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
                nljProbe->open(executionContext, recordBuffer);
            }

            checkRecordsInProbe(allLeftRecords, allRightRecords, windowIdentifier, joinSchema.joinSchema, collector);
        }
    }

    /**
     * @brief writes every record to the corresponding window's PagedVector and calls checkWindowsInProbe()
     * @param numberOfRecordsLeft
     * @param numberOfRecordsRight
     */
    void insertRecordsIntoProbe(uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight) {
        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, QueryCompilation::JoinBuildSideType::Left);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, QueryCompilation::JoinBuildSideType::Right);

        auto memoryProviderLeft = MemoryProvider::MemoryProvider::createMemoryProvider(bm->getBufferSize(), leftSchema);
        auto memoryProviderRight = MemoryProvider::MemoryProvider::createMemoryProvider(bm->getBufferSize(), rightSchema);
        nljOperatorHandler->setBufferManager(bm);

        uint64_t maxTimestamp = 0;
        Value<UInt64> zeroVal(0_u64);
        for (auto& leftRecord : allLeftRecords) {
            auto timestamp = leftRecord.read(timestampFieldNameLeft).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);

            auto nljOpHandler = std::dynamic_pointer_cast<Operators::NLJOperatorHandlerSlicing>(nljOperatorHandler);
            auto nljWindow = std::dynamic_pointer_cast<NLJSlice>(nljOpHandler->getSliceByTimestampOrCreateIt(timestamp));
            auto leftPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefLeft(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorVarSizedRef leftPagedVector(leftPagedVectorRef, leftSchema);
            leftPagedVector.writeRecord(leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            auto timestamp = rightRecord.read(timestampFieldNameRight).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);

            auto nljOpHandler = std::dynamic_pointer_cast<Operators::NLJOperatorHandlerSlicing>(nljOperatorHandler);
            auto nljWindow = std::dynamic_pointer_cast<NLJSlice>(nljOpHandler->getSliceByTimestampOrCreateIt(timestamp));
            auto rightPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefRight(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorVarSizedRef rightPagedVector(rightPagedVectorRef, rightSchema);
            rightPagedVector.writeRecord(rightRecord);
        }

        NES_INFO("left: {} right: {}", allLeftRecords.size(), allRightRecords.size());
        checkWindowsInProbe(maxTimestamp, allLeftRecords, allRightRecords);
    }
};

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestOneRecord) {
    auto numberOfRecordsLeft = 1;
    auto numberOfRecordsRight = 1;

    insertRecordsIntoBuild(numberOfRecordsLeft, numberOfRecordsRight);
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleRecords) {
    auto numberOfRecordsLeft = 250;
    auto numberOfRecordsRight = 250;

    insertRecordsIntoBuild(numberOfRecordsLeft, numberOfRecordsRight);
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleWindows) {
    auto numberOfRecordsLeft = 2000;
    auto numberOfRecordsRight = 2000;

    insertRecordsIntoBuild(numberOfRecordsLeft, numberOfRecordsRight);
}

TEST_F(NestedLoopJoinOperatorTest, joinProbeSimpleTestOneWindow) {
    const auto numberOfRecordsLeft = 250;
    const auto numberOfRecordsRight = 250;

    insertRecordsIntoProbe(numberOfRecordsLeft, numberOfRecordsRight);
}

TEST_F(NestedLoopJoinOperatorTest, joinProbeSimpleTestMultipleWindows) {
    auto numberOfRecordsLeft = 200;
    auto numberOfRecordsRight = 200;
    windowSize = 10;
    nljOperatorHandler = Operators::NLJOperatorHandlerSlicing::create({0},
                                                                      1,
                                                                      windowSize,
                                                                      windowSize,
                                                                      leftSchema,
                                                                      rightSchema,
                                                                      leftPageSize,
                                                                      rightPageSize);

    insertRecordsIntoProbe(numberOfRecordsLeft, numberOfRecordsRight);
}

}// namespace NES::Runtime::Execution