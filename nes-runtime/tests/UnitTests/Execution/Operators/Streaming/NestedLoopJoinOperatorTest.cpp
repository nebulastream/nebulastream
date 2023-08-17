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
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/DataStructure/NLJWindow.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJProbe.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/RecordCollectOperator.hpp>
#include <Util/Common.hpp>
#include <random>

namespace NES::Runtime::Execution {

class NLJBuildPiplineExecutionContext : public PipelineExecutionContext {
  public:
    NLJBuildPiplineExecutionContext(OperatorHandlerPtr nljOperatorHandler)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            1,
            [](TupleBuffer&, Runtime::WorkerContextRef) {
            },
            [](TupleBuffer&) {
            },
            {nljOperatorHandler}) {}
};

class NLJProbePiplineExecutionContext : public PipelineExecutionContext {
  public:
    std::vector<TupleBuffer> emittedBuffers;
    NLJProbePiplineExecutionContext(OperatorHandlerPtr nljOperatorHandler)
        : PipelineExecutionContext(
            -1,// mock pipeline id
            0, // mock query id
            nullptr,
            1,
            [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                emittedBuffers.emplace_back(std::move(buffer));
            },
            [this](TupleBuffer& buffer) {
                emittedBuffers.emplace_back(std::move(buffer));
            },
            {nljOperatorHandler}) {}
};

class NestedLoopJoinOperatorTest : public Testing::TestWithErrorHandling {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    uint64_t windowSize;
    uint64_t handlerIndex;
    const uint64_t leftPageSize = 1024;
    const uint64_t rightPageSize = 256;
    uint64_t leftEntrySize;
    uint64_t rightEntrySize;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NestedLoopJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NestedLoopJoinOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        TestWithErrorHandling::SetUp();
        NES_INFO("Setup NestedLoopJoinOperatorTest test case.");
        leftSchema = Schema::create()
                         ->addField("id", BasicType::UINT64)
                         ->addField("value_left", BasicType::UINT64)
                         ->addField("ts", BasicType::UINT64);
        rightSchema = Schema::create()
                          ->addField("id", BasicType::UINT64)
                          ->addField("value_right", BasicType::UINT64)
                          ->addField("ts", BasicType::UINT64);

        bm = std::make_shared<BufferManager>();
        windowSize = 1000;
        handlerIndex = 0;

        leftEntrySize = leftSchema->getSchemaSizeInBytes();
        rightEntrySize = rightSchema->getSchemaSizeInBytes();
    }

    std::vector<Record> createRandomRecords(uint64_t numberOfRecords,
                                            QueryCompilation::JoinBuildSideType joinBuildSide,
                                            uint64_t minValue = 0,
                                            uint64_t maxValue = 1000,
                                            uint64_t randomSeed = 42) {
        std::vector<Record> retVector;
        auto schema = joinBuildSide == QueryCompilation::JoinBuildSideType::Left ? leftSchema : rightSchema;

        std::mt19937 generator(randomSeed);
        std::uniform_int_distribution<uint64_t> distribution(minValue, maxValue);

        for (auto i = 0_u64; i < numberOfRecords; ++i) {
            retVector.emplace_back(Record({{schema->get(0)->getName(), Value<UInt64>(0_u64)},
                                           {schema->get(1)->getName(), Value<UInt64>(i + 1000)},
                                           {schema->get(2)->getName(), Value<UInt64>(i)}}));
        }

        return retVector;
    }

    uint64_t calculateExpNoTuplesInWindow(uint64_t totalTuples, uint64_t windowIdentifier) {
        std::vector<uint64_t> tmpVec;
        while (totalTuples > windowSize) {
            tmpVec.emplace_back(windowSize);
            totalTuples -= windowSize;
        }
        tmpVec.emplace_back(totalTuples);
        auto noWindow = windowIdentifier / windowSize;
        return tmpVec[noWindow];
    }

    void insertRecordsIntoBuild(Operators::NLJBuild& nljBuildLeft,
                                Operators::NLJBuild& nljBuildRight,
                                uint64_t numberOfRecordsLeft,
                                uint64_t numberOfRecordsRight,
                                Operators::NLJOperatorHandler& nljOperatorHandler,
                                const std::string& timestampLeft,
                                const std::string& timestampRight,
                                ExecutionContext& executionContext) {

        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, QueryCompilation::JoinBuildSideType::Left);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, QueryCompilation::JoinBuildSideType::Right);

        nljBuildLeft.setup(executionContext);
        nljBuildRight.setup(executionContext);

        // We do not care for the record buffer in the current NLJBuild::open() implementation
        RecordBuffer recordBuffer(Value<MemRef>((int8_t*) nullptr));
        nljBuildLeft.open(executionContext, recordBuffer);
        nljBuildRight.open(executionContext, recordBuffer);

        uint64_t maxTimestamp = 2;
        for (auto& leftRecord : allLeftRecords) {
            maxTimestamp = std::max(leftRecord.read(timestampLeft).getValue().staticCast<UInt64>().getValue(), maxTimestamp);
            nljBuildLeft.execute(executionContext, leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            maxTimestamp = std::max(rightRecord.read(timestampRight).getValue().staticCast<UInt64>().getValue(), maxTimestamp);
            nljBuildRight.execute(executionContext, rightRecord);
        }

        auto memoryProviderLeft =
            MemoryProvider::MemoryProvider::createMemoryProvider(leftSchema->getSchemaSizeInBytes(), leftSchema);
        auto memoryProviderRight =
            MemoryProvider::MemoryProvider::createMemoryProvider(rightSchema->getSchemaSizeInBytes(), rightSchema);

        auto maxWindowIdentifier = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);
            auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(numberOfRecordsRight, windowIdentifier);

            NES_DEBUG("Check window={}", windowIdentifier);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, QueryCompilation::JoinBuildSideType::Left),
                      expectedNumberOfTuplesInWindowLeft);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, QueryCompilation::JoinBuildSideType::Right),
                      expectedNumberOfTuplesInWindowRight);
            auto nljWindow =
                std::dynamic_pointer_cast<NLJWindow>(nljOperatorHandler.getWindowByWindowIdentifier(windowIdentifier).value());
            Nautilus::Value<UInt64> zeroVal((uint64_t) 0);

            auto leftPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefLeft(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftEntrySize);
            auto windowStartPos = windowIdentifier - windowSize;
            auto windowEndPosLeft = windowStartPos + expectedNumberOfTuplesInWindowLeft;
            uint64_t posInWindow = 0UL;
            for (auto pos = windowStartPos; pos < windowEndPosLeft; ++pos, ++posInWindow) {
                auto leftRecordMemRef = *leftPagedVector.at(pos);
                auto& leftRecord = allLeftRecords[pos];
                auto readRecordLeft = memoryProviderLeft->read({}, leftRecordMemRef, zeroVal);
                NES_TRACE("readRecordLeft {} leftRecord{}", readRecordLeft.toString(), leftRecord.toString());

                for (auto& field : leftSchema->fields) {
                    EXPECT_EQ(readRecordLeft.read(field->getName()), leftRecord.read(field->getName()));
                }
            }

            posInWindow = 0;
            auto windowEndPosRight = windowStartPos + expectedNumberOfTuplesInWindowRight;
            auto rightPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefRight(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightEntrySize);

            for (auto pos = windowStartPos; pos < windowEndPosRight; ++pos, ++posInWindow) {
                auto rightRecordMemRef = *leftPagedVector.at(pos);
                auto& rightRecord = allRightRecords[pos];
                auto readRecordRight = memoryProviderRight->read({}, rightRecordMemRef, zeroVal);
                NES_TRACE("readRecordRight {} rightRecord{}", readRecordRight.toString(), rightRecord.toString());

                for (auto& field : rightSchema->fields) {
                    EXPECT_EQ(readRecordRight.read(field->getName()), rightRecord.read(field->getName()));
                }
            }
        }
    }

    void insertRecordsIntoProbe(Operators::NLJProbe& nljProbe,
                                uint64_t numberOfRecordsLeft,
                                uint64_t numberOfRecordsRight,
                                Operators::NLJOperatorHandler& nljOperatorHandler,
                                const std::string& timestampFieldNameLeft,
                                const std::string& timestampFieldNameRight,
                                const std::string& joinFieldNameLeft,
                                const std::string& joinFieldNameRight,
                                ExecutionContext& executionContext) {

        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, QueryCompilation::JoinBuildSideType::Left);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, QueryCompilation::JoinBuildSideType::Right);
        auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);

        auto memoryProviderLeft = MemoryProvider::MemoryProvider::createMemoryProvider(bm->getBufferSize(), leftSchema);
        auto memoryProviderRight = MemoryProvider::MemoryProvider::createMemoryProvider(bm->getBufferSize(), rightSchema);

        uint64_t maxTimestamp = 0Ul;
        Value<UInt64> zeroVal(0_u64);
        for (auto& leftRecord : allLeftRecords) {
            auto timestamp = leftRecord.read(timestampFieldNameLeft).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);

            auto nljWindow = std::dynamic_pointer_cast<NLJWindow>(nljOperatorHandler.getWindowByTimestampOrCreateIt(timestamp));
            auto leftPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefLeft(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorRef leftPagedVector(leftPagedVectorRef, leftEntrySize);

            auto memRefToRecord = leftPagedVector.allocateEntry();
            memoryProviderLeft->write(zeroVal, memRefToRecord, leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            auto timestamp = rightRecord.read(timestampFieldNameRight).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);

            auto nljWindow = std::dynamic_pointer_cast<NLJWindow>(nljOperatorHandler.getWindowByTimestampOrCreateIt(timestamp));
            auto rightPagedVectorRef =
                Nautilus::Value<Nautilus::MemRef>((int8_t*) nljWindow->getPagedVectorRefRight(/*workerId*/ 0));
            Nautilus::Interface::PagedVectorRef rightPagedVector(rightPagedVectorRef, rightEntrySize);

            auto memRefToRecord = rightPagedVector.allocateEntry();
            memoryProviderRight->write(zeroVal, memRefToRecord, rightRecord);
        }

        auto collector = std::make_shared<Operators::CollectOperator>();
        nljProbe.setChild(collector);

        Value<UInt64> zeroValue(0_u64);
        auto maxWindowIdentifier = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);
            auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(numberOfRecordsRight, windowIdentifier);

            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, QueryCompilation::JoinBuildSideType::Left),
                      expectedNumberOfTuplesInWindowLeft);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, QueryCompilation::JoinBuildSideType::Right),
                      expectedNumberOfTuplesInWindowRight);

            {
                auto tupleBuffer = bm->getBufferBlocking();
                std::memcpy(tupleBuffer.getBuffer(), &windowIdentifier, sizeof(windowIdentifier));
                tupleBuffer.setNumberOfTuples(1);

                RecordBuffer recordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
                nljProbe.open(executionContext, recordBuffer);
            }

            for (auto& leftRecord : allLeftRecords) {
                for (auto& rightRecord : allRightRecords) {

                    auto timestampLeftVal = leftRecord.read(timestampFieldNameLeft).getValue().staticCast<UInt64>().getValue();
                    auto timestampRightVal = rightRecord.read(timestampFieldNameRight).getValue().staticCast<UInt64>().getValue();

                    auto windowStart = windowIdentifier - windowSize;
                    auto windowEnd = windowIdentifier;
                    auto leftKey = leftRecord.read(joinFieldNameLeft);
                    auto rightKey = rightRecord.read(joinFieldNameRight);

                    DefaultPhysicalTypeFactory physicalDataTypeFactory;
                    auto joinKeySize =
                        physicalDataTypeFactory.getPhysicalType(leftSchema->get(joinFieldNameLeft)->getDataType())->size();
                    auto leftTupleSize = leftSchema->getSchemaSizeInBytes();
                    auto rightTupleSize = rightSchema->getSchemaSizeInBytes();

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
                    }
                }
            }
        }
    }
};

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestOneRecord) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 1;
    auto numberOfRecordsRight = 1;

    std::vector<OriginId> originIds{0};
    OriginId outputOriginId = 1;
    auto nljOperatorHandler = Operators::NLJOperatorHandler::create(originIds,
                                                                    outputOriginId,
                                                                    leftEntrySize,
                                                                    rightEntrySize,
                                                                    leftPageSize,
                                                                    rightPageSize,
                                                                    windowSize);

    auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldLeft);
    auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(
        handlerIndex,
        leftSchema,
        joinFieldnameLeft,
        QueryCompilation::JoinBuildSideType::Left,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft));
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(
        handlerIndex,
        rightSchema,
        joinFieldnameRight,
        QueryCompilation::JoinBuildSideType::Right,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight));

    NLJBuildPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft,
                           *nljBuildRight,
                           numberOfRecordsLeft,
                           numberOfRecordsRight,
                           *nljOperatorHandler,
                           timestampFieldLeft,
                           timestampFieldRight,
                           executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleRecords) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 500;
    auto numberOfRecordsRight = 500;
    windowSize = 2000;

    std::vector<OriginId> originIds{0};
    OriginId outputOriginId = 1;
    auto nljOperatorHandler = Operators::NLJOperatorHandler::create(originIds,
                                                                    outputOriginId,
                                                                    leftEntrySize,
                                                                    rightEntrySize,
                                                                    leftPageSize,
                                                                    rightPageSize,
                                                                    windowSize);

    auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldLeft);
    auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(
        handlerIndex,
        leftSchema,
        joinFieldnameLeft,
        QueryCompilation::JoinBuildSideType::Left,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft));
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(
        handlerIndex,
        rightSchema,
        joinFieldnameRight,
        QueryCompilation::JoinBuildSideType::Right,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight));

    NLJBuildPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft,
                           *nljBuildRight,
                           numberOfRecordsLeft,
                           numberOfRecordsRight,
                           *nljOperatorHandler,
                           timestampFieldLeft,
                           timestampFieldRight,
                           executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleWindows) {
    auto joinFieldNameLeft = leftSchema->get(1)->getName();
    auto joinFieldNameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 100;
    auto numberOfRecordsRight = 100;
    windowSize = 50;

    std::vector<OriginId> originIds{0};
    OriginId outputOriginId = 1;
    auto nljOperatorHandler = Operators::NLJOperatorHandler::create(originIds,
                                                                    outputOriginId,
                                                                    leftEntrySize,
                                                                    rightEntrySize,
                                                                    leftPageSize,
                                                                    rightPageSize,
                                                                    windowSize);
    auto readTsFieldLeft = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldLeft);
    auto readTsFieldRight = std::make_shared<Expressions::ReadFieldExpression>(timestampFieldRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(
        handlerIndex,
        leftSchema,
        joinFieldNameLeft,
        QueryCompilation::JoinBuildSideType::Left,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldLeft));
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(
        handlerIndex,
        rightSchema,
        joinFieldNameRight,
        QueryCompilation::JoinBuildSideType::Right,
        std::make_unique<Runtime::Execution::Operators::EventTimeFunction>(readTsFieldRight));

    NLJBuildPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft,
                           *nljBuildRight,
                           numberOfRecordsLeft,
                           numberOfRecordsRight,
                           *nljOperatorHandler,
                           timestampFieldLeft,
                           timestampFieldRight,
                           executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinProbeSimpleTestOneWindow) {
    const auto joinFieldNameLeft = leftSchema->get(1)->getName();
    const auto joinFieldNameRight = rightSchema->get(1)->getName();
    const auto timestampFieldLeft = leftSchema->get(2)->getName();
    const auto timestampFieldRight = leftSchema->get(2)->getName();
    const auto numberOfRecordsLeft = 200;
    const auto numberOfRecordsRight = 200;
    windowSize = 1000;

    std::vector<OriginId> originIds{0};
    const auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();
    const auto windowKeyFieldName = joinSchema->get(2)->getName();

    OriginId outputOriginId = 1;
    auto nljOperatorHandler = Operators::NLJOperatorHandler::create(originIds,
                                                                    outputOriginId,
                                                                    leftEntrySize,
                                                                    rightEntrySize,
                                                                    leftPageSize,
                                                                    rightPageSize,
                                                                    windowSize);
    auto nljProbe = std::make_shared<Operators::NLJProbe>(handlerIndex,
                                                          leftSchema,
                                                          rightSchema,
                                                          joinSchema,
                                                          leftEntrySize,
                                                          rightEntrySize,
                                                          joinFieldNameLeft,
                                                          joinFieldNameRight,
                                                          windowStartFieldName,
                                                          windowEndFieldName,
                                                          windowKeyFieldName);
    NLJProbePiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoProbe(*nljProbe,
                           numberOfRecordsLeft,
                           numberOfRecordsRight,
                           *nljOperatorHandler,
                           timestampFieldLeft,
                           timestampFieldRight,
                           joinFieldNameLeft,
                           joinFieldNameRight,
                           executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinProbeSimpleTestMultipleWindows) {
    auto joinFieldNameLeft = leftSchema->get(1)->getName();
    auto joinFieldNameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 100;
    auto numberOfRecordsRight = 100;
    windowSize = 10;

    std::vector<OriginId> originIds{0};
    auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldNameLeft);
    const auto windowStartFieldName = joinSchema->get(0)->getName();
    const auto windowEndFieldName = joinSchema->get(1)->getName();
    const auto windowKeyFieldName = joinSchema->get(2)->getName();

    OriginId outputOriginId = 1;
    auto nljOperatorHandler = Operators::NLJOperatorHandler::create(originIds,
                                                                    outputOriginId,
                                                                    leftEntrySize,
                                                                    rightEntrySize,
                                                                    leftPageSize,
                                                                    rightPageSize,
                                                                    windowSize);
    auto nljProbe = std::make_shared<Operators::NLJProbe>(handlerIndex,
                                                          leftSchema,
                                                          rightSchema,
                                                          joinSchema,
                                                          leftEntrySize,
                                                          rightEntrySize,
                                                          joinFieldNameLeft,
                                                          joinFieldNameRight,
                                                          windowStartFieldName,
                                                          windowEndFieldName,
                                                          windowKeyFieldName);
    NLJProbePiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoProbe(*nljProbe,
                           numberOfRecordsLeft,
                           numberOfRecordsRight,
                           *nljOperatorHandler,
                           timestampFieldLeft,
                           timestampFieldRight,
                           joinFieldNameLeft,
                           joinFieldNameRight,
                           executionContext);
}

}// namespace NES::Runtime::Execution