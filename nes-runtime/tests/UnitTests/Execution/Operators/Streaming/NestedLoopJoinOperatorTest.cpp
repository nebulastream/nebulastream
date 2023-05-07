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
#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJBuild.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/JoinPhases/NLJSink.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <random>

namespace NES::Runtime::Execution {

class NestedLoopJoinOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
    std::vector<TupleBuffer> emittedBuffers;
    WorkerContextPtr workerContext;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    uint64_t windowSize;
    uint64_t handlerIndex;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NestedLoopJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NestedLoopJoinOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
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
        emittedBuffers.clear();
        windowSize = 1000;
        handlerIndex = 0;
    }

    std::vector<Record> createRandomRecords(uint64_t numberOfRecords, bool isLeftSide,
                                            uint64_t minValue = 0, uint64_t maxValue = 1000, uint64_t randomSeed = 42) {
        std::vector<Record> retVector;
        auto schema = isLeftSide ? leftSchema : rightSchema;

        std::mt19937 generator(randomSeed);
        std::uniform_int_distribution<uint64_t> distribution(minValue, maxValue);

        for (auto i = 0UL; i < numberOfRecords; ++i) {
            retVector.emplace_back(Record({{schema->get(0)->getName(), Value<UInt64>(0UL)},
                                           {schema->get(1)->getName(), Value<UInt64>(distribution(generator))},
                                           {schema->get(2)->getName(), Value<UInt64>(i)}}));
        }

        return retVector;
    }

    void insertRecordsIntoBuild(Operators::NLJBuild& nljBuildLeft, Operators::NLJBuild& nljBuildRight,
                                uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight,
                                Operators::NLJOperatorHandler& nljOperatorHandler,
                                const std::string& timestampLeft, const std::string& timestampRight,
                                ExecutionContext& executionContext) {


        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, /*isLeftSide*/ true);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, /*isLeftSide*/ false);


        nljBuildLeft.setup(executionContext);
        nljBuildRight.setup(executionContext);
        auto maxTimestamp = 0Ul;
        for (auto& leftRecord : allLeftRecords) {
            maxTimestamp = std::max(leftRecord.read(timestampLeft).getValue().staticCast<UInt64>().getValue(), maxTimestamp);
            nljBuildLeft.execute(executionContext, leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            maxTimestamp = std::max(rightRecord.read(timestampRight).getValue().staticCast<UInt64>().getValue(), maxTimestamp);
            nljBuildRight.execute(executionContext, rightRecord);
        }

        auto memoryProviderLeft = MemoryProvider::MemoryProvider::createMemoryProvider(leftSchema->getSchemaSizeInBytes(),
                                                                                       leftSchema);
        auto memoryProviderRight = MemoryProvider::MemoryProvider::createMemoryProvider(rightSchema->getSchemaSizeInBytes(),
                                                                                        rightSchema);
        Value<UInt64> zeroValue(0UL);
        auto maxWindowIdentifier = std::round((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize - 1; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ true), 1);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ false), 1);

            auto startOfTupleLeft = Value<MemRef>((int8_t*) nljOperatorHandler.getFirstTuple(windowIdentifier, /*isLeftSide*/ true));
            auto startOfTupleRight = Value<MemRef>((int8_t*) nljOperatorHandler.getFirstTuple(windowIdentifier, /*isLeftSide*/ false));
            auto readRecordLeft = memoryProviderLeft->read({}, startOfTupleLeft, zeroValue);
            auto readRecordRight = memoryProviderRight->read({}, startOfTupleRight, zeroValue);

            for (auto& leftRecord : allLeftRecords) {
                for (auto& field : leftSchema->fields) {
                    ASSERT_EQ(readRecordLeft.read(field->getName()), leftRecord.read(field->getName()));
                }
            }
            for (auto& rightRecord : allRightRecords) {
                for (auto& field : rightSchema->fields) {
                    ASSERT_EQ(readRecordRight.read(field->getName()), rightRecord.read(field->getName()));
                }
            }
        }
    }

    void insertRecordsIntoSink(Operators::NLJSink& nljSink,
                               uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight,
                               Operators::NLJOperatorHandler& nljOperatorHandler,
                               const std::string& timestampFieldnameLeft, const std::string& timestampFieldnameRight,
                               const std::string& joinFieldnameLeft, const std::string& joinFieldnameRight,
                               ExecutionContext& executionContext) {

        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, /*isLeftSide*/ true);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, /*isLeftSide*/ false);
        auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldnameLeft);


        auto maxTimestamp = 0Ul;
        for (auto& leftRecord : allLeftRecords) {
            auto timestamp = leftRecord.read(timestampFieldnameLeft).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);
            nljOperatorHandler.insertNewTuple(timestamp, /*isLeftSide*/ true);
        }

        for (auto& rightRecord : allRightRecords) {
            auto timestamp = rightRecord.read(timestampFieldnameRight).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);
            nljOperatorHandler.insertNewTuple(timestamp, /*isLeftSide*/ false);
        }

        auto memoryProviderLeft = MemoryProvider::MemoryProvider::createMemoryProvider(leftSchema->getSchemaSizeInBytes(),
                                                                                       leftSchema);
        auto memoryProviderRight = MemoryProvider::MemoryProvider::createMemoryProvider(rightSchema->getSchemaSizeInBytes(),
                                                                                        rightSchema);
        Value<UInt64> zeroValue(0UL);
        auto maxWindowIdentifier = std::round((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize - 1; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ true), 1);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ false), 1);

            auto startOfTupleLeft = Value<MemRef>((int8_t*) nljOperatorHandler.getFirstTuple(windowIdentifier, /*isLeftSide*/ true));
            auto startOfTupleRight = Value<MemRef>((int8_t*) nljOperatorHandler.getFirstTuple(windowIdentifier, /*isLeftSide*/ false));
            auto readRecordLeft = memoryProviderLeft->read({}, startOfTupleLeft, zeroValue);
            auto readRecordRight = memoryProviderRight->read({}, startOfTupleRight, zeroValue);

            for (auto& leftRecord : allLeftRecords) {
                for (auto& field : leftSchema->fields) {
                    ASSERT_EQ(readRecordLeft.read(field->getName()), leftRecord.read(field->getName()));
                }
            }
            for (auto& rightRecord : allRightRecords) {
                for (auto& field : rightSchema->fields) {
                    ASSERT_EQ(readRecordRight.read(field->getName()), rightRecord.read(field->getName()));
                }
            }

            {
                auto tupleBuffer = bm->getBufferBlocking();
                std::memcpy(tupleBuffer.getBuffer(), &windowIdentifier, sizeof(windowIdentifier));
                tupleBuffer.setNumberOfTuples(1);
            }

            RecordBuffer recordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
            nljSink.open(executionContext, recordBuffer);

            std::vector<TupleBuffer> expectedTuplesBuffers;
            auto buffer = bm->getBufferBlocking();
            auto numberOfTuplesPerBuffer = bm->getBufferSize() / joinSchema->getSchemaSizeInBytes();
            for (auto& leftRecord : allLeftRecords) {
                for (auto& rightRecord : allRightRecords) {

                    auto timestampLeftVal = leftRecord.read(timestampFieldnameLeft).getValue().staticCast<UInt64>().getValue();
                    auto timestampRightVal = rightRecord.read(timestampFieldnameRight).getValue().staticCast<UInt64>().getValue();

                    auto windowStart = windowIdentifier - windowSize + 1;
                    auto windowEnd = windowIdentifier + 1;
                    auto leftKey = leftRecord.read(joinFieldnameLeft);
                    auto rightKey = rightRecord.read(joinFieldnameRight);

                    auto sizeOfWindowStart = sizeof(uint64_t);
                    auto sizeOfWindowEnd = sizeof(uint64_t);

                    DefaultPhysicalTypeFactory physicalDataTypeFactory;
                    auto joinKeySize = physicalDataTypeFactory.getPhysicalType(leftSchema->get(joinFieldnameLeft)->getDataType())->size();
                    auto leftTupleSize = leftSchema->getSchemaSizeInBytes();
                    auto rightTupleSize = rightSchema->getSchemaSizeInBytes();


                    if (windowStart <= timestampLeftVal && timestampLeftVal < windowEnd &&
                        windowStart <= timestampRightVal && timestampRightVal < windowEnd &&
                        leftKey == rightKey) {
                        // Join these two tuples together
                        auto bufferPtr = buffer.getBuffer();
                        auto numberOfTuples = buffer.getNumberOfTuples();
                        auto bufferMemRef = Value<MemRef>((int8_t*) bufferPtr + joinSchema->getSchemaSizeInBytes());

                        bufferMemRef.store(Value<UInt64>(windowStart));
                        bufferMemRef = bufferMemRef + sizeOfWindowStart;
                        bufferMemRef.store(Value<UInt64>(windowEnd));
                        bufferMemRef = bufferMemRef + sizeOfWindowEnd;

                        bufferMemRef.store(leftKey);
                        bufferMemRef = bufferMemRef + joinKeySize;

                        for (auto& field : leftSchema->fields) {
                            auto const fieldName = field->getName();
                            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

                            bufferMemRef.store(leftRecord.read(fieldName));
                            bufferMemRef = bufferMemRef + fieldType->size();
                        }
                        for (auto& field : rightSchema->fields) {
                            auto const fieldName = field->getName();
                            auto const fieldType = physicalDataTypeFactory.getPhysicalType(field->getDataType());

                            bufferMemRef.store(rightRecord.read(fieldName));
                            bufferMemRef = bufferMemRef + fieldType->size();
                        }

                        buffer.setNumberOfTuples(++numberOfTuples);

                        if (numberOfTuples >= numberOfTuplesPerBuffer) {
                            expectedTuplesBuffers.emplace_back(buffer);
                            buffer = bm->getBufferBlocking();
                            buffer.setNumberOfTuples(0);
                        }
                    }
                }

                hier weiter machen mit dem vergleichen von den expectedTuplesBuffers und emittedBuffers...
            }
        }


    }
};

class MockPiplineExecutionContext : public PipelineExecutionContext {
  public:
    MockPiplineExecutionContext(OperatorHandlerPtr nljOperatorHandler,
                                NestedLoopJoinOperatorTest* nljOperatorTest) :
                                    PipelineExecutionContext(
                                    -1,// mock pipeline id
                                    0, // mock query id
                                    nullptr,
                                    1,
                                    [&nljOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                                    nljOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
                                    },
                                    [&nljOperatorTest](TupleBuffer& buffer) {
                                    nljOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
                                    },
                                    {nljOperatorHandler}) {}
};

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestOneRecord) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 1;
    auto numberOfRecordsRight = 1;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldnameLeft, timestampFieldLeft,
                                                              /*isLeftSide*/ true);
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldnameRight, timestampFieldRight,
                                                               /*isLeftSide*/ false);

    MockPiplineExecutionContext pipelineContext(nljOperatorHandler, this);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft, *nljBuildRight, numberOfRecordsLeft, numberOfRecordsRight,
                           *nljOperatorHandler, timestampFieldLeft, timestampFieldRight, executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleRecords) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 500;
    auto numberOfRecordsRight = 500;
    windowSize = 1000;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldnameLeft, timestampFieldLeft,
                                                              /*isLeftSide*/ true);
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldnameRight, timestampFieldRight,
                                                               /*isLeftSide*/ false);

    MockPiplineExecutionContext pipelineContext(nljOperatorHandler, this);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft, *nljBuildRight, numberOfRecordsLeft, numberOfRecordsRight,
                           *nljOperatorHandler, timestampFieldLeft, timestampFieldRight, executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinBuildSimpleTestMultipleWindows) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 10000;
    auto numberOfRecordsRight = 10000;
    windowSize = 200;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldnameLeft, timestampFieldLeft,
                                                              /*isLeftSide*/ true);
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldnameRight, timestampFieldRight,
                                                               /*isLeftSide*/ false);

    MockPiplineExecutionContext pipelineContext(nljOperatorHandler, this);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft, *nljBuildRight, numberOfRecordsLeft, numberOfRecordsRight,
                           *nljOperatorHandler, timestampFieldLeft, timestampFieldRight, executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestOneRecord) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 10000;
    auto numberOfRecordsRight = 10000;
    windowSize = 10;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);
    auto nljSink = std::make_shared<Operators::NLJSink>(handlerIndex);
    MockPiplineExecutionContext pipelineContext(nljOperatorHandler, this);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));


    insertRecordsIntoSink(*nljSink, numberOfRecordsLeft, numberOfRecordsRight, *nljOperatorHandler,
                          timestampFieldLeft, timestampFieldRight, executionContext);

}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestMultipleRecords) {

}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestMultipleWindows) {

}

} // namespace NES::Runtime::Execution