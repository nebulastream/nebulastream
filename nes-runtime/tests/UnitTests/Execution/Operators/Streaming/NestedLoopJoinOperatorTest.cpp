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

class NLJBuildPiplineExecutionContext : public PipelineExecutionContext {
  public:
    NLJBuildPiplineExecutionContext(OperatorHandlerPtr nljOperatorHandler) :
                 PipelineExecutionContext(
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

class NLJSinkPiplineExecutionContext : public PipelineExecutionContext {
  public:
    std::vector<TupleBuffer> emittedBuffers;
    NLJSinkPiplineExecutionContext(OperatorHandlerPtr nljOperatorHandler) :
                 PipelineExecutionContext(
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

class NestedLoopJoinOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
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
//            retVector.emplace_back(Record({{schema->get(0)->getName(), Value<UInt64>(0UL)},
//                                           {schema->get(1)->getName(), Value<UInt64>(distribution(generator))},
//                                           {schema->get(2)->getName(), Value<UInt64>(i)}}));
            retVector.emplace_back(Record({{schema->get(0)->getName(), Value<UInt64>(0UL)},
                                           {schema->get(1)->getName(), Value<UInt64>(i + 1000)},
                                           {schema->get(2)->getName(), Value<UInt64>(i)}}));
        }

        return retVector;
    }

    uint64_t calculateExpNoTuplesInWindow(uint64_t totalTuples, uint64_t windowIdentifier) {
        std::vector<uint64_t> tmpVec;
        while(totalTuples > windowSize) {
            tmpVec.emplace_back(windowSize);
            totalTuples -= windowSize;
        }
        tmpVec.emplace_back(totalTuples);
        auto noWindow = windowIdentifier / windowSize;
        return tmpVec[noWindow];
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
        auto maxWindowIdentifier = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize - 1; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);
            auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);

            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ true),
                      expectedNumberOfTuplesInWindowLeft);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ false),
                      expectedNumberOfTuplesInWindowRight);

            auto startOfTupleLeft = Value<MemRef>((int8_t*) nljOperatorHandler.getFirstTuple(windowIdentifier, /*isLeftSide*/ true));
            auto startOfTupleRight = Value<MemRef>((int8_t*) nljOperatorHandler.getFirstTuple(windowIdentifier, /*isLeftSide*/ false));

            auto windowStartPos = windowIdentifier - windowSize + 1;
            auto windowEndPosLeft = windowStartPos + expectedNumberOfTuplesInWindowLeft;
            auto posInWindow = 0UL;
            for (auto pos = windowStartPos; pos < windowEndPosLeft; ++pos, ++posInWindow) {
                Value<UInt64> posInWindowVal(posInWindow);
                auto& leftRecord = allLeftRecords[pos];
                auto readRecordLeft = memoryProviderLeft->read({}, startOfTupleLeft, posInWindowVal);
                NES_DEBUG2("readRecordLeft {} leftRecord{}", readRecordLeft.toString(), leftRecord.toString());

                for (auto& field : leftSchema->fields) {
                    EXPECT_EQ(readRecordLeft.read(field->getName()), leftRecord.read(field->getName()));
                }
            }

            posInWindow = 0;
            auto windowEndPosRight = windowStartPos + expectedNumberOfTuplesInWindowRight;
            for (auto pos = windowStartPos; pos < windowEndPosRight; ++pos, ++posInWindow) {
                Value<UInt64> posInWindowVal(posInWindow);
                auto& rightRecord = allRightRecords[pos];
                auto readRecordRight = memoryProviderRight->read({}, startOfTupleRight, posInWindowVal);
                NES_DEBUG2("readRecordRight {} rightRecord{}", readRecordRight.toString(), rightRecord.toString());

                for (auto& field : rightSchema->fields) {
                    EXPECT_EQ(readRecordRight.read(field->getName()), rightRecord.read(field->getName()));
                }
            }
        }
    }

    void insertRecordsIntoSink(Operators::NLJSink& nljSink,
                               uint64_t numberOfRecordsLeft, uint64_t numberOfRecordsRight,
                               Operators::NLJOperatorHandler& nljOperatorHandler,
                               const std::string& timestampFieldnameLeft, const std::string& timestampFieldnameRight,
                               const std::string& joinFieldnameLeft, const std::string& joinFieldnameRight,
                               NLJSinkPiplineExecutionContext& pipelineContext, ExecutionContext& executionContext) {

        auto allLeftRecords = createRandomRecords(numberOfRecordsLeft, /*isLeftSide*/ true);
        auto allRightRecords = createRandomRecords(numberOfRecordsRight, /*isLeftSide*/ false);
        auto joinSchema = Util::createJoinSchema(leftSchema, rightSchema, joinFieldnameLeft);

        auto memoryProviderLeft = MemoryProvider::MemoryProvider::createMemoryProvider(leftSchema->getSchemaSizeInBytes(),
                                                                                       leftSchema);
        auto memoryProviderRight = MemoryProvider::MemoryProvider::createMemoryProvider(rightSchema->getSchemaSizeInBytes(),
                                                                                        rightSchema);

        auto maxTimestamp = 0Ul;
        Value<UInt64> zeroVal(0UL);
        for (auto& leftRecord : allLeftRecords) {
            auto timestamp = leftRecord.read(timestampFieldnameLeft).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);
            auto pointerToRecord = nljOperatorHandler.insertNewTuple(timestamp, /*isLeftSide*/ true);
            auto memRefToRecord = Value<MemRef>((int8_t*) pointerToRecord);
            memoryProviderLeft->write(zeroVal, memRefToRecord, leftRecord);
        }

        for (auto& rightRecord : allRightRecords) {
            auto timestamp = rightRecord.read(timestampFieldnameRight).getValue().staticCast<UInt64>().getValue();
            maxTimestamp = std::max(timestamp, maxTimestamp);
            auto pointerToRecord = nljOperatorHandler.insertNewTuple(timestamp, /*isLeftSide*/ false);
            auto memRefToRecord = Value<MemRef>((int8_t*) pointerToRecord);
            memoryProviderRight->write(zeroVal, memRefToRecord, rightRecord);

        }

        Value<UInt64> zeroValue(0UL);
        auto maxWindowIdentifier = std::ceil((double) maxTimestamp / windowSize) * windowSize;
        for (auto windowIdentifier = windowSize - 1; windowIdentifier < maxWindowIdentifier; windowIdentifier += windowSize) {
            auto expectedNumberOfTuplesInWindowLeft = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);
            auto expectedNumberOfTuplesInWindowRight = calculateExpNoTuplesInWindow(numberOfRecordsLeft, windowIdentifier);

            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ true),
                      expectedNumberOfTuplesInWindowLeft);
            ASSERT_EQ(nljOperatorHandler.getNumberOfTuplesInWindow(windowIdentifier, /*isLeftSide*/ false),
                      expectedNumberOfTuplesInWindowRight);

            {
                auto tupleBuffer = bm->getBufferBlocking();
                std::memcpy(tupleBuffer.getBuffer(), &windowIdentifier, sizeof(windowIdentifier));
                tupleBuffer.setNumberOfTuples(1);

                RecordBuffer recordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
                nljSink.open(executionContext, recordBuffer);
            }


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
                        auto bufferMemRef = Value<MemRef>((int8_t*) bufferPtr + numberOfTuples * joinSchema->getSchemaSizeInBytes());

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
            }

            if (numberOfTuplesPerBuffer > 0) {
                expectedTuplesBuffers.emplace_back(buffer);
            }

            ASSERT_TRUE(expectedTuplesBuffers.size() > 0);
            for (auto expectedBuf : expectedTuplesBuffers) {
                bool foundBuffer = checkIfBufferFoundAndRemove(pipelineContext.emittedBuffers, expectedBuf, joinSchema, windowIdentifier);
                if (!foundBuffer) {
                    NES_ERROR("Could not find buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema) << " in emittedBuffers!");
                    ASSERT_TRUE(false);
                }
            }
        }
    }

    bool checkIfBufferFoundAndRemove(std::vector<TupleBuffer>& emittedBuffers,
                                     Runtime::TupleBuffer expectedBuffer,
                                     SchemaPtr joinSchema,
                                     uint64_t& windowIdentifier) {

        bool foundBuffer = false;

        for (auto tupleBufferIt = emittedBuffers.begin(); tupleBufferIt != emittedBuffers.end(); ++tupleBufferIt) {
            NES_TRACE2("Comparing versus {}", Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema));
            if (memcmp(tupleBufferIt->getBuffer(),
                       expectedBuffer.getBuffer(),
                       joinSchema->getSchemaSizeInBytes() * expectedBuffer.getNumberOfTuples())
                == 0) {
                NES_TRACE2("Removing buffer for windowIdentifier {} {} of size {}", windowIdentifier,
                           Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema), joinSchema->getSchemaSizeInBytes());
                emittedBuffers.erase(tupleBufferIt);
                foundBuffer = true;
                break;
            }
        }
        return foundBuffer;
    }
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

    NLJBuildPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);

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
    auto numberOfRecordsLeft = 5;
    auto numberOfRecordsRight = 5;
    windowSize = 10;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldnameLeft, timestampFieldLeft,
                                                              /*isLeftSide*/ true);
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldnameRight, timestampFieldRight,
                                                               /*isLeftSide*/ false);

    NLJBuildPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
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
    auto numberOfRecordsLeft = 10;
    auto numberOfRecordsRight = 10;
    windowSize = 5;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);

    auto nljBuildLeft = std::make_shared<Operators::NLJBuild>(handlerIndex, leftSchema, joinFieldnameLeft, timestampFieldLeft,
                                                              /*isLeftSide*/ true);
    auto nljBuildRight = std::make_shared<Operators::NLJBuild>(handlerIndex, rightSchema, joinFieldnameRight, timestampFieldRight,
                                                               /*isLeftSide*/ false);

    NLJBuildPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    insertRecordsIntoBuild(*nljBuildLeft, *nljBuildRight, numberOfRecordsLeft, numberOfRecordsRight,
                           *nljOperatorHandler, timestampFieldLeft, timestampFieldRight, executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestOneWindow) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 2;
    auto numberOfRecordsRight = 2;
    windowSize = 10;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);
    auto nljSink = std::make_shared<Operators::NLJSink>(handlerIndex);
    NLJSinkPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));


    insertRecordsIntoSink(*nljSink, numberOfRecordsLeft, numberOfRecordsRight, *nljOperatorHandler,
                          timestampFieldLeft, timestampFieldRight, joinFieldnameLeft, joinFieldnameRight,
                          pipelineContext, executionContext);
}

TEST_F(NestedLoopJoinOperatorTest, joinSinkSimpleTestMultipleWindows) {
    auto joinFieldnameLeft = leftSchema->get(1)->getName();
    auto joinFieldnameRight = rightSchema->get(1)->getName();
    auto timestampFieldLeft = leftSchema->get(2)->getName();
    auto timestampFieldRight = leftSchema->get(2)->getName();
    auto numberOfRecordsLeft = 100;
    auto numberOfRecordsRight = 100;
    windowSize = 10;

    auto nljOperatorHandler = std::make_shared<Operators::NLJOperatorHandler>(windowSize, leftSchema, rightSchema,
                                                                              joinFieldnameLeft, joinFieldnameRight);
    auto nljSink = std::make_shared<Operators::NLJSink>(handlerIndex);
    NLJSinkPiplineExecutionContext pipelineContext(nljOperatorHandler);
    WorkerContextPtr workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));


    insertRecordsIntoSink(*nljSink, numberOfRecordsLeft, numberOfRecordsRight, *nljOperatorHandler,
                          timestampFieldLeft, timestampFieldRight, joinFieldnameLeft, joinFieldnameRight,
                          pipelineContext, executionContext);
}

} // namespace NES::Runtime::Execution