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
#include <Common/DataTypes/DataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/JoinPhases/StreamJoinSink.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <NesBaseTest.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/UtilityFunctions.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES::Runtime::Execution {

class StreamJoinOperatorTest : public Testing::NESBaseTest {
  public:
    std::shared_ptr<Runtime::BufferManager> bm;
    std::vector<TupleBuffer> emittedBuffers;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("StreamJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup StreamJoinOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NESBaseTest::SetUp();
        NES_INFO("Setup StreamJoinOperatorTest test case.");
        bm = std::make_shared<Runtime::BufferManager>();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down StreamJoinOperatorTest test case.");
        NESBaseTest::TearDown();
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { NES_INFO("Tear down StreamJoinOperatorTest test class."); }
};

struct StreamJoinBuildHelper {
    size_t pageSize;
    size_t numPartitions;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t totalNumSources;
    size_t joinSizeInByte;
    size_t windowSize;
    Operators::StreamJoinBuildPtr streamJoinBuild;
    std::string joinFieldName;
    BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::string timeStampField;
    StreamJoinOperatorTest* streamJoinOperatorTest;
    bool isLeftSide;

    StreamJoinBuildHelper(Operators::StreamJoinBuildPtr streamJoinBuild,
                          const std::string& joinFieldName,
                          BufferManagerPtr bufferManager,
                          SchemaPtr schema,
                          const std::string& timeStampField,
                          StreamJoinOperatorTest* streamJoinOperatorTest,
                          bool isLeftSide)
        : pageSize(CHUNK_SIZE), numPartitions(NUM_PARTITIONS), numberOfTuplesToProduce(100), numberOfBuffersPerWorker(128),
          noWorkerThreads(1), totalNumSources(2), joinSizeInByte(1 * 1024 * 1024), windowSize(1000),
          streamJoinBuild(streamJoinBuild), joinFieldName(joinFieldName), bufferManager(bufferManager), schema(schema),
          timeStampField(timeStampField), streamJoinOperatorTest(streamJoinOperatorTest), isLeftSide(isLeftSide) {}
};

bool streamJoinBuildAndCheck(StreamJoinBuildHelper buildHelper) {
    auto workerContext =
        std::make_shared<WorkerContext>(/*workerId*/ 0, buildHelper.bufferManager, buildHelper.numberOfBuffersPerWorker);
    auto streamJoinOpHandler = std::make_shared<Operators::StreamJoinOperatorHandler>(buildHelper.schema,
                                                                                      buildHelper.schema,
                                                                                      buildHelper.joinFieldName,
                                                                                      buildHelper.joinFieldName,
                                                                                      buildHelper.noWorkerThreads * 2,
                                                                                      buildHelper.totalNumSources,
                                                                                      buildHelper.joinSizeInByte,
                                                                                      buildHelper.windowSize,
                                                                                      buildHelper.pageSize,
                                                                                      buildHelper.numPartitions);

    auto streamJoinOperatorTest = buildHelper.streamJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(
        -1,// mock pipeline id
        0, // mock query id
        nullptr,
        buildHelper.noWorkerThreads,
        [&streamJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
            streamJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        [&streamJoinOperatorTest](TupleBuffer& buffer) {
            streamJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        {streamJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    // Execute record and thus fill the hash table
    for (auto i = 0UL; i < buildHelper.numberOfTuplesToProduce + 1; ++i) {
        auto record = Nautilus::Record({{buildHelper.schema->get(0)->getName(), Value<UInt64>((uint64_t) i)},
                                        {buildHelper.schema->get(1)->getName(), Value<UInt64>((uint64_t)(i % 10) + 1)},
                                        {buildHelper.schema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});
        buildHelper.streamJoinBuild->execute(executionContext, record);

        uint64_t joinKey = record.read(buildHelper.joinFieldName).as<UInt64>().getValue().getValue();
        uint64_t timeStamp = record.read(buildHelper.timeStampField).as<UInt64>().getValue().getValue();
        auto hash = Util::murmurHash(joinKey);
        auto hashTable =
            streamJoinOpHandler->getWindow(timeStamp).getLocalHashTable(workerContext->getId(), buildHelper.isLeftSide);
        auto bucket = hashTable->getBucketLinkedList(hashTable->getBucketPos(hash));

        bool correctlyInserted = false;
        for (auto&& page : bucket->getPages()) {
            for (auto k = 0UL; k < page->size(); ++k) {
                uint8_t* recordPtr = page->operator[](k);
                auto bucketBuffer = Util::getBufferFromPointer(recordPtr, buildHelper.schema, buildHelper.bufferManager);
                auto recordBuffer = Util::getBufferFromRecord(record, buildHelper.schema, buildHelper.bufferManager);

                if (memcmp(bucketBuffer.getBuffer(), recordBuffer.getBuffer(), buildHelper.schema->getSchemaSizeInBytes()) == 0) {
                    correctlyInserted = true;
                    break;
                }
            }
        }

        if (!correctlyInserted) {
            NES_ERROR("Could not find buffer in bucket!");
            return false;
        }
    }

    return true;
}

struct StreamJoinSinkHelper {
    size_t pageSize;
    size_t numPartitions;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t numSourcesLeft, numSourcesRight;
    size_t joinSizeInByte;
    size_t windowSize;
    std::string joinFieldNameLeft, joinFieldNameRight;
    BufferManagerPtr bufferManager;
    SchemaPtr leftSchema, rightSchema;
    std::string timeStampField;
    StreamJoinOperatorTest* streamJoinOperatorTest;

    StreamJoinSinkHelper(const std::string& joinFieldNameLeft,
                         const std::string& joinFieldNameRight,
                         BufferManagerPtr bufferManager,
                         SchemaPtr leftSchema,
                         SchemaPtr rightSchema,
                         const std::string& timeStampField,
                         StreamJoinOperatorTest* streamJoinOperatorTest)
        : pageSize(CHUNK_SIZE), numPartitions(NUM_PARTITIONS), numberOfTuplesToProduce(100), numberOfBuffersPerWorker(128),
          noWorkerThreads(1), numSourcesLeft(1), numSourcesRight(1), joinSizeInByte(1 * 1024 * 1024), windowSize(1000),
          joinFieldNameLeft(joinFieldNameLeft), joinFieldNameRight(joinFieldNameRight), bufferManager(bufferManager),
          leftSchema(leftSchema), rightSchema(rightSchema), timeStampField(timeStampField),
          streamJoinOperatorTest(streamJoinOperatorTest) {}
};

bool checkIfBufferFoundAndRemove(std::vector<Runtime::TupleBuffer>& emittedBuffers,
                                 Runtime::TupleBuffer expectedBuffer,
                                 size_t sizeJoinedTuple,
                                 SchemaPtr joinSchema,
                                 uint64_t& removedBuffer) {

    bool foundBuffer = false;
    NES_TRACE("NLJ buffer = " << Util::printTupleBufferAsCSV(expectedBuffer, joinSchema));
    for (auto tupleBufferIt = emittedBuffers.begin(); tupleBufferIt != emittedBuffers.end(); ++tupleBufferIt) {
        if (memcmp(tupleBufferIt->getBuffer(), expectedBuffer.getBuffer(), sizeJoinedTuple * expectedBuffer.getNumberOfTuples())
            == 0) {
            NES_TRACE("Removing buffer #" << removedBuffer << " " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema)
                                          << " of size " << sizeJoinedTuple);
            emittedBuffers.erase(tupleBufferIt);
            foundBuffer = true;
            removedBuffer += 1;
            break;
        }
    }
    return foundBuffer;
}

bool streamJoinSinkAndCheck(StreamJoinSinkHelper streamJoinSinkHelper) {

    if (!streamJoinSinkHelper.leftSchema->contains(streamJoinSinkHelper.joinFieldNameLeft)) {
        NES_ERROR("JoinFieldNameLeft " << streamJoinSinkHelper.joinFieldNameLeft << " is not in leftSchema!");
        return false;
    }
    if (!streamJoinSinkHelper.rightSchema->contains(streamJoinSinkHelper.joinFieldNameRight)) {
        NES_ERROR("JoinFieldNameLeft " << streamJoinSinkHelper.joinFieldNameRight << " is not in leftSchema!");
        return false;
    }

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0,
                                                         streamJoinSinkHelper.bufferManager,
                                                         streamJoinSinkHelper.numberOfBuffersPerWorker);
    auto streamJoinOpHandler = std::make_shared<Operators::StreamJoinOperatorHandler>(streamJoinSinkHelper.leftSchema,
                                                                                      streamJoinSinkHelper.rightSchema,
                                                                                      streamJoinSinkHelper.joinFieldNameLeft,
                                                                                      streamJoinSinkHelper.joinFieldNameRight,
                                                                                      streamJoinSinkHelper.noWorkerThreads * 2,
                                                                                      streamJoinSinkHelper.numSourcesLeft
                                                                                          + streamJoinSinkHelper.numSourcesRight,
                                                                                      streamJoinSinkHelper.joinSizeInByte,
                                                                                      streamJoinSinkHelper.windowSize,
                                                                                      streamJoinSinkHelper.pageSize,
                                                                                      streamJoinSinkHelper.numPartitions);

    auto streamJoinOperatorTest = streamJoinSinkHelper.streamJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(
        0,// mock pipeline id
        1,// mock query id
        streamJoinSinkHelper.bufferManager,
        streamJoinSinkHelper.noWorkerThreads,
        [&streamJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
            streamJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        [&streamJoinOperatorTest](TupleBuffer& buffer) {
            streamJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
        },
        {streamJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    auto handlerIndex = 0UL;
    auto streamJoinBuildLeft = std::make_shared<Operators::StreamJoinBuild>(handlerIndex,
                                                                            /*isLeftSide*/ true,
                                                                            streamJoinSinkHelper.joinFieldNameLeft,
                                                                            streamJoinSinkHelper.timeStampField,
                                                                            streamJoinSinkHelper.leftSchema);
    auto streamJoinBuildRight = std::make_shared<Operators::StreamJoinBuild>(handlerIndex,
                                                                             /*isLeftSide*/ false,
                                                                             streamJoinSinkHelper.joinFieldNameRight,
                                                                             streamJoinSinkHelper.timeStampField,
                                                                             streamJoinSinkHelper.rightSchema);
    auto streamJoinSink = std::make_shared<Operators::StreamJoinSink>(handlerIndex);

    std::vector<std::vector<Nautilus::Record>> leftRecords;
    std::vector<std::vector<Nautilus::Record>> rightRecords;

    uint64_t lastTupleTimeStampWindow = streamJoinSinkHelper.windowSize - 1;
    std::vector<Nautilus::Record> tmpRecordsLeft, tmpRecordsRight;

    for (auto i = 0UL, curWindow = 0UL; i < streamJoinSinkHelper.numberOfTuplesToProduce + 1; ++i) {
        auto recordLeft =
            Nautilus::Record({{streamJoinSinkHelper.leftSchema->get(0)->getName(), Value<UInt64>((uint64_t) i)},
                              {streamJoinSinkHelper.leftSchema->get(1)->getName(), Value<UInt64>((uint64_t)(i % 10) + 10)},
                              {streamJoinSinkHelper.leftSchema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});

        auto recordRight =
            Nautilus::Record({{streamJoinSinkHelper.rightSchema->get(0)->getName(), Value<UInt64>((uint64_t) i + 1000)},
                              {streamJoinSinkHelper.rightSchema->get(1)->getName(), Value<UInt64>((uint64_t)(i % 10) + 10)},
                              {streamJoinSinkHelper.rightSchema->get(2)->getName(), Value<UInt64>((uint64_t) i)}});

        if (recordRight.read(streamJoinSinkHelper.timeStampField) > lastTupleTimeStampWindow) {
            leftRecords.push_back(std::vector(tmpRecordsLeft.begin(), tmpRecordsLeft.end()));
            rightRecords.push_back(std::vector(tmpRecordsRight.begin(), tmpRecordsRight.end()));

            tmpRecordsLeft = std::vector<Nautilus::Record>();
            tmpRecordsRight = std::vector<Nautilus::Record>();

            lastTupleTimeStampWindow += streamJoinSinkHelper.windowSize;
        }

        tmpRecordsLeft.emplace_back(recordLeft);
        tmpRecordsRight.emplace_back(recordRight);

        streamJoinBuildLeft->execute(executionContext, recordLeft);
        streamJoinBuildRight->execute(executionContext, recordRight);
    }

    auto numberOfEmittedBuffersBuild = streamJoinOperatorTest->emittedBuffers.size();
    for (auto cnt = 0UL; cnt < numberOfEmittedBuffersBuild; ++cnt) {
        auto tupleBuffer = streamJoinOperatorTest->emittedBuffers[cnt];
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        streamJoinSink->open(executionContext, recordBuffer);
    }

    // Delete all buffers that have been emitted from the build phase
    streamJoinOperatorTest->emittedBuffers.erase(streamJoinOperatorTest->emittedBuffers.begin(),
                                                 streamJoinOperatorTest->emittedBuffers.begin() + numberOfEmittedBuffersBuild);

    auto joinSchema = Util::createJoinSchema(streamJoinSinkHelper.leftSchema,
                                             streamJoinSinkHelper.rightSchema,
                                             streamJoinSinkHelper.joinFieldNameLeft);

    /* Checking if all windows have been deleted except for one.
     * We require always one window as we do not know here if we have to take care of more tuples*/
    if (streamJoinOpHandler->getNumActiveWindows() != 1) {
        NES_ERROR("Not exactly one active window!");
        return false;
    }

    uint64_t removedBuffer = 0UL;
    auto sizeJoinedTuple = joinSchema->getSchemaSizeInBytes();
    auto buffer = streamJoinSinkHelper.bufferManager->getBufferBlocking();
    auto tuplePerBuffer = streamJoinSinkHelper.bufferManager->getBufferSize() / sizeJoinedTuple;
    auto mergedEmittedBuffers = Util::mergeBuffersSameWindow(streamJoinOperatorTest->emittedBuffers,
                                                             joinSchema,
                                                             streamJoinSinkHelper.timeStampField,
                                                             streamJoinSinkHelper.bufferManager,
                                                             streamJoinSinkHelper.windowSize);
    auto sortedEmittedBuffers = Util::sortBuffersInTupleBuffer(mergedEmittedBuffers,
                                                               joinSchema,
                                                               streamJoinSinkHelper.timeStampField,
                                                               streamJoinSinkHelper.bufferManager);

    streamJoinOperatorTest->emittedBuffers.clear();
    mergedEmittedBuffers.clear();

    for (auto curWindow = 0UL; curWindow < leftRecords.size(); ++curWindow) {
        auto numberOfTuplesInBuffer = 0UL;
        for (auto& leftRecord : leftRecords[curWindow]) {
            for (auto& rightRecord : rightRecords[curWindow]) {
                if (leftRecord.read(streamJoinSinkHelper.joinFieldNameLeft)
                    == rightRecord.read(streamJoinSinkHelper.joinFieldNameRight)) {
                    // We expect to have at least one more buffer that was created by our join
                    if (sortedEmittedBuffers.size() == 0) {
                        NES_ERROR("Expected at least one buffer!");
                        return false;
                    }

                    int8_t* bufferPtr = (int8_t*) buffer.getBuffer() + numberOfTuplesInBuffer * sizeJoinedTuple;
                    auto keyRef = Nautilus::Value<Nautilus::MemRef>(bufferPtr);
                    keyRef.store(leftRecord.read(streamJoinSinkHelper.joinFieldNameLeft));

                    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
                    auto const fieldType = physicalDataTypeFactory.getPhysicalType(
                        streamJoinSinkHelper.leftSchema->get(streamJoinSinkHelper.joinFieldNameLeft)->getDataType());
                    bufferPtr += fieldType->size();

                    Util::writeNautilusRecord(0,
                                              bufferPtr,
                                              leftRecord,
                                              streamJoinSinkHelper.leftSchema,
                                              streamJoinSinkHelper.bufferManager);
                    Util::writeNautilusRecord(0,
                                              bufferPtr + streamJoinSinkHelper.leftSchema->getSchemaSizeInBytes(),
                                              rightRecord,
                                              streamJoinSinkHelper.rightSchema,
                                              streamJoinSinkHelper.bufferManager);
                    numberOfTuplesInBuffer += 1;
                    buffer.setNumberOfTuples(numberOfTuplesInBuffer);

                    if (numberOfTuplesInBuffer >= tuplePerBuffer) {
                        std::vector<Runtime::TupleBuffer> bufVec({buffer});
                        auto sortedBuffer = Util::sortBuffersInTupleBuffer(bufVec,
                                                                           joinSchema,
                                                                           streamJoinSinkHelper.timeStampField,
                                                                           streamJoinSinkHelper.bufferManager);

                        bool foundBuffer = checkIfBufferFoundAndRemove(sortedEmittedBuffers,
                                                                       sortedBuffer[0],
                                                                       sizeJoinedTuple,
                                                                       joinSchema,
                                                                       removedBuffer);

                        if (!foundBuffer) {
                            NES_ERROR("Could not find buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema)
                                                               << " in emittedBuffers!");
                            return false;
                        }

                        numberOfTuplesInBuffer = 0;
                        buffer = streamJoinSinkHelper.bufferManager->getBufferBlocking();
                    }
                }
            }
        }

        if (numberOfTuplesInBuffer > 0) {
            std::vector<Runtime::TupleBuffer> bufVec({buffer});
            auto sortedBuffer = Util::sortBuffersInTupleBuffer(bufVec,
                                                               joinSchema,
                                                               streamJoinSinkHelper.timeStampField,
                                                               streamJoinSinkHelper.bufferManager);
            bool foundBuffer =
                checkIfBufferFoundAndRemove(sortedEmittedBuffers, sortedBuffer[0], sizeJoinedTuple, joinSchema, removedBuffer);
            if (!foundBuffer) {
                NES_ERROR("Could not find buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema) << " in emittedBuffers!");
                return false;
            }
        }
    }

    // Make sure that after we have joined all records together no more buffer exist
    if (sortedEmittedBuffers.size() > 0) {
        NES_ERROR("Have not removed all buffers. So some tuples have not been joined together!");
        return false;
    }

    return true;
}

class TestRunner : public NES::Exceptions::ErrorListener {
  public:
    void onFatalError(int signalNumber, std::string callStack) override {
        std::ostringstream fatalErrorMessage;
        fatalErrorMessage << "onFatalError: signal [" << signalNumber << "] error [" << strerror(errno) << "] callstack "
                          << callStack;

        NES_FATAL_ERROR(fatalErrorMessage.str());
        std::cerr << fatalErrorMessage.str() << std::endl;
    }

    void onFatalException(std::shared_ptr<std::exception> exceptionPtr, std::string callStack) override {
        std::ostringstream fatalExceptionMessage;
        fatalExceptionMessage << "onFatalException: exception=[" << exceptionPtr->what() << "] callstack=\n" << callStack;

        NES_FATAL_ERROR(fatalExceptionMessage.str());
        std::cerr << fatalExceptionMessage.str() << std::endl;
    }
};

TEST_F(StreamJoinOperatorTest, joinBuildTest) {
    // Activating and installing error listener
    auto runner = std::make_shared<TestRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto streamJoinBuild =
        std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft, timeStampField, leftSchema);

    StreamJoinBuildHelper buildHelper(streamJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    EXPECT_TRUE(streamJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    EXPECT_EQ(emittedBuffers.size(), 0);
}

TEST_F(StreamJoinOperatorTest, joinBuildTestRight) {
    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = false;

    auto handlerIndex = 0;
    auto streamJoinBuild =
        std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameRight, timeStampField, rightSchema);
    StreamJoinBuildHelper buildHelper(streamJoinBuild, joinFieldNameRight, bm, rightSchema, timeStampField, this, isLeftSide);

    EXPECT_TRUE(streamJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    EXPECT_EQ(emittedBuffers.size(), 0);
}

TEST_F(StreamJoinOperatorTest, joinBuildTestMultiplePagesPerBucket) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto streamJoinBuild =
        std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft, timeStampField, leftSchema);

    StreamJoinBuildHelper buildHelper(streamJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    buildHelper.pageSize = leftSchema->getSchemaSizeInBytes() * 2;
    buildHelper.numPartitions = 1;

    EXPECT_TRUE(streamJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    EXPECT_EQ(emittedBuffers.size(), 0);
}

TEST_F(StreamJoinOperatorTest, joinBuildTestMultipleWindows) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto handlerIndex = 0;
    const auto isLeftSide = true;

    auto streamJoinBuild =
        std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft, timeStampField, leftSchema);

    StreamJoinBuildHelper buildHelper(streamJoinBuild, joinFieldNameLeft, bm, leftSchema, timeStampField, this, isLeftSide);
    buildHelper.pageSize = leftSchema->getSchemaSizeInBytes() * 2, buildHelper.numPartitions = 1;
    buildHelper.windowSize = 5;

    EXPECT_TRUE(streamJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    EXPECT_EQ(emittedBuffers.size(), 0);
}

TEST_F(StreamJoinOperatorTest, joinSinkTest) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    EXPECT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    StreamJoinSinkHelper streamJoinSinkHelper("f2_left", "f2_right", bm, leftSchema, rightSchema, "timestamp", this);
    streamJoinSinkHelper.pageSize = 2 * leftSchema->getSchemaSizeInBytes();
    streamJoinSinkHelper.numPartitions = 2;
    streamJoinSinkHelper.windowSize = 20;

    EXPECT_TRUE(streamJoinSinkAndCheck(streamJoinSinkHelper));
}

TEST_F(StreamJoinOperatorTest, joinSinkTestMultipleBuckets) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    EXPECT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    StreamJoinSinkHelper streamJoinSinkHelper("f2_left", "f2_right", bm, leftSchema, rightSchema, "timestamp", this);
    streamJoinSinkHelper.windowSize = 10;

    EXPECT_TRUE(streamJoinSinkAndCheck(streamJoinSinkHelper));
}

TEST_F(StreamJoinOperatorTest, joinSinkTestMultipleWindows) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                 ->addField("f1_right", BasicType::UINT64)
                                 ->addField("f2_right", BasicType::UINT64)
                                 ->addField("timestamp", BasicType::UINT64);

    EXPECT_EQ(leftSchema->getSchemaSizeInBytes(), rightSchema->getSchemaSizeInBytes());

    StreamJoinSinkHelper streamJoinSinkHelper("f2_left", "f2_right", bm, leftSchema, rightSchema, "timestamp", this);
    streamJoinSinkHelper.numPartitions = 1;
    streamJoinSinkHelper.windowSize = 10;

    EXPECT_TRUE(streamJoinSinkAndCheck(streamJoinSinkHelper));
}

}// namespace NES::Runtime::Execution