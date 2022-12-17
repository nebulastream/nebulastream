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
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinSink.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <Exceptions/ErrorListener.hpp>


namespace NES::Runtime::Execution {

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


class StreamJoinOperatorTest : public testing::Test {
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
        NES_INFO("Setup StreamJoinOperatorTest test case.");
        bm = std::make_shared<Runtime::BufferManager>();
    }

    /* Will be called after a test is executed. */
    void TearDown() override {
        NES_INFO("Tear down StreamJoinOperatorTest test case.");
    }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() {
        NES_INFO("Tear down StreamJoinOperatorTest test class.");
    }
};

struct StreamJoinBuildHelper {
    size_t pageSize = CHUNK_SIZE;
    size_t numPartitions = NUM_PARTITIONS;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t totalNumSources = 2;
    size_t joinSizeInByte;
    size_t windowSize;
    Operators::StreamJoinBuildPtr lazyJoinBuild;
    std::string joinFieldName;
    BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::string timeStampField;
    StreamJoinOperatorTest* lazyJoinOperatorTest;
    bool isLeftSide;
};



bool lazyJoinBuildAndCheck(StreamJoinBuildHelper buildHelper) {

    auto pageSize = buildHelper.pageSize;
    auto numPartitions = buildHelper.numPartitions;
    auto bufferManager = buildHelper.bufferManager;
    auto schema = buildHelper.schema;
    auto& joinFieldName = buildHelper.joinFieldName;
    auto numberOfBuffersPerWorker = buildHelper.numberOfBuffersPerWorker;
    auto noWorkerThreads = buildHelper.noWorkerThreads;
    auto joinSizeInByte = buildHelper.joinSizeInByte;
    auto windowSize = buildHelper.windowSize;
    auto numberOfTuplesToProduce = buildHelper.numberOfTuplesToProduce;
    auto lazyJoinBuild = buildHelper.lazyJoinBuild;
    auto& timeStampField = buildHelper.timeStampField;
    auto& lazyJoinOperatorTest = buildHelper.lazyJoinOperatorTest;
    auto isLeftSide = buildHelper.isLeftSide;
    auto totalNumSources = buildHelper.totalNumSources;

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bufferManager, numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<StreamJoinOperatorHandler>(schema, schema,
                                                                         joinFieldName, joinFieldName,
                                                                         noWorkerThreads * 2,
                                                                         totalNumSources,
                                                                         joinSizeInByte,
                                                                         windowSize, pageSize, numPartitions);

    auto pipelineContext = PipelineExecutionContext(-1,// mock pipeline id
                                                    0, // mock query id
                                                    nullptr,
                                                    noWorkerThreads,
                                                    [&lazyJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                                                        lazyJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
                                                    },
                                                    [&lazyJoinOperatorTest](TupleBuffer& buffer) {
                                                        lazyJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
                                                    },
                                                    {lazyJoinOpHandler});


    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*)workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*)(&pipelineContext)));


    // Execute record and thus fill the hash table
    for (auto i = 0UL; i < numberOfTuplesToProduce + 1; ++i) {
        auto record = Nautilus::Record({{schema->get(0)->getName(), Value<UInt64>(i)}, {schema->get(1)->getName(), Value<UInt64>((i % 10) + 1)}, {schema->get(2)->getName(), Value<UInt64>(i)}});
        lazyJoinBuild->execute(executionContext, record);

        uint64_t joinKey = record.read(joinFieldName).as<UInt64>().getValue().getValue();
        uint64_t timeStamp = record.read(timeStampField).as<UInt64>().getValue().getValue();
        auto hash = Util::murmurHash(joinKey);
        auto hashTable = lazyJoinOpHandler->getWindow(timeStamp).getLocalHashTable(workerContext->getId(), isLeftSide);
        auto bucket = hashTable.getBucketLinkedList(hashTable.getBucketPos(hash));

        bool correctlyInserted = false;
        for (auto page : bucket->getPages()) {
            for (auto k = 0UL; k < page->size(); ++k) {
                uint8_t* recordPtr = page->operator[](k);
                auto bucketBuffer = Util::getBufferFromPointer(recordPtr, schema, bufferManager);
                auto recordBuffer = Util::getBufferFromRecord(record, schema, bufferManager);

                if (memcmp(bucketBuffer.getBuffer(), recordBuffer.getBuffer(), schema->getSchemaSizeInBytes()) == 0) {
                    correctlyInserted = true;
                    break;
                }
            }
        }

        if (!correctlyInserted) {
            NES_ERROR("correctlyInserted is false!");
            return false;
        }
    }

    return true;
}


struct StreamJoinSinkHelper {
    BufferManagerPtr bufferManager;
    SchemaPtr leftSchema;
    SchemaPtr rightSchema;
    size_t numSourcesLeft, numSourcesRight;
    std::string joinFieldNameLeft, joinFieldNameRight;
    std::string timeStampField;
    size_t pageSize = CHUNK_SIZE;
    size_t numPartitions = NUM_PARTITIONS;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t joinSizeInByte;
    size_t windowSize;
    StreamJoinOperatorTest* lazyJoinOperatorTest;
};

bool checkIfBufferFoundAndRemove(std::vector<Runtime::TupleBuffer>& emittedBuffers,
                                 Runtime::TupleBuffer expectedBuffer,
                                 size_t sizeJoinedTuple, SchemaPtr joinSchema,
                                 uint64_t& removedBuffer) {

    bool foundBuffer = false;
    NES_DEBUG("NLJ buffer = " << Util::printTupleBufferAsCSV(expectedBuffer, joinSchema));
    for (auto tupleBufferIt = emittedBuffers.begin(); tupleBufferIt != emittedBuffers.end(); ++tupleBufferIt) {
        NES_DEBUG("Comparing versus = " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema));
        if (memcmp(tupleBufferIt->getBuffer(), expectedBuffer.getBuffer(), sizeJoinedTuple * expectedBuffer.getNumberOfTuples()) == 0) {
            NES_DEBUG("Removing buffer #" << removedBuffer << " " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema) << " of size " << sizeJoinedTuple);
            emittedBuffers.erase(tupleBufferIt);
            foundBuffer = true;
            removedBuffer += 1;
            break;
        }
    }
    return foundBuffer;
}

bool lazyJoinSinkAndCheck(StreamJoinSinkHelper lazyJoinSinkHelper) {

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, lazyJoinSinkHelper.bufferManager,
                                                         lazyJoinSinkHelper.numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<StreamJoinOperatorHandler>(lazyJoinSinkHelper.leftSchema, lazyJoinSinkHelper.rightSchema,
                                                                       lazyJoinSinkHelper.joinFieldNameLeft, lazyJoinSinkHelper.joinFieldNameRight,
                                                                       lazyJoinSinkHelper.noWorkerThreads * 2,
                                                                       lazyJoinSinkHelper.numSourcesLeft + lazyJoinSinkHelper.numSourcesRight,
                                                                       lazyJoinSinkHelper.joinSizeInByte,
                                                                       lazyJoinSinkHelper.windowSize,
                                                                       lazyJoinSinkHelper.pageSize,
                                                                       lazyJoinSinkHelper.numPartitions);

    auto lazyJoinOperatorTest = lazyJoinSinkHelper.lazyJoinOperatorTest;
    auto pipelineContext = PipelineExecutionContext(0,// mock pipeline id
                                                    1, // mock query id
                                                    lazyJoinSinkHelper.bufferManager,
                                                    lazyJoinSinkHelper.noWorkerThreads,
                                                    [&lazyJoinOperatorTest](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                                                        lazyJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
                                                    },
                                                    [&lazyJoinOperatorTest](TupleBuffer& buffer) {
                                                        lazyJoinOperatorTest->emittedBuffers.emplace_back(std::move(buffer));
                                                    },
                                                    {lazyJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*)workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*)(&pipelineContext)));


    auto handlerIndex = 0UL;
    auto lazyJoinBuildLeft = std::make_shared<Operators::StreamJoinBuild>(handlerIndex, /*isLeftSide*/ true,
                                                                        lazyJoinSinkHelper.joinFieldNameLeft,
                                                                        lazyJoinSinkHelper.timeStampField,
                                                                        lazyJoinSinkHelper.leftSchema);
    auto lazyJoinBuildRight = std::make_shared<Operators::StreamJoinBuild>(handlerIndex, /*isLeftSide*/ false,
                                                                         lazyJoinSinkHelper.joinFieldNameRight,
                                                                         lazyJoinSinkHelper.timeStampField,
                                                                         lazyJoinSinkHelper.rightSchema);
    auto lazyJoinSink = std::make_shared<Operators::StreamJoinSink>(handlerIndex);



    std::vector<std::vector<Nautilus::Record>> leftRecords;
    std::vector<std::vector<Nautilus::Record>> rightRecords;

    uint64_t lastTupleTimeStampWindow = lazyJoinSinkHelper.windowSize - 1;
    std::vector<Nautilus::Record> tmpRecordsLeft, tmpRecordsRight;

    for (auto i = 0UL, curWindow = 0UL; i < lazyJoinSinkHelper.numberOfTuplesToProduce + 1; ++i) {
        auto recordLeft = Nautilus::Record({{lazyJoinSinkHelper.leftSchema->get(0)->getName(), Value<UInt64>(i)}, {lazyJoinSinkHelper.leftSchema->get(1)->getName(), Value<UInt64>((i % 10) + 10)},
                                            {lazyJoinSinkHelper.leftSchema->get(2)->getName(), Value<UInt64>(i)}});
        auto recordRight = Nautilus::Record({{lazyJoinSinkHelper.rightSchema->get(0)->getName(), Value<UInt64>(i+1000)}, {lazyJoinSinkHelper.rightSchema->get(1)->getName(), Value<UInt64>((i % 10) + 10)},
                                             {lazyJoinSinkHelper.rightSchema->get(2)->getName(), Value<UInt64>(i)}});

        if (recordRight.read(lazyJoinSinkHelper.timeStampField) > lastTupleTimeStampWindow) {
            leftRecords.push_back(std::vector(tmpRecordsLeft.begin(), tmpRecordsLeft.end()));
            rightRecords.push_back(std::vector(tmpRecordsRight.begin(), tmpRecordsRight.end()));

            tmpRecordsLeft = std::vector<Nautilus::Record>();
            tmpRecordsRight = std::vector<Nautilus::Record>();

            lastTupleTimeStampWindow += lazyJoinSinkHelper.windowSize;
        }

        tmpRecordsLeft.emplace_back(recordLeft);
        tmpRecordsRight.emplace_back(recordRight);


        lazyJoinBuildLeft->execute(executionContext, recordLeft);
        lazyJoinBuildRight->execute(executionContext, recordRight);
    }


    auto numberOfEmittedBuffersBuild = lazyJoinOperatorTest->emittedBuffers.size();
    for (auto cnt = 0UL; cnt < numberOfEmittedBuffersBuild; ++cnt) {
        auto tupleBuffer = lazyJoinOperatorTest->emittedBuffers[cnt];
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        lazyJoinSink->open(executionContext, recordBuffer);
    }

    // Delete all buffers that have been emitted from the build phase
    lazyJoinOperatorTest->emittedBuffers.erase(lazyJoinOperatorTest->emittedBuffers.begin(),
                                               lazyJoinOperatorTest->emittedBuffers.begin() + numberOfEmittedBuffersBuild);

    SchemaPtr joinSchema = Schema::create(lazyJoinSinkHelper.leftSchema->getLayoutType());
    joinSchema->addField(lazyJoinSinkHelper.leftSchema->get(lazyJoinSinkHelper.joinFieldNameLeft));
    joinSchema->copyFields(lazyJoinSinkHelper.leftSchema);
    joinSchema->copyFields(lazyJoinSinkHelper.rightSchema);


    /* Checking if all windows have been deleted except for one.
     * We require always one window as we do not know here if we have to take care of more tuples*/
    if (lazyJoinOpHandler->getNumActiveWindows() != 1) {
        NES_ERROR("Not exactly one active window!");
        return false;
    }

    auto removedBuffer = 0UL;
    auto sizeJoinedTuple = joinSchema->getSchemaSizeInBytes();
    auto buffer = lazyJoinSinkHelper.bufferManager->getBufferBlocking();
    auto tuplePerBuffer = lazyJoinSinkHelper.bufferManager->getBufferSize() / sizeJoinedTuple;
    auto mergedEmittedBuffers = Util::mergeBuffersSameWindow(lazyJoinOperatorTest->emittedBuffers,
                                                             joinSchema, lazyJoinSinkHelper.timeStampField,
                                                             lazyJoinSinkHelper.bufferManager, lazyJoinSinkHelper.windowSize);
    auto sortedEmittedBuffers = Util::sortBuffersInTupleBuffer(mergedEmittedBuffers, joinSchema, lazyJoinSinkHelper.timeStampField,
                                                         lazyJoinSinkHelper.bufferManager);

    lazyJoinOperatorTest->emittedBuffers.clear();
    mergedEmittedBuffers.clear();

    for (auto curWindow = 0UL; curWindow < leftRecords.size(); ++curWindow) {
        auto numberOfTuplesInBuffer = 0UL;
        for (auto& leftRecord : leftRecords[curWindow]) {
            for (auto& rightRecord : rightRecords[curWindow]) {
                if (leftRecord.read(lazyJoinSinkHelper.joinFieldNameLeft) == rightRecord.read(lazyJoinSinkHelper.joinFieldNameRight)) {
                    // We expect to have at least one more buffer that was created by our join
                    if (sortedEmittedBuffers.size() == 0) {
                        NES_ERROR("Expected at least one buffer!");
                        return false;
                    }

                    int8_t* bufferPtr = (int8_t*) buffer.getBuffer() + numberOfTuplesInBuffer * sizeJoinedTuple;
                    auto keyRef = Nautilus::Value<Nautilus::MemRef>(bufferPtr);
                    keyRef.store(leftRecord.read(lazyJoinSinkHelper.joinFieldNameLeft));

                    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
                    auto const fieldType = physicalDataTypeFactory.getPhysicalType(lazyJoinSinkHelper.leftSchema->get(lazyJoinSinkHelper.joinFieldNameLeft)->getDataType());
                    bufferPtr += fieldType->size();

                    Util::writeNautilusRecord(bufferPtr, leftRecord, lazyJoinSinkHelper.leftSchema, lazyJoinSinkHelper.bufferManager);
                    Util::writeNautilusRecord(bufferPtr + lazyJoinSinkHelper.leftSchema->getSchemaSizeInBytes(),
                                              rightRecord, lazyJoinSinkHelper.rightSchema, lazyJoinSinkHelper.bufferManager);
                    numberOfTuplesInBuffer += 1;
                    buffer.setNumberOfTuples(numberOfTuplesInBuffer);

                    if (numberOfTuplesInBuffer >= tuplePerBuffer) {
                        std::vector<Runtime::TupleBuffer> bufVec({buffer});
                        auto sortedBuffer = Util::sortBuffersInTupleBuffer(bufVec, joinSchema, lazyJoinSinkHelper.timeStampField,
                                                                           lazyJoinSinkHelper.bufferManager);

                        bool foundBuffer = checkIfBufferFoundAndRemove(sortedEmittedBuffers, sortedBuffer[0], sizeJoinedTuple,
                                                                       joinSchema, removedBuffer);

                        if (!foundBuffer) {
                            NES_ERROR("Could not find buffer " << Util::printTupleBufferAsCSV(buffer, joinSchema) << " in emittedBuffers!");
                            return false;
                        }

                        numberOfTuplesInBuffer = 0;
                        buffer = lazyJoinSinkHelper.bufferManager->getBufferBlocking();
                    }
                }
            }
        }

        if (numberOfTuplesInBuffer > 0) {
            std::vector<Runtime::TupleBuffer> bufVec({buffer});
            auto sortedBuffer = Util::sortBuffersInTupleBuffer(bufVec, joinSchema, lazyJoinSinkHelper.timeStampField,
                                                               lazyJoinSinkHelper.bufferManager);
            bool foundBuffer = checkIfBufferFoundAndRemove(sortedEmittedBuffers, sortedBuffer[0], sizeJoinedTuple,
                                                           joinSchema, removedBuffer);
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

TEST_F(StreamJoinOperatorTest, joinBuildTest) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::UINT64)
                          ->addField("f2_left", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    StreamJoinBuildHelper buildHelper = {
        .pageSize = CHUNK_SIZE,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1UL,
        .joinSizeInByte = 1 * 1024 * 1024UL,
        .windowSize = 1000,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameLeft,
        .bufferManager = bm,
        .schema = leftSchema,
        .timeStampField = timeStampField,
        .lazyJoinOperatorTest = this,
        .isLeftSide = isLeftSide
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
    ASSERT_EQ(emittedBuffers.size(), buildHelper.numPartitions * (buildHelper.numberOfTuplesToProduce / buildHelper.windowSize));
}

TEST_F(StreamJoinOperatorTest, joinBuildTestRight) {

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_right", BasicType::UINT64)
                                ->addField("f2_right", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameRight,
                                                                    timeStampField, rightSchema);

    StreamJoinBuildHelper buildHelper = {
        .pageSize = CHUNK_SIZE,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1UL,
        .joinSizeInByte = 1 * 1024 * 1024UL,
        .windowSize = 1000,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameRight,
        .bufferManager = bm,
        .schema = rightSchema,
        .timeStampField = timeStampField,
        .lazyJoinOperatorTest = this,
        .isLeftSide = isLeftSide
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
    ASSERT_EQ(emittedBuffers.size(), buildHelper.numPartitions * (buildHelper.numberOfTuplesToProduce / buildHelper.windowSize));
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
    auto lazyJoinBuild = std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    StreamJoinBuildHelper buildHelper = {
        .pageSize = leftSchema->getSchemaSizeInBytes() * 2,
        .numPartitions = 1,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1UL,
        .joinSizeInByte = 1 * 1024 * 1024UL,
        .windowSize = 1000,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameLeft,
        .bufferManager = bm,
        .schema = leftSchema,
        .timeStampField = timeStampField,
        .lazyJoinOperatorTest = this,
        .isLeftSide = isLeftSide
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
    ASSERT_EQ(emittedBuffers.size(), buildHelper.numPartitions * (buildHelper.numberOfTuplesToProduce / buildHelper.windowSize));
}

TEST_F(StreamJoinOperatorTest, joinBuildTestMultipleWindows) {

    // Activating and installing error listener
    auto runner = std::make_shared<TestRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto handlerIndex = 0;
    const auto isLeftSide = true;

    auto lazyJoinBuild = std::make_shared<Operators::StreamJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    StreamJoinBuildHelper buildHelper = {
        .pageSize = leftSchema->getSchemaSizeInBytes() * 2,
        .numPartitions = 1,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1UL,
        .joinSizeInByte = 1 * 1024 * 1024UL,
        .windowSize = 5,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameLeft,
        .bufferManager = bm,
        .schema = leftSchema,
        .timeStampField = timeStampField,
        .lazyJoinOperatorTest = this,
        .isLeftSide = isLeftSide
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
    // As we are only building here the left side, we do not emit any buffers
    ASSERT_EQ(emittedBuffers.size(), 0);
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


    StreamJoinSinkHelper lazyJoinSinkHelper {
        .bufferManager = bm,
        .leftSchema = leftSchema,
        .rightSchema = rightSchema,
        .numSourcesLeft = 1,
        .numSourcesRight = 1,
        .joinFieldNameLeft = "f2_left",
        .joinFieldNameRight = "f2_right",
        .timeStampField = "timestamp",
        .pageSize = 2 * leftSchema->getSchemaSizeInBytes(),
        .numPartitions = 2,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1,
        .joinSizeInByte = 1 * 1024 * 1024,
        .windowSize = 20,
        .lazyJoinOperatorTest = this
    };

    ASSERT_TRUE(lazyJoinSinkAndCheck(lazyJoinSinkHelper));
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


    StreamJoinSinkHelper lazyJoinSinkHelper {
        .bufferManager = bm,
        .leftSchema = leftSchema,
        .rightSchema = rightSchema,
        .numSourcesLeft = 1,
        .numSourcesRight = 1,
        .joinFieldNameLeft = "f2_left",
        .joinFieldNameRight = "f2_right",
        .timeStampField = "timestamp",
        .numPartitions = 128,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1,
        .joinSizeInByte = 1 * 1024 * 1024,
        .windowSize = 10,
        .lazyJoinOperatorTest = this
    };

    ASSERT_TRUE(lazyJoinSinkAndCheck(lazyJoinSinkHelper));
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


    StreamJoinSinkHelper lazyJoinSinkHelper {
        .bufferManager = bm,
        .leftSchema = leftSchema,
        .rightSchema = rightSchema,
        .numSourcesLeft = 1,
        .numSourcesRight = 1,
        .joinFieldNameLeft = "f2_left",
        .joinFieldNameRight = "f2_right",
        .timeStampField = "timestamp",
        .numPartitions = 1,
        .numberOfTuplesToProduce = 100UL,
        .numberOfBuffersPerWorker = 128UL,
        .noWorkerThreads = 1,
        .joinSizeInByte = 1 * 1024 * 1024,
        .windowSize = 10,
        .lazyJoinOperatorTest = this
    };

    ASSERT_TRUE(lazyJoinSinkAndCheck(lazyJoinSinkHelper));

}


} // namespace NES::Runtime::Execution::Operators