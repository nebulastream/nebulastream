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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinBuild.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinSink.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Exceptions/ErrorListener.hpp>
#include <gtest/gtest.h>

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


class LazyJoinOperatorTest : public testing::Test, public AbstractPipelineExecutionTest {
  public:
    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("LazyJoinOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        std::cout << "Setup LazyJoinOperatorTest test class." << std::endl;


    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        std::cout << "Setup LazyJoinOperatorTest test case." << std::endl;
        bm = std::make_shared<Runtime::BufferManager>();
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down LazyJoinOperatorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down LazyJoinOperatorTest test class." << std::endl; }

    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::vector<TupleBuffer> emittedBuffers;
};

struct LazyJoinBuildHelper {
    size_t pageSize = CHUNK_SIZE;
    size_t numPartitions = NUM_PARTITIONS;
    size_t numberOfTuplesToProduce;
    size_t numberOfBuffersPerWorker;
    size_t noWorkerThreads;
    size_t joinSizeInByte;
    size_t windowSize;
    Operators::LazyJoinBuildPtr lazyJoinBuild;
    std::string joinFieldName;
    BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::string timeStampField;
    LazyJoinOperatorTest* lazyJoinOperatorTest;
    bool isLeftSide;
};



bool lazyJoinBuildAndCheck(LazyJoinBuildHelper buildHelper) {

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

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bufferManager, numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<LazyJoinOperatorHandler>(schema, schema,
                                                                       joinFieldName, joinFieldName,
                                                                       noWorkerThreads, noWorkerThreads, joinSizeInByte,
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
                auto recordBuffer = Util::getBufferFromNautilus(record, schema, bufferManager);

                NES_DEBUG("\nBucketBuffer: " << Util::printTupleBufferAsCSV(bucketBuffer, schema)
                              << "\nRecordBuffer: " << Util::printTupleBufferAsCSV(recordBuffer, schema));

                if (memcmp(bucketBuffer.getBuffer(), recordBuffer.getBuffer(), schema->getSchemaSizeInBytes()) == 0) {
                    correctlyInserted = true;
                    break;
                }
            }
        }

        if (!correctlyInserted) {
            return false;
        }
    }

    return true;
}



TEST_F(LazyJoinOperatorTest, joinBuildTest) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::UINT64)
                          ->addField("f2_left", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    LazyJoinBuildHelper buildHelper = {
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

TEST_F(LazyJoinOperatorTest, joinBuildTestRight) {

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_right", BasicType::UINT64)
                                ->addField("f2_right", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, isLeftSide, joinFieldNameRight,
                                                                    timeStampField, rightSchema);

    LazyJoinBuildHelper buildHelper = {
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

TEST_F(LazyJoinOperatorTest, joinBuildTestMultiplePagesPerBucket) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto isLeftSide = true;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    LazyJoinBuildHelper buildHelper = {
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

TEST_F(LazyJoinOperatorTest, joinBuildTestMultipleWindows) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto handlerIndex = 0;
    const auto isLeftSide = true;

    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, isLeftSide, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    LazyJoinBuildHelper buildHelper = {
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
    ASSERT_EQ(emittedBuffers.size(), buildHelper.numPartitions * (buildHelper.numberOfTuplesToProduce / buildHelper.windowSize));
}

TEST_F(LazyJoinOperatorTest, joinSinkTest) {

    // Activating and installing error listener
    auto runner = std::make_shared<TestRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::UINT64)
                          ->addField("f2_left", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField("f1_right", BasicType::UINT64)
                           ->addField("f2_right", BasicType::UINT64)
                           ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "timestamp";
    const auto noWorkerThreads = 1UL;
    const auto joinSizeInByte = 1 * 1024 * 1024UL;
    const auto numberOfBuffersPerWorker = 128UL;
    const auto numberOfTuplesToProduce = 100UL;
    const auto windowSize = 10;
    const auto numPartitions = 2;
    const auto numSourcesLeft = 1;
    const auto numSourcesRight = 1;

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<LazyJoinOperatorHandler>(leftSchema, rightSchema,
                                                                       joinFieldNameLeft, joinFieldNameRight,
                                                                       noWorkerThreads,
                                                                       numSourcesLeft + numSourcesRight,
                                                                       joinSizeInByte, windowSize, CHUNK_SIZE, numPartitions);

    auto pipelineContext = PipelineExecutionContext(0,// mock pipeline id
                                                    1, // mock query id
                                                    bm,
                                                    noWorkerThreads,
                                                    [this](TupleBuffer& buffer, Runtime::WorkerContextRef) {
                                                        this->emittedBuffers.emplace_back(std::move(buffer));
                                                    },
                                                    [this](TupleBuffer& buffer) {
                                                        this->emittedBuffers.emplace_back(std::move(buffer));
                                                    },
                                                    {lazyJoinOpHandler});

    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*)workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*)(&pipelineContext)));


    auto handlerIndex = 0UL;
    auto lazyJoinBuildLeft = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);
    auto lazyJoinBuildRight = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ false, joinFieldNameRight,
                                                                    timeStampField, rightSchema);
    auto lazyJoinSink = std::make_shared<Operators::LazyJoinSink>(handlerIndex);



    std::vector<std::vector<Nautilus::Record>> leftRecords(std::ceil((double)numberOfTuplesToProduce / windowSize) + 1);
    std::vector<std::vector<Nautilus::Record>> rightRecords(std::ceil((double)numberOfTuplesToProduce / windowSize) + 1);

    uint64_t lastTupleTimeStampWindow = windowSize - 1;
    std::vector<Nautilus::Record> tmpRecordsLeft, tmpRecordsRight;

    for (auto i = 0UL, curWindow = 0UL; i < numberOfTuplesToProduce + 1; ++i) {
        auto recordLeft = Nautilus::Record({{leftSchema->get(0)->getName(), Value<UInt64>(i)}, {leftSchema->get(1)->getName(), Value<UInt64>((i % 10) + 10)},
                                            {leftSchema->get(2)->getName(), Value<UInt64>(i)}});
        auto recordRight = Nautilus::Record({{rightSchema->get(0)->getName(), Value<UInt64>(i+1000)}, {rightSchema->get(1)->getName(), Value<UInt64>((i % 10) + 10)},
                                             {rightSchema->get(2)->getName(), Value<UInt64>(i)}});

        if (recordRight.read(timeStampField) > lastTupleTimeStampWindow) {
            leftRecords.emplace_back(tmpRecordsLeft);
            rightRecords.emplace_back(tmpRecordsRight);

            tmpRecordsLeft.clear();
            tmpRecordsRight.clear();

            lastTupleTimeStampWindow += windowSize;
        }

        tmpRecordsLeft.emplace_back(recordLeft);
        tmpRecordsRight.emplace_back(recordRight);


        lazyJoinBuildLeft->execute(executionContext, recordLeft);
        lazyJoinBuildRight->execute(executionContext, recordRight);
    }

    NES_DEBUG("Performing the sink step for the lazyJoin for " << emittedBuffers.size() << " buffers!");
    auto numberOfEmittedBuffersBuild = emittedBuffers.size();
    for (auto cnt = 0UL; cnt < numberOfEmittedBuffersBuild; ++cnt) {
        auto tupleBuffer = emittedBuffers[cnt];
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        lazyJoinSink->open(executionContext, recordBuffer);
        NES_DEBUG("emittedBuffers.size() is now " << emittedBuffers.size());
    }

    // Delete all buffers that have been emitted from the build phase
    emittedBuffers.erase(emittedBuffers.begin(), emittedBuffers.begin() + numberOfEmittedBuffersBuild);
    NES_DEBUG("After remove emittedBuffers.size() is now " << emittedBuffers.size());


    /* Checking if all windows have been deleted except for one.
     * We require always one window as we do not know here if we have to take care of more tuples*/
    ASSERT_EQ(lazyJoinOpHandler->getNumActiveWindows(), 1);

    auto removedBuffer = 0;
    for (auto curWindow = 0UL; curWindow < leftRecords.size(); ++curWindow) {
        for (auto& leftRecord : leftRecords[curWindow]) {
            for (auto& rightRecord : rightRecords[curWindow]) {
                if (leftRecord.read(joinFieldNameLeft) == rightRecord.read(joinFieldNameRight)) {
                    // We expect to have at least one more buffer that was created by our join
                    ASSERT_TRUE(emittedBuffers.size() > 0);


                    auto buffer = bm->getBufferBlocking();
                    int8_t* bufferPtr = (int8_t*) buffer.getBuffer();

                    auto keyRef = Nautilus::Value<Nautilus::MemRef>(bufferPtr);
                    keyRef.store(leftRecord.read(joinFieldNameLeft));

                    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();
                    auto const fieldType = physicalDataTypeFactory.getPhysicalType(leftSchema->get(joinFieldNameLeft)->getDataType());
                    bufferPtr += fieldType->size();

                    Util::writeNautilusRecord(bufferPtr, leftRecord, leftSchema, bm);
                    Util::writeNautilusRecord(bufferPtr + leftSchema->getSchemaSizeInBytes(),
                                              rightRecord, rightSchema, bm);
                    buffer.setNumberOfTuples(1);

                    SchemaPtr joinSchema = Schema::create(leftSchema->getLayoutType());
                    joinSchema->addField(leftSchema->get(joinFieldNameLeft));
                    joinSchema->copyFields(leftSchema);
                    joinSchema->copyFields(rightSchema);
                    NES_DEBUG("Created join test buffer = " << Util::printTupleBufferAsCSV(buffer, joinSchema));

                    auto sizeJoinedTuple = joinSchema->getSchemaSizeInBytes();



                    bool foundBuffer;
                    for (auto tupleBufferIt = emittedBuffers.begin(); tupleBufferIt != emittedBuffers.end(); ++tupleBufferIt) {
                        tupleBufferIt->setNumberOfTuples(1);
                        NES_DEBUG("Checking if join test buffer is equal to buffer = " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema));

                        if (memcmp(tupleBufferIt->getBuffer(), buffer.getBuffer(), sizeJoinedTuple) == 0) {
                            foundBuffer = true;
                            NES_DEBUG("Removing buffer #" << removedBuffer << " " << Util::printTupleBufferAsCSV(*tupleBufferIt, joinSchema) << " of size " << sizeJoinedTuple);
                            emittedBuffers.erase(tupleBufferIt);
                            ++removedBuffer;
                            break;
                        }
                    }
                    ASSERT_TRUE(foundBuffer);
                }
            }
        }
    }

    // Make sure that after we have joined all records together
    ASSERT_EQ(emittedBuffers.size(), 0);
}

TEST_F(LazyJoinOperatorTest, joinSinkTestMultipleBuckets) {
    // test here if multiple buckets can be correctly sinked together
    // and this means setting the window size to be larger than the no. tuples and the no. partitions to some large stuff

    NES_NOT_IMPLEMENTED();
}

TEST_F(LazyJoinOperatorTest, joinSinkTestMultipleWindows) {
    // test here if multiple windows can be correctly sinked together
    // and this means setting the windowsize to 10 and then inserting  1000 tuples and testing if the expected output is being
    // returned
    NES_NOT_IMPLEMENTED();

}


} // namespace NES::Runtime::Execution::Operators