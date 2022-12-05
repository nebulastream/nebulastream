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
    LazyJoinOperatorTest* lazyJoinOperatorTest;
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
    auto& lazyJoinOperatorTest = buildHelper.lazyJoinOperatorTest;

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bufferManager, numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<LazyJoinOperatorHandler>(schema, schema,
                                                                       joinFieldName, joinFieldName,
                                                                       noWorkerThreads, noWorkerThreads,
                                                                       noWorkerThreads, joinSizeInByte,
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
    for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
        auto record = Nautilus::Record({{"f1_left", Value<UInt64>(i)}, {"f2_left", Value<UInt64>((i % 10) + 1)}, {"timestamp", Value<UInt64>(i)}});
        lazyJoinBuild->execute(executionContext, record);

        uint64_t joinKey = record.read(joinFieldName).as<UInt64>().getValue().getValue();
        auto hash = Util::murmurHash(joinKey);
        auto hashTable = lazyJoinOpHandler->getWindowToBeFilled().getLocalHashTable(/* workerThreadId*/ 0, /*isLeftSide*/ true);
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



TEST_F(LazyJoinOperatorTest, joinBuildTestSingleThreaded) {

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::UINT64)
                          ->addField("f2_left", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto noWorkerThreads = 1UL;
    const auto joinSizeInByte = 1 * 1024 * 1024UL;
    const auto numberOfBuffersPerWorker = 128UL;
    const auto numberOfTuplesToProduce = 100UL;
    const auto windowSize = 1000;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    LazyJoinBuildHelper buildHelper = {
        .pageSize = CHUNK_SIZE,
        .numberOfTuplesToProduce = numberOfTuplesToProduce,
        .numberOfBuffersPerWorker = numberOfBuffersPerWorker,
        .noWorkerThreads = noWorkerThreads,
        .joinSizeInByte = joinSizeInByte,
        .windowSize = windowSize,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameLeft,
        .bufferManager = bm,
        .schema = leftSchema,
        .lazyJoinOperatorTest = this
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
}

TEST_F(LazyJoinOperatorTest, joinBuildTestMultiplePagesPerBucket) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto noWorkerThreads = 1UL;
    const auto joinSizeInByte = 1 * 1024 * 1024UL;
    const auto numberOfBuffersPerWorker = 128UL;
    const auto numberOfTuplesToProduce = 100UL;
    const auto windowSize = 1000;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    LazyJoinBuildHelper buildHelper = {
        .pageSize = leftSchema->getSchemaSizeInBytes() * 2,
        .numPartitions = 1,
        .numberOfTuplesToProduce = numberOfTuplesToProduce,
        .numberOfBuffersPerWorker = numberOfBuffersPerWorker,
        .noWorkerThreads = noWorkerThreads,
        .joinSizeInByte = joinSizeInByte,
        .windowSize = windowSize,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameLeft,
        .bufferManager = bm,
        .schema = leftSchema,
        .lazyJoinOperatorTest = this
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
}

TEST_F(LazyJoinOperatorTest, joinBuildTestMultipleWindowSingleThreaded) {
    // test here if multiple windows can be correctly build
    // and this means setting the windowsize to 10 and then inserting  1000 tuples and testing if only the expected records
    // are on the page

    // Activating and installing error listener
    auto runner = std::make_shared<TestRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);

    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                                ->addField("f1_left", BasicType::UINT64)
                                ->addField("f2_left", BasicType::UINT64)
                                ->addField("timestamp", BasicType::UINT64);


    const auto joinFieldNameLeft = "f2_left";
    const auto timeStampField = "timestamp";
    const auto noWorkerThreads = 1UL;
    const auto joinSizeInByte = 1 * 1024 * 1024UL;
    const auto numberOfBuffersPerWorker = 128UL;
    const auto numberOfTuplesToProduce = 100UL;
    const auto windowSize = 10;

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true, joinFieldNameLeft,
                                                                    timeStampField, leftSchema);

    LazyJoinBuildHelper buildHelper = {
        .pageSize = leftSchema->getSchemaSizeInBytes() * 2,
        .numPartitions = 1,
        .numberOfTuplesToProduce = numberOfTuplesToProduce,
        .numberOfBuffersPerWorker = numberOfBuffersPerWorker,
        .noWorkerThreads = noWorkerThreads,
        .joinSizeInByte = joinSizeInByte,
        .windowSize = windowSize,
        .lazyJoinBuild = lazyJoinBuild,
        .joinFieldName = joinFieldNameLeft,
        .bufferManager = bm,
        .schema = leftSchema,
        .lazyJoinOperatorTest = this
    };

    ASSERT_TRUE(lazyJoinBuildAndCheck(buildHelper));
}


TEST_F(LazyJoinOperatorTest, joinSinkTest) {
    const auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::UINT64)
                          ->addField("f2_left", BasicType::UINT64)
                          ->addField("timestamp", BasicType::UINT64);

    const auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                           ->addField("f1_right", BasicType::UINT64)
                           ->addField("f2_right", BasicType::UINT64)
                           ->addField("timestamp", BasicType::UINT64);

    const auto leftMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bm->getBufferSize());

    const auto joinFieldNameLeft = "f2_left";
    const auto joinFieldNameRight = "f2_right";
    const auto timeStampField = "timestamp";
    const auto noWorkerThreads = 1UL;
    const auto joinSizeInByte = 16 * 1024 * 1024UL; // 16 GiB
    const auto numberOfBuffersPerWorker = 128UL;
    const auto numberOfTuplesToProduce = 100UL;
    const auto windowSize = 2;

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<LazyJoinOperatorHandler>(leftSchema, rightSchema,
                                                                       joinFieldNameLeft, joinFieldNameRight,
                                                                       noWorkerThreads, noWorkerThreads,
                                                                       noWorkerThreads,
                                                                       joinSizeInByte, windowSize);

    auto pipelineContext = PipelineExecutionContext( -1,// mock pipeline id
                                                    0, // mock query id
                                                    nullptr,
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

    auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

    for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
        auto recordLeft = Nautilus::Record({{"f1_left", Value<UInt64>(i)}, {"f2_left", Value<UInt64>(i % 10)}, {"timestamp", Value<UInt64>(i)}});
        auto recordRight = Nautilus::Record({{"f1_right", Value<UInt64>(i)}, {"f2_right", Value<UInt64>(i % 10)}, {"timestamp", Value<UInt64>(i)}});

        lazyJoinBuildLeft->execute(executionContext, recordLeft);
        lazyJoinBuildRight->execute(executionContext, recordRight);
    }


    for (auto tupleBuffer : emittedBuffers) {
        RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(tupleBuffer)));
        lazyJoinSink->open(executionContext, recordBuffer);
    }



}

TEST_F(LazyJoinOperatorTest, joinSinkTestMultipleWindows) {
    // test here if multiple windows can be correctly sinked together
    // and this means setting the windowsize to 10 and then inserting  1000 tuples and testing if the expected output is being
    // returned
    NES_NOT_IMPLEMENTED();

}


} // namespace NES::Runtime::Execution::Operators