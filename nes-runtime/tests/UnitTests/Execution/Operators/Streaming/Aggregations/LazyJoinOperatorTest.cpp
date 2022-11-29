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



/**
 * @brief tests a simple join with two sources in a columnar layout with one thread for both sides
 */
TEST_F(LazyJoinOperatorTest, joinBuildTestRowLayoutSingleThreaded) {
    // Activating and installing error listener
    auto runner = std::make_shared<TestRunner>();
    NES::Exceptions::installGlobalErrorListener(runner);



    auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::INT64)
                          ->addField("f2_left", BasicType::INT64)
                          ->addField("timestamp", BasicType::INT64);

    auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_right", BasicType::INT64)
                          ->addField("f2_right", BasicType::INT64)
                          ->addField("timestamp", BasicType::INT64);

    auto leftMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bm->getBufferSize());

    auto joinFieldNameLeft = "f1_left";
    auto joinFieldNameRight = "f1_right";
    auto noWorkerThreads = 1UL;
    auto joinSizeInByte = 16 * 1024 * 1024UL; // 16 GiB
    auto numberOfBuffersPerWorker = 128UL;
    auto numberOfTuplesToProduce = 100UL;
    auto windowSize = 1000;

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

    auto handlerIndex = 0;
    auto lazyJoinBuild = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true, joinFieldNameLeft);


    // Execute record and thus fill the hash table
    for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
        auto record = Nautilus::Record({{"f1_left", Value<>(i)}, {"f2_left", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
        lazyJoinBuild->execute(executionContext, record);

        auto hash = Util::murmurHash(record.read(joinFieldNameLeft));
        auto hashTable = lazyJoinOpHandler->getCurrentWindow().getLocalHashTable(/* workerThreadId*/ 0, /*isLeftSide*/ true);
        auto bucket = hashTable.getBucketLinkedList(hash & MASK);

        bool found = false;
        for (auto page : bucket->getPages()) {
            for (auto k = 0UL; k < page->size(); ++k) {
                uint8_t* recordPtr = page->operator[](k);
                auto bucketBuffer = Util::getRecordFromPointer(recordPtr, leftSchema);
                auto recordBuffer = Util::getBufferFromNautilus(record, leftSchema);

                if (memcmp(bucketBuffer.getBuffer(), recordBuffer.getBuffer(), leftSchema->getSchemaSizeInBytes()) == 0) {
                    found = true;
                    break;
                }
            }
        }
        ASSERT_TRUE(found);
    }
}

TEST_F(LazyJoinOperatorTest, joinBuildTestColumnLayoutSingleThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_F(LazyJoinOperatorTest, joinSinkTestRowLayoutSingleThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_F(LazyJoinOperatorTest, joinSinkTestColumnLayoutSingleThreaded) {
    NES_NOT_IMPLEMENTED();
}


} // namespace NES::Runtime::Execution::Operators