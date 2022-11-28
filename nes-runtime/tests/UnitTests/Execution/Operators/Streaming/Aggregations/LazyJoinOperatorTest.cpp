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
#include <gtest/gtest.h>

namespace NES::Runtime::Execution {


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
TEST_P(LazyJoinOperatorTest, joinBuildTestRowLayoutSingleThreaded) {
    auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::INT64)
                          ->addField("f2_left", BasicType::INT64)
                          ->addField("timestamp", BasicType::INT64);

    auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_right", BasicType::INT64)
                          ->addField("f2_right", BasicType::INT64)
                          ->addField("timestamp", BasicType::INT64);

    auto leftMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bm->getBufferSize());
    auto rightMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bm->getBufferSize());

    auto joinFieldNameLeft = "f1_left";
    auto joinFieldNameRight = "f1_right";
    auto noWorkerThreads = 1UL;
    auto joinSizeInByte = 1 * 1024 * 1024UL; // 1 GiB
    auto numberOfBuffersPerWorker = 128UL;
    auto numberOfTuplesToProduce = 100UL;

    auto workerContext = std::make_shared<WorkerContext>(/*workerId*/ 0, bm, numberOfBuffersPerWorker);
    auto lazyJoinOpHandler = std::make_shared<LazyJoinOperatorHandler>(leftSchema, rightSchema,
                                                                       joinFieldNameLeft, joinFieldNameRight,
                                                                       noWorkerThreads, noWorkerThreads,
                                                                       noWorkerThreads,
                                                                       joinSizeInByte);

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


    auto opHandlers = pipelineContext.getOperatorHandlers();
    ASSERT_EQ(opHandlers.size(), 1);

    auto handlerIndex = 0;
    auto lazyJoinBuildLeft = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ true, joinFieldNameLeft);
    auto lazyJoinBuildRight = std::make_shared<Operators::LazyJoinBuild>(handlerIndex, /*isLeftSide*/ false, joinFieldNameRight);
    auto lazyJoinSink = std::make_shared<Operators::LazyJoinSink>(handlerIndex);

    // Execute record and thus fill the hash table
    RecordBuffer recordBuffer = RecordBuffer(Value<MemRef>(nullptr));
    for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
        auto recordLeft = Nautilus::Record({{"f1_left", Value<>(i)}, {"f2_left", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
        lazyJoinBuildLeft->execute(executionContext, recordLeft);

        auto recordRight = Nautilus::Record({{"f1_right", Value<>(i)}, {"f2_right", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
        lazyJoinBuildRight->execute(executionContext, recordRight);
    }
    auto recordLeft = Nautilus::Record({{"f1_left", Value<>(numberOfTuplesToProduce)}, {"f2_left", Value<>(numberOfTuplesToProduce % 10)}, {"timestamp", Value<>(0)}});
    lazyJoinBuildLeft->execute(executionContext, recordLeft);

    auto recordRight = Nautilus::Record({{"f1_right", Value<>(numberOfTuplesToProduce)}, {"f2_right", Value<>(numberOfTuplesToProduce % 10)}, {"timestamp", Value<>(0)}});
    lazyJoinBuildRight->execute(executionContext, recordRight);



    // Create here a vector of tuples that contains all of the join tuples
    std::vector<TupleBuffer> joinBuffers;
    for (auto i = 0UL; i < numberOfTuplesToProduce; ++i) {
        auto recordLeft = Nautilus::Record({{"f1_left", Value<>(i)}, {"f2_left", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
        for (auto j = 0UL; j < numberOfTuplesToProduce; ++j) {
            auto recordRight = Nautilus::Record({{"f1_right", Value<>(i)}, {"f2_right", Value<>(i % 10)}, {"timestamp", Value<>(i)}});
            if (recordLeft.read(joinFieldNameLeft) == recordRight.read(joinFieldNameRight)) {
                hier weiter machen und shcauen, ob windows hier überhaupt eine rolle spielen sollen
            }
        }
    }

    // Checking if the tuples have been inserted into the correct bucket

    



}

TEST_P(LazyJoinOperatorTest, joinBuildTestColumnLayoutSingleThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_P(LazyJoinOperatorTest, joinSinkTestRowLayoutSingleThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_P(LazyJoinOperatorTest, joinSinkTestColumnLayoutSingleThreaded) {
    NES_NOT_IMPLEMENTED();
}



TEST_P(LazyJoinOperatorTest, joinBuildTestRowLayoutMultiThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_P(LazyJoinOperatorTest, joinBuildTestColumnLayoutMultiThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_P(LazyJoinOperatorTest, joinSinkTestRowLayoutMultiThreaded) {
    NES_NOT_IMPLEMENTED();
}

TEST_P(LazyJoinOperatorTest, joinSinkTestColumnLayoutMultiThreaded) {
    NES_NOT_IMPLEMENTED();
}

} // namespace NES::Runtime::Execution::Operators