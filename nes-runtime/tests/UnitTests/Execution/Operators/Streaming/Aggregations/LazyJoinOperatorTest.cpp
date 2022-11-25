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
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinBuild.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinSink.hpp>
#include <Execution/Operators/Streaming/Aggregations/Join/LazyJoinOperatorHandler.hpp>
#include <Execution/Pipelines/ExecutablePipelineProvider.hpp>
#include <Runtime/WorkerContext.hpp>
#include <TestUtils/AbstractPipelineExecutionTest.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/BufferManager.hpp>
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
        provider = ExecutablePipelineProviderRegistry::getPlugin(this->GetParam()).get();
        bm = std::make_shared<Runtime::BufferManager>();
        wc = std::make_shared<WorkerContext>(0, bm, 100);
    }

    /* Will be called before a test is executed. */
    void TearDown() override { std::cout << "Tear down LazyJoinOperatorTest test case." << std::endl; }

    /* Will be called after all tests in this class are finished. */
    static void TearDownTestCase() { std::cout << "Tear down LazyJoinOperatorTest test class." << std::endl; }

    ExecutablePipelineProvider* provider;
    std::shared_ptr<Runtime::BufferManager> bm;
    std::shared_ptr<WorkerContext> wc;
};



/**
 * @brief tests a simple join with two sources
 */
TEST_P(LazyJoinOperatorTest, simpleJoinTest) {
    auto leftSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_left", BasicType::INT64)
                          ->addField("f2_left", BasicType::INT64)
                          ->addField("ts_left", BasicType::INT64);

    auto rightSchema = Schema::create(Schema::MemoryLayoutType::ROW_LAYOUT)
                          ->addField("f1_right", BasicType::INT64)
                          ->addField("f2_right", BasicType::INT64)
                          ->addField("ts_right", BasicType::INT64);

    auto leftMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(leftSchema, bm->getBufferSize());
    auto rightMemoryLayout = Runtime::MemoryLayouts::RowLayout::create(rightSchema, bm->getBufferSize());

    auto joinFieldNameLeft = "f1_left";
    auto joinFieldNameRight = "f1_right";
    auto maxNoWorkerThreadsLeft = 1;
    auto maxNoWorkerThreadsRight = 1;
    auto joinSizeInByte = 1 * 1024 * 1024; // 1 GiB

    auto lazyJoinOpHandler = std::make_shared<LazyJoinOperatorHandler>(leftSchema, rightSchema, joinFieldNameLeft, joinFieldNameRight,
                                                                       maxNoWorkerThreadsLeft, maxNoWorkerThreadsRight,
                                                                       maxNoWorkerThreadsLeft + maxNoWorkerThreadsRight,
                                                                       joinSizeInByte);

    auto lazyJoinBuildLeft = std::make_shared<Operators::LazyJoinBuild>();

}



} // namespace NES::Runtime::Execution::Operators