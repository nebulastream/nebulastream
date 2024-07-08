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

#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ReorderTupleBuffersOperator.hpp>
#include <Execution/Operators/ReorderTupleBuffersOperatorHandler.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <random>

namespace NES::Runtime::Execution {

class ReorderTupleBuffersOperatorPipelineExecutionContext : public PipelineExecutionContext {
  public:
    ReorderTupleBuffersOperatorPipelineExecutionContext(
        Operators::ReorderTupleBuffersOperatorHandlerPtr reorderTupleBuffersOperatorHandler,
        BufferManagerPtr bm,
        std::function<void(TupleBuffer&, WorkerContextRef)>&& emitFunction)
        : PipelineExecutionContext(INVALID_PIPELINE_ID,             // mock pipeline id
                                   INVALID_DECOMPOSED_QUERY_PLAN_ID,// mock query id
                                   bm,
                                   1,
                                   std::move(emitFunction),
                                   [](TupleBuffer&) {
                                   },
                                   {reorderTupleBuffersOperatorHandler}) {}
};

class ReorderTupleBuffersOperatorTest : public Testing::BaseUnitTest {
  public:
    std::shared_ptr<Operators::ReorderTupleBuffersOperator> reorderTupleBuffersOperator;
    std::shared_ptr<Operators::ReorderTupleBuffersOperatorHandler> reorderTupleBuffersOperatorHandler;
    std::shared_ptr<Runtime::BufferManager> bm;

    /* Will be called before any test in this class are executed. */
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReorderTupleBuffersOperatorTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ReorderTupleBuffersOperatorTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        BaseUnitTest::SetUp();
        NES_INFO("Setup ReorderTupleBuffersOperatorTest test case.");
        constexpr auto operatorHandlerIndex = 0;
        reorderTupleBuffersOperator = std::make_shared<Operators::ReorderTupleBuffersOperator>(operatorHandlerIndex);
        reorderTupleBuffersOperatorHandler = std::make_shared<Operators::ReorderTupleBuffersOperatorHandler>();
        bm = std::make_shared<BufferManager>(8196, 5000);
    }
};

/**
 * Check that reorderTupleBuffer operator emits buffers in order
*/
TEST_F(ReorderTupleBuffersOperatorTest, checkOrderInReorderTupleBuffersOperator) {
    const auto numberOfTupleBuffers = 1000;

    // 1. Set mock emit lambda to be able to check order of emitted buffers
    std::vector<TupleBuffer> emittedBuffers;
    auto emitLambda = [&emittedBuffers](TupleBuffer& buffer, Runtime::WorkerContextRef) {
        emittedBuffers.emplace_back(buffer);
    };

    // 2. Create mocked pipeline context
    ReorderTupleBuffersOperatorPipelineExecutionContext pipelineContext(reorderTupleBuffersOperatorHandler, bm, emitLambda);
    auto workerContext = std::make_shared<WorkerContext>(INITIAL<WorkerThreadId>, bm, 100);
    auto executionContext = ExecutionContext(Nautilus::Value<Nautilus::MemRef>((int8_t*) workerContext.get()),
                                             Nautilus::Value<Nautilus::MemRef>((int8_t*) (&pipelineContext)));

    reorderTupleBuffersOperator->setup(executionContext);

    // 3. Create buffers
    std::vector<TupleBuffer> expectedBuffers;
    for (auto i = 0; i < numberOfTupleBuffers; i++) {
        auto newTupleBuffer = bm->getBufferBlocking();
        newTupleBuffer.setSequenceNumber(i + 1);
        expectedBuffers.emplace_back(newTupleBuffer);
    }
    // 4. Shuffle buffers
    std::random_device r;
    auto rng = std::default_random_engine(r());
    std::shuffle(expectedBuffers.begin(), expectedBuffers.end(), rng);

    // 5. Go over buffer and put into migrate operator
    for (auto i = 0; i < numberOfTupleBuffers; i++) {
        auto record = RecordBuffer(Value<MemRef>((int8_t*) std::addressof(expectedBuffers[i])));
        reorderTupleBuffersOperator->open(executionContext, record);
    }

    // 6. Check that buffer are emitted in order, starting from 1
    for (auto i = 1; i <= numberOfTupleBuffers; i++) {
        EXPECT_EQ(emittedBuffers[i - 1].getSequenceNumber(), i);
    }
}
}// namespace NES::Runtime::Execution
