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

#include <cstdint>

#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <BaseUnitTest.hpp>
#include <TestTaskQueue.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include "Util/TestUtil.hpp"


namespace NES
{
namespace Memory::MemoryLayouts
{
class RowLayout;
}

class InputFormatterTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }
    static std::shared_ptr<NES::Memory::BufferManager>
    getBufferManager(const size_t numTasks, const size_t numExpectedResultBuffers, const size_t bufferSize)
    {
        return Memory::BufferManager::create(bufferSize, numTasks + numExpectedResultBuffers);
    }
};

TEST_F(InputFormatterTest, testTaskPipeline)
{
    constexpr size_t NUM_THREADS = 1;
    /// Setup buffers
    constexpr size_t NUM_RESULT_BUFFERS = 1;
    auto testBufferManager = getBufferManager(1, 1, 16);

    using TestTuple = std::tuple<int>;
    SchemaPtr schema = Schema::create()->addField("INT", BasicType::INT32);

    // Fill test tuple buffer with values, which also sets up the expected KV pairs that must be contained in the JSON.
    auto testTupleBuffer = TestUtil::createTestTupleBufferFromTuples(schema, testBufferManager, TestTuple(42));
    NES_DEBUG("test tuple buffer is: {}", testTupleBuffer->toString(schema, true));

    TestTaskQueue taskQueue(NUM_THREADS);
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers
        = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>();
    resultBuffers->reserve(NUM_RESULT_BUFFERS);

    // Todo: introduce helper function to fill buffers for tasks (use TestTupleBuffer convenience functions)
    // auto theTestBuffer = testBufferManager->getBufferBlocking();
    // *theTestBuffer.getBuffer<int>() = 42;

    auto stage1 = std::make_unique<TestablePipelineStage>(
        "step_1",
        [](const Memory::TupleBuffer& tupleBuffer, Runtime::Execution::PipelineExecutionContext& pec)
        {
            auto theNewBuffer = pec.getBufferManager()->getBufferBlocking();
            const auto theOnlyIntInBuffer = *tupleBuffer.getBuffer<int>();
            NES_DEBUG("The only int in buffer: {}", theOnlyIntInBuffer);
            *theNewBuffer.getBuffer<int>() = theOnlyIntInBuffer;
            pec.emitBuffer(theNewBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        });
    auto task1 = std::make_unique<TestablePipelineTask>(testTupleBuffer->getBuffer(), std::move(stage1), testBufferManager, resultBuffers);

    taskQueue.enqueueTask(std::move(task1));

    taskQueue.waitForCompletion();
    ASSERT_FALSE(resultBuffers->empty());
    ASSERT_FALSE(resultBuffers->at(0).empty());
    auto resultBuffer = resultBuffers->at(0).at(0);
    ASSERT_EQ(*resultBuffer.getBuffer<int>(), 42);
}

// TEST_F(InputFormatterTest, testTaskFunctions)
// {
//     TestTaskQueue taskQueue(1);
//
//     // auto eps = std::make_unique<TestExecutablePipelineStage>();
//     auto task1 = std::make_unique<TestablePipelineTask>("Task1");
//     taskQueue.enqueueTask(std::move(task1));
//
//     taskQueue.waitForCompletion();
// }

// Todo: make TBs smaller (two var sized,each 16 bytes (2 * (4 bytes size, 4 bytes index))
// - start with simple test case where variable sized data fits perfectly into buffer
TEST_F(InputFormatterTest, testFormattingEmptyRawBuffer)
{
    // (*testBuffer)[0].writeVarSized("t1", "1234", *bufferManager);
    // (*testBuffer)[1].writeVarSized("t2", "5678", *bufferManager);
    // const auto readVarSizedOne = (*testBuffer)[0].readVarSized("t1");
    // const auto readVarSizedTwo = (*testBuffer)[1].readVarSized("t2");
    // NES_DEBUG("Var sized in buffer is: {}", readVarSizedOne);
    // NES_DEBUG("Var sized in buffer is: {}", readVarSizedTwo);

    // Todo: think about whether to emit tuple or not
    //  - might require mocked TaskQueue or similar

    // Todo:
    // - create InputParserTask that consumes buffer and produces formatted buffer with two UINT64s
    //      - create PipelineExecutionContext <-- required to access OperatorHandler & BufferManager
    //      - create WorkerContext <-- required to emit buffer

    // Query manager has:
    /// folly::MPMCQueue<Task> taskQueue;
    // New task is started like this:
    /// taskQueue.blockingWrite(Task(executable, buffer, getNextTaskId()));
    // Task is executed in runningRoutine which calls:
    /// processNextTask(bool running, WorkerContext& workerContext)
    /// Task task;
    /// if (running)
    /// {
    ///     taskQueue.blockingRead(task);
    /// }
    /// ExecutionResult result = task(workerContext); //<--- actual task execution


    // for (size_t i = 0; i < 10; ++i)
    // {
    //     (*testBuffer)[i].writeVarSized("t1", "" + std::to_string(i) + std::to_string(i), *bufferManager);
    //
    //     const auto readVarSized = (*testBuffer)[i].readVarSized("t1");
    //     NES_DEBUG("Var sized in buffer is: {}", readVarSized);
    // }
    // for (int i = 0; i < 10; i++)
    // {
    //     auto testTuple = std::make_tuple(static_cast<uint64_t>(i));
    //     testBuffer->pushRecordToBuffer(testTuple);
    //     const auto readTupleFromBuffer = (testBuffer->readRecordFromBuffer<uint64_t>(i));
    //     ASSERT_EQ(readTupleFromBuffer, testTuple);
    //     NES_DEBUG("Buffer at {} is {}", i, std::get<uint64_t>(readTupleFromBuffer))
    // }
    // Todo: mock PipelineExecutionContext
    // auto pec = std::make_unique<Runtime::Execution::PipelineExecutionContext>(
    //         PipelineId(1),
    //         QueryId(1),
    //         std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider,
    //         size_t numberOfWorkerThreads,
    //         std::function<void(Memory::TupleBuffer&, WorkerContext&)>&& emitFunctionHandler,
    //         std::function<void(Memory::TupleBuffer&)>&& emitToQueryManagerFunctionHandler,
    //         std::vector<OperatorHandlerPtr> operatorHandlers)
    // auto inputFormatterTask = std::make_unique<InputFormatters::InputFormatterTask>();
}
}
