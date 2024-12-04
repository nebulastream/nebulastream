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
#include <Util/Logger/Logger.hpp>
#include <BaseUnitTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <TestUtils/TestTaskQueue.hpp>
#include <TestTaskQueue.hpp>


namespace NES
{
namespace Memory::MemoryLayouts
{
class RowLayout;
}
class InputFormatterTest : public Testing::BaseUnitTest
{
public:
    Memory::BufferManagerPtr bufferManager;
    SchemaPtr schema;
    std::unique_ptr<Memory::MemoryLayouts::TestTupleBuffer> testBuffer;

    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        bufferManager = Memory::BufferManager::create(16, 10);

        // schema = Schema::create()->addField("t1", BasicType::UINT16)->addField("t2", BasicType::BOOLEAN)->addField("t3", BasicType::FLOAT64);
        // schema = Schema::create()->addField("t1", BasicType::UINT64);
        schema = Schema::create()->addField("t1", DataTypeFactory::createVariableSizedData())->addField("t2", DataTypeFactory::createVariableSizedData());
        std::shared_ptr<NES::Memory::MemoryLayouts::RowLayout> layout;
        ASSERT_NO_THROW(layout = NES::Memory::MemoryLayouts::RowLayout::create(schema, bufferManager->getBufferSize()));
        ASSERT_NE(layout, nullptr);

        auto tupleBuffer = bufferManager->getBufferBlocking();
        testBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(layout, tupleBuffer);

    }
};

TEST_F(InputFormatterTest, testMockedTaskQueue)
{

    TestTaskQueue taskQueue(1);  // Single thread for deterministic order

    // Todo: TestableTask should mock the actual Tasks in `Task.hpp`, which are NOT exposed! Neither are the RunningQueryPlanNodes.
    // -> alternatively, implement in the QueryEngine to expose
    // The RunningQueryPlanNodes is essentially a wrapper around: 'std::unique_ptr<Execution::ExecutablePipelineStage>'
    // -> thus, it suffices to create another wrapper around an ExecutablePipelineStage
    auto task1 = std::make_unique<TestableTask>("Task1");
    task1->addStep("step1", []{ std::cout << "hello from thread 1\n"; });

    auto task2 = std::make_unique<TestableTask>("Task2");
    task2->addStep("step1", []{ std::cout << "hello from thread 2\n"; });

    // Add tasks in desired order
    taskQueue.enqueueTask(std::move(task1));
    taskQueue.enqueueTask(std::move(task2));

    taskQueue.waitForCompletion();
}

// Todo: make TBs smaller (two var sized,each 16 bytes (2 * (4 bytes size, 4 bytes index))
// - start with simple test case where variable sized data fits perfectly into buffer
TEST_F(InputFormatterTest, testFormattingEmptyRawBuffer)
{
    (*testBuffer)[0].writeVarSized("t1", "1234", *bufferManager);
    (*testBuffer)[1].writeVarSized("t2", "5678", *bufferManager);
    const auto readVarSizedOne = (*testBuffer)[0].readVarSized("t1");
    const auto readVarSizedTwo = (*testBuffer)[1].readVarSized("t2");
    NES_DEBUG("Var sized in buffer is: {}", readVarSizedOne);
    NES_DEBUG("Var sized in buffer is: {}", readVarSizedTwo);

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
