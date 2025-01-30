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
#include <memory>
#include <API/Schema.hpp>
#include <InputFormatters/InputFormatterOperatorHandler.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <MemoryLayout/RowLayoutField.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <BaseUnitTest.hpp>
#include <TestTaskQueue.hpp>
#include "InputFormatterTestUtil.hpp" //Todo


namespace NES
{

class InputFormatterTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override { BaseUnitTest::TearDown(); }
};


// [x] 1. finish utils refactoring
// [ ] 2. enable binding tasks (here rawBuffers) to workerThreadIds
// [ ] 3. write a decent basis for tests (fix InputFormatter issues) (fix in CSVInputFormatter fix synchronization between Tasks (staging area access, etc.)
// [ ] 4. new design for Informar (chunk/synchronize/parse)
// [ ] 5. multiple stages per task (allow splitting tasks)
// [ ] 6. rigorous testing (improve informar design/fix issues)
TEST_F(InputFormatterTest, testTaskPipelineWithMultipleTasksOneRawByteBuffer)
{
    // Todo: make assignment of tasks to worker ids explicit
    using namespace InputFormatterTestUtil;
    using enum TestDataTypes;
    using TestTuple = std::tuple<int32_t, int32_t>;
    runTest<TestTuple>(TestConfig<TestTuple>{
        .numRequiredBuffers = 2,
        .numThreads = 1,
        .bufferSize = 16,
        .parserConfig = {.parserType = "CSV", .tupleDelimiter = "\n", .fieldDelimiter = ","},
        .testSchema = {INT32, INT32},
        .expectedResults = {{{TestTuple(123456789, 123456789)}}},
        // .expectedResults = {{}, {{TestTuple(123456789, 123456789)}}},
        // Todo: set workerThreadId in TestablePipelineTask? <--- would allow TestTaskQueue to only take tasks
        // Todo: first buffer should not contain starting delimiter <-- we must recognize first buffer in parser
        .rawBytesPerThread = {/* buffer 1 */ {SequenceNumber(1), WorkerThreadId(0), "123456789,123456"}, /* buffer 2 */ {SequenceNumber(2), WorkerThreadId(0), "789"}}});
}
// Todo: Test out of order
// Todo: add third buffer without delimiter in between two buffers with delimiter
// Todo: test with multiple tasks per buffer/thread (split tasks)
// TEST_F(InputFormatterTest, testTaskPipelineWithFunction)
// {
//     constexpr size_t NUM_THREADS = 1;
//     constexpr size_t NUM_TASKS = 1;
//     constexpr size_t NUM_RESULT_BUFFERS = 1;
//     auto testBufferManager = getBufferManager(1, 1, 16);
//     auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUM_RESULT_BUFFERS);
//
//     /// Create and fill buffers for tasks.
//     using TestTuple = std::tuple<int>;
//     SchemaPtr schema = Schema::create()->addField("INT", BasicType::INT32);
//     auto testTupleBuffer = TestUtil::createTestTupleBufferFromTuples(schema, *testBufferManager, TestTuple(42));
//
//     /// create task queue and add tasks to it.
//     TestTaskQueue taskQueue(NUM_THREADS, NUM_TASKS);
//     auto stage1 = std::make_unique<TestablePipelineStage>(
//         "step_1",
//         [](const Memory::TupleBuffer& tupleBuffer, Runtime::Execution::PipelineExecutionContext& pec)
//         {
//             auto theNewBuffer = pec.getBufferManager()->getBufferBlocking();
//             const auto theOnlyIntInBuffer = *tupleBuffer.getBuffer<int>();
//             *theNewBuffer.getBuffer<int>() = theOnlyIntInBuffer;
//             pec.emitBuffer(theNewBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
//         });
//     auto task1
//         = std::make_unique<TestablePipelineTask>(SequenceNumber(0), testTupleBuffer, std::move(stage1), testBufferManager, resultBuffers);
//
//     taskQueue.enqueueTask(std::move(task1));
//
//     taskQueue.waitForCompletion();
//     ASSERT_FALSE(resultBuffers->at(0).empty());
//     auto resultBuffer = resultBuffers->at(0).at(0);
//     ASSERT_EQ(*resultBuffer.getBuffer<int>(), 42);
// }
}
