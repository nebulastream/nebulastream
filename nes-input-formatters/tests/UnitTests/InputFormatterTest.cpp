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
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Util/TestUtil.hpp>
#include <BaseUnitTest.hpp>
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
    using ResultBufferVector = std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>>;
    ResultBufferVector resultBuffers;
    std::shared_ptr<NES::Memory::BufferManager> testBufferManager;
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;

    struct TestValues
    {
        const size_t numTasks;
        const size_t numThreads;
        const size_t numResultBuffers;
        const uint64_t bufferSize;
        const std::string inputFormatterType;
        const std::string tupleDelimiter;
        const std::string fieldDelimiter;
    };

    static void SetUpTestCase()
    {
        Logger::setupLogging("InputFormatterTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup InputFormatterTest test class.");
    }
    void SetUp() override { BaseUnitTest::SetUp(); }

    void TearDown() override
    {
        /// Let go of all TupleBuffer references owned by the vector holding the results, then destroy the BufferManager.
        resultBuffers->clear();
        operatorHandlers.clear();
        testBufferManager->destroy();
        BaseUnitTest::TearDown();
    }

    static std::shared_ptr<NES::Memory::BufferManager>
    getBufferManager(const size_t numTasks, const size_t numExpectedResultBuffers, const size_t bufferSize)
    {
        return Memory::BufferManager::create(bufferSize, numTasks + numExpectedResultBuffers);
    }


    void setupTest(const TestValues& testValues)
    {
        /// Multiplying result buffers x2, because we want to create one expected buffer, for each result buffer.
        this->resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testValues.numResultBuffers);
        this->operatorHandlers = {std::make_shared<InputFormatterOperatorHandler>()};
        this->testBufferManager = getBufferManager(testValues.numTasks, testValues.numResultBuffers * 2, testValues.bufferSize);
    }

    // Todo: take sequence number!!!!
    std::unique_ptr<TestablePipelineTask>
    createInputFormatterTask(SequenceNumber sequenceNumber, Memory::TupleBuffer taskBuffer, const Schema& schema, const TestValues& testConfig)
    {
        auto inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
            testConfig.inputFormatterType, schema, testConfig.tupleDelimiter, testConfig.fieldDelimiter);
        return std::make_unique<TestablePipelineTask>(sequenceNumber, taskBuffer, std::move(inputFormatter), testBufferManager, resultBuffers);
    }
};

// Todo: test with multiple buffers/tasks
TEST_F(InputFormatterTest, testTaskPipelineWithFunction)
{
    constexpr size_t NUM_THREADS = 1;
    constexpr size_t NUM_RESULT_BUFFERS = 1;
    auto testBufferManager = getBufferManager(1, 1, 16);
    auto resultBuffers = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(NUM_RESULT_BUFFERS);

    /// Create and fill buffers for tasks.
    using TestTuple = std::tuple<int>;
    SchemaPtr schema = Schema::create()->addField("INT", BasicType::INT32);
    auto testTupleBuffer = TestUtil::createTestTupleBufferFromTuples(schema, *testBufferManager, TestTuple(42));

    /// create task queue and add tasks to it.
    TestTaskQueue taskQueue(NUM_THREADS);
    auto stage1 = std::make_unique<TestablePipelineStage>(
        "step_1",
        [](const Memory::TupleBuffer& tupleBuffer, Runtime::Execution::PipelineExecutionContext& pec)
        {
            auto theNewBuffer = pec.getBufferManager()->getBufferBlocking();
            const auto theOnlyIntInBuffer = *tupleBuffer.getBuffer<int>();
            *theNewBuffer.getBuffer<int>() = theOnlyIntInBuffer;
            pec.emitBuffer(theNewBuffer, NES::Runtime::Execution::PipelineExecutionContext::ContinuationPolicy::POSSIBLE);
        });
    auto task1 = std::make_unique<TestablePipelineTask>(SequenceNumber(0), testTupleBuffer->getBuffer(), std::move(stage1), testBufferManager, resultBuffers);

    taskQueue.enqueueTask(std::move(task1));

    taskQueue.waitForCompletion();
    ASSERT_FALSE(resultBuffers->at(0).empty());
    auto resultBuffer = resultBuffers->at(0).at(0);
    ASSERT_EQ(*resultBuffer.getBuffer<int>(), 42);
}

Memory::MemoryLayouts::TestTupleBuffer
createTestTupleBufferFromString(const std::string_view rawData, NES::Memory::TupleBuffer tupleBuffer, std::shared_ptr<Schema> schema)
{
    NES_ASSERT(
        tupleBuffer.getBufferSize() >= rawData.size(),
        fmt::format("{} < {}, size of TupleBuffer is not sufficient to contain string", tupleBuffer.getBufferSize(), rawData.size()));
    std::memcpy(tupleBuffer.getBuffer(), rawData.data(), rawData.size());
    tupleBuffer.setNumberOfTuples(rawData.size());
    auto testTupleBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(std::move(tupleBuffer), std::move(schema));
    return testTupleBuffer;
}

// Todo: use sequence numbers!
TEST_F(InputFormatterTest, testTaskPipeline)
{
    auto testConfig = TestValues{
        .numTasks = 1,
        .numThreads = 1,
        .numResultBuffers = 1,
        .bufferSize = 16,
        .inputFormatterType = "CSV",
        .tupleDelimiter = "\n",
        .fieldDelimiter = ","};
    setupTest(testConfig);
    const SchemaPtr schema = Schema::create()->addField("INT8", BasicType::INT8)->addField("INT8", BasicType::INT8);

    /// Create tuple buffers with raw input data.
    constexpr std::string_view rawData = "1,2\n"
                                         "3,4\n";
    auto testTupleBuffer = createTestTupleBufferFromString(rawData, testBufferManager->getBufferBlocking(), schema);

    /// Create expected results
    using TestTuple = std::tuple<int8_t, int8_t>;
    auto expectedBuffer = TestUtil::createTestTupleBufferFromTuples(schema, *testBufferManager, TestTuple(1, 2), TestTuple(3, 4));

    auto inputFormatterTask = createInputFormatterTask(SequenceNumber(0), testTupleBuffer.getBuffer(), schema, testConfig);
    inputFormatterTask->setOperatorHandlers(operatorHandlers);

    /// create task queue and add tasks to it.
    TestTaskQueue taskQueue(testConfig.numThreads);
    taskQueue.enqueueTask(std::move(inputFormatterTask));
    taskQueue.startProcessing();
    taskQueue.waitForCompletion();

    // Todo: compare correctly (all vectors of the result vector^2 with all expected vectors in the expected vector^2
    ASSERT_FALSE(resultBuffers->at(0).empty());
    auto resultBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(resultBuffers->at(0).at(0), schema);
    ASSERT_TRUE(TestUtil::checkIfBuffersAreEqual(resultBuffer.getBuffer(), expectedBuffer->getBuffer(), schema->getSchemaSizeInBytes()));
}
// Todo: could use
// auto rawData = TestUtil::testTupleBufferToString(*expectedBuffer, schema);
}
