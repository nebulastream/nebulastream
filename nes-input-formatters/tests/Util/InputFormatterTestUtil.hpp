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

#pragma once

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTask.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/TestUtil.hpp>
#include <TestTaskQueue.hpp>
#include "Util/Logger/Logger.hpp"
#include "Util/TestTupleBuffer.hpp"


namespace NES::InputFormatterTestUtil
{

enum class TestDataTypes : uint8_t
{
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    FLOAT32,
    UINT64,
    FLOAT64,
    BOOLEAN,
    CHAR,
    VARSIZED,
};

struct ThreadInputBuffers
{
    SequenceNumber sequenceNumber;
    std::string rawBytes;
};

struct TaskPackage
{
    SequenceNumber sequenceNumber;
    Memory::TupleBuffer rawByteBuffer;
};

template <typename TupleSchemaTemplate>
struct WorkerThreadResults
{
    std::vector<std::vector<TupleSchemaTemplate>> expectedResultsForThread;
};

template <typename TupleSchemaTemplate>
struct TestConfig
{
    size_t numRequiredBuffers{};
    uint64_t bufferSize{};
    Sources::ParserConfig parserConfig;
    std::vector<TestDataTypes> testSchema;
    /// Each workerThread(vector) can produce multiple buffers(vector) with multiple tuples(vector<TupleSchemaTemplate>)
    std::vector<WorkerThreadResults<TupleSchemaTemplate>> expectedResults;
    std::vector<ThreadInputBuffers> rawBytesPerThread;
    using TupleSchema = TupleSchemaTemplate;
};

Schema createSchema(const std::vector<TestDataTypes>& testDataTypes);

/// Creates an emit function that places buffers into 'resultBuffers' when there is data.
std::function<void(OriginId, Sources::SourceReturnType::SourceReturnType)>
getEmitFunction(std::vector<NES::Memory::TupleBuffer>& resultBuffers);

Sources::ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig);

std::unique_ptr<Sources::SourceHandle> createFileSource(
    const std::string& filePath,
    Schema schema,
    std::shared_ptr<Memory::BufferManager> sourceBufferPool,
    int numberOfLocalBuffersInSource);

std::shared_ptr<InputFormatters::InputFormatterTask> createInputFormatterTask(const Schema& schema);

/// Waits until source reached EoS
void waitForSource(const std::vector<NES::Memory::TupleBuffer>& resultBuffers, size_t numExpectedBuffers);

/// Compares two files and returns true if they are equal on a byte level.
bool compareFiles(const std::filesystem::path& file1, const std::filesystem::path& file2);

TestablePipelineTask createInputFormatterTask(
    SequenceNumber sequenceNumber,
    WorkerThreadId workerThreadId,
    Memory::TupleBuffer taskBuffer,
    std::shared_ptr<InputFormatters::InputFormatterTask> inputFormatterTask);

template <typename TupleSchemaTemplate>
struct TestHandle
{
    TestConfig<TupleSchemaTemplate> testConfig;
    std::shared_ptr<Memory::BufferManager> testBufferManager;
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers;
    Schema schema;
    std::unique_ptr<SingleThreadedTestTaskQueue> testTaskQueue;
    std::vector<TaskPackage> inputBuffers;
    std::vector<std::vector<Memory::TupleBuffer>> expectedResultVectors;

    void destroy()
    {
        inputBuffers.clear();
        expectedResultVectors.clear();
        resultBuffers->clear();
        testTaskQueue.reset();
        testBufferManager->destroy();
    }
};

/// Gets the actual result buffers and the expected result buffers from the test handle and compares them.
/// Logs both the actual and the expected buffers if 'PrintDebug' is set to true.
template <typename TupleSchemaTemplate, bool PrintDebug>
bool validateResult(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    /// check that vectors of vectors contain the same number of vectors.
    bool isValid = true;
    isValid &= (testHandle.resultBuffers->size() == testHandle.expectedResultVectors.size());

    /// iterate over all vectors in the actual results
    for (size_t taskIndex = 0; const auto& actualResultVector : *testHandle.resultBuffers)
    {
        /// check that the corresponding vector in the vector of vector containing the expected results is of the same size
        isValid &= (actualResultVector.size() == testHandle.expectedResultVectors[taskIndex].size());
        /// iterate over all buffers in the vector containing the actual results and compare the buffer with the corresponding buffers
        /// in the expected results.
        for (size_t bufferIndex = 0; const auto& actualResultBuffer : actualResultVector)
        {
            if (PrintDebug)
            {
                /// If specified, print the contents of the buffers.
                auto actualResultTestBuffer
                    = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(actualResultBuffer, testHandle.schema);
                actualResultTestBuffer.setNumberOfTuples(actualResultBuffer.getNumberOfTuples());
                auto expectedTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(
                    testHandle.expectedResultVectors[taskIndex][bufferIndex], testHandle.schema);
                expectedTestBuffer.setNumberOfTuples(expectedTestBuffer.getNumberOfTuples());
                NES_DEBUG(
                    "\n Actual result buffer:\n{} Expected result buffer:\n{}",
                    actualResultTestBuffer.toString(
                        testHandle.schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_IN_NEWLINE),
                    expectedTestBuffer.toString(
                        testHandle.schema, Memory::MemoryLayouts::TestTupleBuffer::PrintMode::NO_HEADER_END_IN_NEWLINE));
            }
            isValid &= TestUtil::checkIfBuffersAreEqual(
                actualResultBuffer, testHandle.expectedResultVectors[taskIndex][bufferIndex], testHandle.schema.getSchemaSizeInBytes());
            ++bufferIndex;
        }
        ++taskIndex;
    }
    return isValid;
}

template <typename TupleSchemaTemplate, bool PrintDebug>
std::vector<std::vector<Memory::TupleBuffer>> createExpectedResults(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    std::vector<std::vector<Memory::TupleBuffer>> expectedTupleBuffers(1);
    for (const auto workerThreadResultVector : testHandle.testConfig.expectedResults)
    {
        /// expectedBuffersVector: vector<TupleSchemaTemplate>
        for (const auto& expectedBuffersVector : workerThreadResultVector.expectedResultsForThread)
        {
            expectedTupleBuffers.at(0).emplace_back(TestUtil::createTupleBufferFromTuples<TupleSchemaTemplate, false, PrintDebug>(
                testHandle.schema, *testHandle.testBufferManager, expectedBuffersVector));
        }
    }
    return expectedTupleBuffers;
}

template <typename TupleSchemaTemplate>
TestHandle<TupleSchemaTemplate> setupTest(const TestConfig<TupleSchemaTemplate>& testConfig)
{
    std::shared_ptr<Memory::BufferManager> testBufferManager
        = Memory::BufferManager::create(testConfig.bufferSize, 2 * testConfig.numRequiredBuffers);
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers
        = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(1);
    Schema schema = createSchema(testConfig.testSchema);
    return {
        testConfig,
        testBufferManager,
        resultBuffers,
        std::move(schema),
        std::make_unique<SingleThreadedTestTaskQueue>(testBufferManager, resultBuffers),
        {},
        {}};
}

template <typename TupleSchemaTemplate>
std::vector<TestablePipelineTask> createTasks(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
        testHandle.testConfig.parserConfig.parserType,
        testHandle.schema,
        testHandle.testConfig.parserConfig.tupleDelimiter,
        testHandle.testConfig.parserConfig.fieldDelimiter);
    const auto inputFormatterTask = std::make_shared<InputFormatters::InputFormatterTask>(OriginId(1), std::move(inputFormatter));
    std::vector<TestablePipelineTask> tasks;
    tasks.reserve(testHandle.inputBuffers.size());
    for (const auto& inputBuffer : testHandle.inputBuffers)
    {
        tasks.emplace_back(
            createInputFormatterTask(inputBuffer.sequenceNumber, WorkerThreadId(0), inputBuffer.rawByteBuffer, inputFormatterTask));
    }
    return tasks;
}

template <typename TupleSchemaTemplate>
std::vector<TaskPackage> createTestTupleBuffers(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    std::vector<TaskPackage> rawTupleBuffers;
    for (const auto& rawInputBuffer : testHandle.testConfig.rawBytesPerThread)
    {
        auto tupleBuffer = testHandle.testBufferManager->getBufferNoBlocking();
        INVARIANT(tupleBuffer, "Couldn't get buffer from bufferManager. Configure test to use more buffers.");
        rawTupleBuffers.emplace_back(TaskPackage{
            rawInputBuffer.sequenceNumber, TestUtil::copyStringDataToTupleBuffer(rawInputBuffer.rawBytes, std::move(tupleBuffer.value()))});
    }
    return rawTupleBuffers;
}

template <typename TupleSchemaTemplate, bool PrintDebug = false>
void runTest(const TestConfig<TupleSchemaTemplate>& testConfig)
{
    /// setup buffer manager, container for results, schema, operator handlers, and the task queue
    auto testHandle = setupTest<TupleSchemaTemplate>(testConfig);
    /// fill input tuple buffers with raw data
    testHandle.inputBuffers = createTestTupleBuffers(testHandle);
    /// create tasks for task queue
    auto tasks = createTasks(testHandle);
    /// process tasks in task queue
    testHandle.testTaskQueue->processTasks(std::move(tasks));
    /// create expected results from supplied in test config
    testHandle.expectedResultVectors = createExpectedResults<TupleSchemaTemplate, PrintDebug>(testHandle);
    /// validate: actual results vs expected results
    const auto validationResult = validateResult<TupleSchemaTemplate, PrintDebug>(testHandle);
    ASSERT_TRUE(validationResult);
    /// clean up
    testHandle.destroy();
}

}
