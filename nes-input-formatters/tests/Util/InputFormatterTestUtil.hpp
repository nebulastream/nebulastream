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

#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/TestUtil.hpp>
#include <TestTaskQueue.hpp>

#pragma once

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

std::shared_ptr<Schema> createSchema(std::vector<TestDataTypes> TestDataTypes)
{
    std::shared_ptr<Schema> schema = std::make_shared<Schema>();
    for (size_t fieldNumber = 1; const auto& dataType : TestDataTypes)
    {
        switch (dataType)
        {
            case TestDataTypes::UINT8:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createUInt8());
                break;
            case TestDataTypes::UINT16:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createUInt16());
                break;
            case TestDataTypes::UINT32:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createUInt32());
                break;
            case TestDataTypes::UINT64:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createUInt64());
                break;
            case TestDataTypes::INT8:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createInt8());
                break;
            case TestDataTypes::INT16:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createInt16());
                break;
            case TestDataTypes::INT32:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createInt32());
                break;
            case TestDataTypes::INT64:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createInt64());
                break;
            case TestDataTypes::FLOAT32:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createFloat());
                break;
            case TestDataTypes::FLOAT64:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createDouble());
                break;
            case TestDataTypes::BOOLEAN:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createBoolean());
                break;
            case TestDataTypes::CHAR:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createChar());
                break;
            case TestDataTypes::VARSIZED:
                schema->addField("Field_" + std::to_string(fieldNumber), DataTypeFactory::createVariableSizedData());
                break;
        }
        ++fieldNumber;
    }
    return schema;
}

struct ThreadInputBuffers
{
    const SequenceNumber sequenceNumber;
    const WorkerThreadId workerThreadId;
    const std::string rawBytes;
};

struct TaskPackage
{
    const SequenceNumber sequenceNumber;
    const WorkerThreadId workerThreadId;
    const Memory::TupleBuffer rawByteBuffer;
};

template <typename TupleSchemaTemplate>
struct WorkerThreadResults
{
    const WorkerThreadId::Underlying workerThreadId;
    const std::vector<std::vector<TupleSchemaTemplate>> expectedResultsForThread;
};

template <typename TupleSchemaTemplate>
struct TestConfig
{
    const size_t numRequiredBuffers;
    const size_t numThreads;
    const uint64_t bufferSize;
    const Sources::ParserConfig parserConfig;
    const std::vector<TestDataTypes> testSchema;
    /// Each workerThread(vector) can produce multiple buffers(vector) with multiple tuples(vector<TupleSchemaTemplate>)
    const std::vector<WorkerThreadResults<TupleSchemaTemplate>> expectedResults;
    const std::vector<ThreadInputBuffers> rawBytesPerThread; //Todo: rename
    using TupleSchema = TupleSchemaTemplate;
};

template <typename TupleSchemaTemplate>
struct TestHandle
{
    TestConfig<TupleSchemaTemplate> testConfig;
    std::shared_ptr<Memory::BufferManager> testBufferManager;
    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers;
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;
    std::shared_ptr<Schema> schema;
    TestTaskQueue testTaskQueue;
    std::vector<TaskPackage> inputBuffers;
    std::vector<std::vector<Memory::TupleBuffer>> expectedResultVectors;

    void destroy()
    {
        inputBuffers.clear();
        expectedResultVectors.clear();
        resultBuffers->clear();
        operatorHandlers.clear();
        testBufferManager->destroy();
        schema->clear();
    }
};

template <typename TupleSchemaTemplate>
TestablePipelineTask createInputFormatterTask(
    const TestHandle<TupleSchemaTemplate>& testHandle,
    const SequenceNumber sequenceNumber,
    const WorkerThreadId workerThreadId,
    Memory::TupleBuffer taskBuffer,
    std::shared_ptr<InputFormatters::InputFormatterTask> inputFormatterTask)
{
    taskBuffer.setSequenceNumber(sequenceNumber);
    return TestablePipelineTask(
        workerThreadId,
        sequenceNumber,
        taskBuffer,
        std::move(inputFormatterTask),
        testHandle.operatorHandlers);
}

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
                auto actualResultTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(actualResultBuffer, testHandle.schema);
                actualResultTestBuffer.setNumberOfTuples(actualResultBuffer.getNumberOfTuples());
                auto expectedTestBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(testHandle.expectedResultVectors[taskIndex][bufferIndex], testHandle.schema);
                expectedTestBuffer.setNumberOfTuples(expectedTestBuffer.getNumberOfTuples());
                NES_DEBUG("\n Actual result buffer:\n{} Expected result buffer:\n{}", actualResultTestBuffer.toString(testHandle.schema, false), expectedTestBuffer.toString(testHandle.schema, false));
            }
            isValid &= TestUtil::checkIfBuffersAreEqual(
                actualResultBuffer, testHandle.expectedResultVectors[taskIndex][bufferIndex], testHandle.schema->getSchemaSizeInBytes());
            ++bufferIndex;
        }
        ++taskIndex;
    }
    return isValid;
}

template <typename TupleSchemaTemplate>
std::vector<std::vector<Memory::TupleBuffer>> createExpectedResults(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    std::vector<std::vector<Memory::TupleBuffer>> expectedTupleBuffers(testHandle.testConfig.numThreads);
    for (const auto workerThreadResultVector : testHandle.testConfig.expectedResults)
    {
        /// expectedBuffersVector: vector<TupleSchemaTemplate>
        for (const auto& expectedBuffersVector : workerThreadResultVector.expectedResultsForThread)
        {
            expectedTupleBuffers.at(workerThreadResultVector.workerThreadId)
                .emplace_back(
                    TestUtil::createTestTupleBufferFromTuples<TupleSchemaTemplate, false, true>(
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
        = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(testConfig.numThreads);
    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers = {std::make_shared<InputFormatters::InputFormatterProvider::InputFormatterOperatorHandler>()};
    std::shared_ptr<Schema> schema = createSchema(testConfig.testSchema);
    return {
        testConfig,
        testBufferManager,
        resultBuffers,
        std::move(operatorHandlers),
        std::move(schema),
        TestTaskQueue(testConfig.numThreads, testBufferManager, resultBuffers),
        {},
        {}};
}

template <typename TupleSchemaTemplate>
std::vector<TestablePipelineTask> createTasks(const TestHandle<TupleSchemaTemplate>& testHandle)
{

    // Todo: can we move the testBufferManagerHere?
    // Todo: refactor InputFormatterProvider to take parser config instead of individual parameters
    std::unique_ptr<InputFormatters::InputFormatter> inputFormatter = InputFormatters::InputFormatterProvider::provideInputFormatter(
        testHandle.testConfig.parserConfig.parserType,
        testHandle.schema,
        testHandle.testConfig.parserConfig.tupleDelimiter,
        testHandle.testConfig.parserConfig.fieldDelimiter);
    const auto inputFormatterTask = std::make_shared<InputFormatters::InputFormatterTask>(std::move(inputFormatter));
    std::vector<TestablePipelineTask> tasks;
    for (const auto& inputBuffer : testHandle.inputBuffers)
    {
        // Todo: allow to configure sequence numbers
        tasks.emplace_back(
            createInputFormatterTask<TupleSchemaTemplate>(
                testHandle, inputBuffer.sequenceNumber, inputBuffer.workerThreadId, inputBuffer.rawByteBuffer, inputFormatterTask));
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
        rawTupleBuffers.emplace_back(
            TaskPackage{
                rawInputBuffer.sequenceNumber,
                rawInputBuffer.workerThreadId,
                TestUtil::createTestTupleBufferFromString(rawInputBuffer.rawBytes, std::move(tupleBuffer.value()))});
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
    // Todo: we need to flush, to get the last buffers
    testHandle.testTaskQueue.processTasks(std::move(tasks));
    /// create expected results from supplied in test config
    testHandle.expectedResultVectors = createExpectedResults<TupleSchemaTemplate>(testHandle);
    /// validate: actual results vs expected results
    // auto intPtr = testHandle.resultBuffers->at(0).at(0).template getBuffer<int32_t>();
    const auto validationResult = validateResult<TupleSchemaTemplate, PrintDebug>(testHandle);
    ASSERT_TRUE(validationResult);
    /// clean up
    testHandle.destroy();
}
}