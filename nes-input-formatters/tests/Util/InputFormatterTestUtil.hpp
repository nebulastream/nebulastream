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
#include <numeric>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatterProvider.hpp>
#include <InputFormatters/InputFormatterTaskPipeline.hpp>
#include <MemoryLayout/RowLayout.hpp>
#include <Runtime/BufferManager.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <TestTaskQueue.hpp>


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
    uint64_t sizeOfRawBuffers{};
    uint64_t sizeOfFormattedBuffers{};
    ParserConfig parserConfig;
    std::vector<TestDataTypes> testSchema;
    /// Each workerThread(vector) can produce multiple buffers(vector) with multiple tuples(vector<TupleSchemaTemplate>)
    std::vector<WorkerThreadResults<TupleSchemaTemplate>> expectedResults;
    std::vector<ThreadInputBuffers> rawBytesPerThread;
    using TupleSchema = TupleSchemaTemplate;
};


template <typename T>
class ThreadSafeVector
{
    std::mutex mtx;
    std::condition_variable_any condition;
    std::vector<T> vector;

public:
    void reserve(size_t reservationSize)
    {
        std::scoped_lock lock(mtx);
        vector.reserve(reservationSize);
    }

    void modifyBuffer(std::function<void(std::vector<T>&)> fn)
    {
        std::scoped_lock lock(mtx);
        fn(vector);
        condition.notify_one();
    }

    template <typename U = T>
    void emplace_back(U&& item)
    {
        {
            std::scoped_lock lock(mtx);
            vector.emplace_back(std::forward<U>(item));
        }
        condition.notify_one();
    }

    size_t size()
    {
        std::scoped_lock lock(mtx);
        return vector.size();
    }

    void waitForSize(size_t size)
    {
        std::unique_lock lock(mtx);
        if (vector.size() >= size)
        {
            return;
        }

        condition.wait(lock, [this, size]() { return vector.size() >= size; });
    }
};

Schema createSchema(const std::vector<TestDataTypes>& testDataTypes);

/// Creates an emit function that places buffers into 'resultBuffers' when there is data.
std::function<void(OriginId, Sources::SourceReturnType::SourceReturnType)>
getEmitFunction(ThreadSafeVector<Memory::TupleBuffer>& resultBuffers);

ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig);

std::unique_ptr<Sources::SourceHandle> createFileSource(
    SourceCatalog& sourceCatalog,
    const std::string& filePath,
    const Schema& schema,
    std::shared_ptr<Memory::BufferManager> sourceBufferPool,
    int numberOfLocalBuffersInSource);

std::shared_ptr<InputFormatters::InputFormatterTaskPipeline> createInputFormatterTask(const Schema& schema, std::string formatterType);

/// Waits until source reached EoS
void waitForSource(const std::vector<Memory::TupleBuffer>& resultBuffers, size_t numExpectedBuffers);

/// Compares two files and returns true if they are equal on a byte level.
bool compareFiles(const std::filesystem::path& file1, const std::filesystem::path& file2);

TestPipelineTask createInputFormatterTask(
    SequenceNumber sequenceNumber,
    WorkerThreadId workerThreadId,
    Memory::TupleBuffer taskBuffer,
    std::shared_ptr<InputFormatters::InputFormatterTaskPipeline> inputFormatterTask);

template <typename TupleSchemaTemplate>
struct TestHandle
{
    TestConfig<TupleSchemaTemplate> testConfig;
    std::shared_ptr<Memory::BufferManager> testBufferManager;
    std::shared_ptr<Memory::BufferManager> formattedBufferManager;
    std::shared_ptr<std::vector<std::vector<Memory::TupleBuffer>>> resultBuffers;
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
        schema = Schema{};
    }
};

inline void sortTupleBuffers(std::vector<Memory::TupleBuffer>& buffers)
{
    std::ranges::sort(
        buffers.begin(),
        buffers.end(),
        [](const Memory::TupleBuffer& left, const Memory::TupleBuffer& right)
        {
            if (left.getSequenceNumber() == right.getSequenceNumber())
            {
                return left.getChunkNumber() < right.getChunkNumber();
            }
            return left.getSequenceNumber() < right.getSequenceNumber();
        });
}

/// Takes a vector of tuple buffers and allows to iterate over all tuples in the buffers in order
class TupleIterator
{
public:
    TupleIterator(std::vector<Memory::TupleBuffer> buffers, Schema schema)
        : schema(std::move(schema))
        , buffers(std::move(buffers))
        , currentBuffer(Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(this->buffers.front(), this->schema))
    {
    }

    std::optional<Memory::MemoryLayouts::DynamicTuple> getNextTuple()
    {
        if (currentTupleIdx >= currentBuffer.getNumberOfTuples())
        {
            ++currentBufferIdx;
            if (currentBufferIdx >= buffers.size())
            {
                /// all buffers exhausted
                return std::nullopt;
            }
            currentBuffer = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(buffers.at(currentBufferIdx), schema);
            currentTupleIdx = 0;
        }
        return currentBuffer[currentTupleIdx++];
    }

private:
    size_t currentBufferIdx = 0;
    size_t currentTupleIdx = 0;
    Schema schema;
    std::vector<Memory::TupleBuffer> buffers;
    Memory::MemoryLayouts::TestTupleBuffer currentBuffer;
};

/// Expects tuple buffers with matching sequence numbers contain the same tuples in the same order
inline bool compareTestTupleBuffersOrderSensitive(
    std::vector<Memory::TupleBuffer>& actualResult, std::vector<Memory::TupleBuffer>& expectedResult, Schema schema)
{
    sortTupleBuffers(actualResult);
    sortTupleBuffers(expectedResult);

    bool allTuplesMatch = true;
    TupleIterator expectedResultTupleIt(std::move(expectedResult), schema);
    for (const auto& actualResultTupleBuffer : actualResult)
    {
        for (auto actualResultTestTupleBuffer
             = Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(actualResultTupleBuffer, schema);
             const auto& actualResultTuple : actualResultTestTupleBuffer)
        {
            if (const auto expectedResultTuple = expectedResultTupleIt.getNextTuple())
            {
                if (actualResultTuple != expectedResultTuple)
                {
                    NES_ERROR(
                        "Tuples don't match: {} != {}", actualResultTuple.toString(schema), expectedResultTuple.value().toString(schema));
                    allTuplesMatch = false;
                }
            }
            else
            {
                NES_ERROR("Found actual result tuple: {}, but exhausted expected", actualResultTuple.toString(schema));
                allTuplesMatch = false;
            }
        }
    }
    while (const auto additionalRhsTuple = expectedResultTupleIt.getNextTuple())
    {
        NES_ERROR("Found expected result tuple: {}, but exhausted actual result tuples", additionalRhsTuple.value().toString(schema));
        allTuplesMatch = false;
    }
    return allTuplesMatch;
}

inline bool
checkIfBuffersAreEqual(const Memory::TupleBuffer& leftBuffer, const Memory::TupleBuffer& rightBuffer, const uint64_t schemaSizeInByte)
{
    NES_DEBUG("Checking if the buffers are equal, so if they contain the same tuples...");
    if (leftBuffer.getNumberOfTuples() != rightBuffer.getNumberOfTuples())
    {
        NES_DEBUG("Buffers do not contain the same tuples, as they do not have the same number of tuples");
        return false;
    }

    std::set<uint64_t> sameTupleIndices;
    for (auto idxBuffer1 = 0UL; idxBuffer1 < leftBuffer.getNumberOfTuples(); ++idxBuffer1)
    {
        bool idxFoundInBuffer2 = false;
        for (auto idxBuffer2 = 0UL; idxBuffer2 < rightBuffer.getNumberOfTuples(); ++idxBuffer2)
        {
            if (sameTupleIndices.contains(idxBuffer2))
            {
                continue;
            }

            const auto* const startPosBuffer1 = leftBuffer.getBuffer() + (schemaSizeInByte * idxBuffer1);
            const auto* const startPosBuffer2 = rightBuffer.getBuffer() + (schemaSizeInByte * idxBuffer2);
            if (std::memcmp(startPosBuffer1, startPosBuffer2, schemaSizeInByte) == 0)
            {
                sameTupleIndices.insert(idxBuffer2);
                idxFoundInBuffer2 = true;
                break;
            }
        }

        if (!idxFoundInBuffer2)
        {
            NES_DEBUG("Buffers do not contain the same tuples, as tuple could not be found in both buffers for idx: {}", idxBuffer1);
            return false;
        }
    }

    return (sameTupleIndices.size() == leftBuffer.getNumberOfTuples());
}

inline Memory::TupleBuffer copyStringDataToTupleBuffer(const std::string_view rawData, NES::Memory::TupleBuffer tupleBuffer)
{
    PRECONDITION(
        tupleBuffer.getBufferSize() >= rawData.size(),
        "{} < {}, size of TupleBuffer is not sufficient to contain string",
        tupleBuffer.getBufferSize(),
        rawData.size());
    std::memcpy(tupleBuffer.getBuffer(), rawData.data(), rawData.size());
    tupleBuffer.setNumberOfTuples(rawData.size());
    return tupleBuffer;
}

/// Takes a schema, a buffer manager and tuples.
/// Creates a TestTupleBuffer with row layout using the schema and the buffer manager.
/// Unfolds the tuples into the TestTupleBuffer.
/// Example usage (assumes a bufferManager (shared_ptr to BufferManager object) is available):
///     using TestTuple = std::tuple<int, bool>;
///     SchemaPtr schema = Schema::create()->addField("INT", DataType::Type::INT32)->addField("BOOL", DataType::Type::BOOLEAN);
///     auto testTupleBuffer = TestUtil::createTupleBufferFromTuples(schema, *bufferManager,
///         TestTuple(42, true), TestTuple(43, false), TestTuple(44, true), TestTuple(45, false));
template <typename TupleSchema, bool ContainsVarSized = false, bool PrintDebug = false>
Memory::TupleBuffer
createTupleBufferFromTuples(const Schema& schema, Memory::BufferManager& bufferManager, const std::vector<TupleSchema>& tuples)
{
    PRECONDITION(bufferManager.getAvailableBuffers() != 0, "Cannot create a test tuple buffer, if there are no buffers available");
    auto rowLayout = std::make_shared<Memory::MemoryLayouts::RowLayout>(bufferManager.getBufferSize(), schema);
    auto testTupleBuffer = std::make_unique<Memory::MemoryLayouts::TestTupleBuffer>(rowLayout, bufferManager.getBufferBlocking());

    for (const auto& testTuple : tuples)
    {
        if constexpr (ContainsVarSized)
        {
            testTupleBuffer->pushRecordToBuffer(testTuple, &bufferManager);
        }
        else
        {
            testTupleBuffer->pushRecordToBuffer(testTuple);
        }
    }

    if constexpr (PrintDebug)
    {
        NES_DEBUG("test tuple buffer is: {}", testTupleBuffer->toString(schema));
    }
    return testTupleBuffer->getBuffer();
}

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
            isValid &= checkIfBuffersAreEqual(
                actualResultBuffer, testHandle.expectedResultVectors[taskIndex][bufferIndex], testHandle.schema.getSizeOfSchemaInBytes());
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
            expectedTupleBuffers.at(0).emplace_back(createTupleBufferFromTuples<TupleSchemaTemplate, false, PrintDebug>(
                testHandle.schema, *testHandle.formattedBufferManager, expectedBuffersVector));
        }
    }
    return expectedTupleBuffers;
}

template <typename TupleSchemaTemplate>
TestHandle<TupleSchemaTemplate> setupTest(const TestConfig<TupleSchemaTemplate>& testConfig)
{
    std::shared_ptr<Memory::BufferManager> testBufferManager
        = Memory::BufferManager::create(testConfig.sizeOfRawBuffers, 2 * testConfig.numRequiredBuffers);
    std::shared_ptr<Memory::BufferManager> formattedBufferManager
        = Memory::BufferManager::create(testConfig.sizeOfFormattedBuffers, 2 * testConfig.numRequiredBuffers);

    std::shared_ptr<std::vector<std::vector<NES::Memory::TupleBuffer>>> resultBuffers
        = std::make_shared<std::vector<std::vector<NES::Memory::TupleBuffer>>>(1);
    auto schema = createSchema(testConfig.testSchema);
    return {
        testConfig,
        testBufferManager,
        formattedBufferManager,
        resultBuffers,
        std::move(schema),
        std::make_unique<SingleThreadedTestTaskQueue>(formattedBufferManager, resultBuffers),
        {},
        {}};
}

template <typename TupleSchemaTemplate>
std::vector<TestPipelineTask> createTasks(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    const std::shared_ptr<InputFormatters::InputFormatterTaskPipeline> inputFormatterTask
        = InputFormatters::InputFormatterProvider::provideInputFormatterTask(
            OriginId(0), testHandle.schema, testHandle.testConfig.parserConfig);
    std::vector<TestPipelineTask> tasks;
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
            rawInputBuffer.sequenceNumber, copyStringDataToTupleBuffer(rawInputBuffer.rawBytes, std::move(tupleBuffer.value()))});
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
