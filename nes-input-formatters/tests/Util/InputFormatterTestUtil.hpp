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
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/VariableSizedAccessRef.hpp>
#include <Nautilus/Util.hpp>
#include <Pipelines/CompiledExecutablePipelineStage.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/Logger.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/std/sstream.h>
#include <ErrorHandling.hpp>
#include <TestTaskQueue.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

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
    const MemoryLayoutType memoryLayoutType;
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

/// Generates field names (for field N: Field_N)
Schema createSchema(const std::vector<TestDataTypes>& testDataTypes);
Schema createSchema(const std::vector<TestDataTypes>& testDataTypes, const std::vector<std::string>& testFieldNames);

/// Creates an emit function that places buffers into 'resultBuffers' when there is data.
SourceReturnType::EmitFunction getEmitFunction(ThreadSafeVector<TupleBuffer>& resultBuffers);

ParserConfig validateAndFormatParserConfig(const std::unordered_map<std::string, std::string>& parserConfig);

std::pair<BackpressureController, std::unique_ptr<SourceHandle>> createFileSource(
    SourceCatalog& sourceCatalog,
    const std::string& filePath,
    const Schema& schema,
    std::shared_ptr<BufferManager> sourceBufferPool,
    size_t numberOfRequiredSourceBuffers);

/// Waits until source reached EoS
void waitForSource(const std::vector<TupleBuffer>& resultBuffers, size_t numExpectedBuffers);

/// Compares two files and returns true if they are equal on a byte level.
bool compareFiles(const std::filesystem::path& file1, const std::filesystem::path& file2);

std::shared_ptr<CompiledExecutablePipelineStage> createInputFormatter(
    const ParserConfig& parserConfiguration,
    const Schema& schema,
    MemoryLayoutType memoryLayoutType,
    size_t sizeOfFormattedBuffers,
    bool isCompiled);

std::shared_ptr<CompiledExecutablePipelineStage> createInputFormatter(
    const std::unordered_map<std::string, std::string>& parserConfiguration,
    const Schema& schema,
    MemoryLayoutType memoryLayoutType,
    size_t sizeOfFormattedBuffers,
    bool isCompiled);

template <typename TupleSchemaTemplate>
struct TestHandle
{
    TestConfig<TupleSchemaTemplate> testConfig;
    std::shared_ptr<BufferManager> testBufferManager;
    std::shared_ptr<BufferManager> formattedBufferManager;
    std::shared_ptr<std::vector<std::vector<TupleBuffer>>> resultBuffers;
    Schema schema;
    const MemoryLayoutType memoryLayoutType;
    std::unique_ptr<SingleThreadedTestTaskQueue> testTaskQueue;
    std::vector<TupleBuffer> inputBuffers;
    std::vector<std::vector<TupleBuffer>> expectedResultVectors;

    void destroy()
    {
        inputBuffers.clear();
        expectedResultVectors.clear();
        resultBuffers->clear();
        testTaskQueue.reset();
        schema = Schema{};
    }
};

inline void sortTupleBuffers(std::vector<TupleBuffer>& buffers)
{
    std::ranges::sort(
        buffers.begin(),
        buffers.end(),
        [](const TupleBuffer& left, const TupleBuffer& right)
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
    TupleIterator(std::vector<TupleBuffer> buffers, const Schema& schema, const MemoryLayoutType layoutType)
        : schema(std::move(schema))
        , buffers(std::move(buffers))
        , bufferRef(LowerSchemaProvider::lowerSchema(this->buffers.at(0).getBufferSize(), this->schema, layoutType))
    {
    }

    std::optional<Record> getNextTuple()
    {
        if (currentTupleIdx >= buffers.at(currentBufferIdx).getNumberOfTuples())
        {
            ++currentBufferIdx;
            if (currentBufferIdx >= buffers.size())
            {
                /// all buffers exhausted
                return std::nullopt;
            }
            currentTupleIdx = 0;
        }
        const RecordBuffer recordBuffer{buffers.data() + currentBufferIdx};
        auto record = bufferRef->readRecord(schema.getFieldNames(), recordBuffer, currentTupleIdx);
        ++currentTupleIdx;
        return record;
    }

private:
    size_t currentBufferIdx = 0;
    nautilus::val<uint64_t> currentTupleIdx = 0;
    Schema schema;
    std::vector<TupleBuffer> buffers;
    std::shared_ptr<TupleBufferRef> bufferRef;
};

/// Expects tuple buffers with matching sequence numbers contain the same tuples in the same order
inline bool
compareTestTupleBuffersOrderSensitive(std::vector<TupleBuffer>& actualResult, std::vector<TupleBuffer>& expectedResult, Schema schema)
{
    InputFormatterTestUtil::sortTupleBuffers(actualResult);
    InputFormatterTestUtil::sortTupleBuffers(expectedResult);

    bool allTuplesMatch = true;
    auto bufferRef = LowerSchemaProvider::lowerSchema(expectedResult.at(0).getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);
    TupleIterator expectedResultTupleIt(std::move(expectedResult), schema, MemoryLayoutType::ROW_LAYOUT);
    TupleIterator actualResultTupleIt(std::move(actualResult), schema, MemoryLayoutType::ROW_LAYOUT);
    while (const auto actualResultTuple = actualResultTupleIt.getNextTuple())
    {
        const auto expectedResultTuple = expectedResultTupleIt.getNextTuple();
        if (actualResultTuple != expectedResultTuple)
        {
            NES_ERROR_EXEC("Tuples don't match: {} " << *actualResultTuple << " != " << *expectedResultTuple);
            allTuplesMatch = false;
        }
    }
    while (const auto additionalRhsTuple = expectedResultTupleIt.getNextTuple())
    {
        NES_ERROR_EXEC("Found expected result tuple: {}, but exhausted actual result tuples" << *additionalRhsTuple);
        allTuplesMatch = false;
    }
    return allTuplesMatch;
}

inline bool checkIfBuffersAreEqual(const TupleBuffer& leftBuffer, const TupleBuffer& rightBuffer, const uint64_t schemaSizeInByte)
{
    NES_DEBUG("Checking if the buffers are equal, so if they contain the same tuples...");
    if (leftBuffer.getNumberOfTuples() != rightBuffer.getNumberOfTuples())
    {
        NES_ERROR("Buffers do not contain the same tuples, as they do not have the same number of tuples");
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

            const auto leftFieldSpan = leftBuffer.getAvailableMemoryArea().subspan(schemaSizeInByte * idxBuffer1, schemaSizeInByte);
            const auto rightFieldSpan = rightBuffer.getAvailableMemoryArea().subspan(schemaSizeInByte * idxBuffer2, schemaSizeInByte);
            if (std::ranges::equal(leftFieldSpan, rightFieldSpan))
            {
                sameTupleIndices.insert(idxBuffer2);
                idxFoundInBuffer2 = true;
                break;
            }
        }

        if (!idxFoundInBuffer2)
        {
            NES_ERROR("Buffers do not contain the same tuples, as tuple could not be found in both buffers for idx: {}", idxBuffer1);
            return false;
        }
    }

    return (sameTupleIndices.size() == leftBuffer.getNumberOfTuples());
}

inline void copyStringDataToTupleBuffer(const std::string_view rawData, TupleBuffer& tupleBuffer)
{
    PRECONDITION(
        tupleBuffer.getBufferSize() >= rawData.size(),
        "{} < {}, size of TupleBuffer is not sufficient to contain string",
        tupleBuffer.getBufferSize(),
        rawData.size());
    std::ranges::copy(rawData, reinterpret_cast<char*>(tupleBuffer.getAvailableMemoryArea().data()));
    tupleBuffer.setNumberOfTuples(rawData.size());
}

template <typename T>
void writeFieldToBuffer(
    const T& fieldValue,
    const size_t fieldIndex,
    NES::TupleBuffer& tupleBuffer,
    TupleBufferRef& tupleBufferRef,
    AbstractBufferProvider& bufferProvider)
{
    Record record;
    const RecordBuffer recordBuffer{std::addressof(tupleBuffer)};
    const auto fieldName = tupleBufferRef.getAllFieldNames().at(fieldIndex);

    /// Creating a Nautilus::Record containing the current field
    if constexpr (std::is_same_v<T, std::string>)
    {
        const auto varSizedAccess
            = TupleBufferRef::writeVarSized<TupleBufferRef::PREPEND_LENGTH_AS_UINT32>(tupleBufferRef, bufferProvider, fieldValue);
        const nautilus::val<NES::VariableSizedAccess> access{varSizedAccess};
        record.write(fieldName, access.convertToValue());
    }
    else
    {
        nautilus::val<T> value{fieldValue};
        record.write(fieldName, value);
    }

    const nautilus::val<AbstractBufferProvider*> bufferProviderVal{std::addressof(bufferProvider)};
    auto recordIndex = recordBuffer.getNumRecords();
    tupleBufferRef.writeRecord(recordIndex, recordBuffer, record, bufferProviderVal);
}

inline void printTupleBuffer(const std::string_view message, TupleBuffer& tupleBuffer, const TupleBufferRef& tupleBufferRef)
{
    /// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage) is fine as we are passing it ot a nautilus::val<>
    const nautilus::val<const char*> messageVal{message.data()};
    nautilus::stringstream ss;
    ss << messageVal;
    const RecordBuffer recordBuffer{std::addressof(tupleBuffer)};
    for (nautilus::val<uint64_t> recordIndex = 0; recordIndex < recordBuffer.getNumRecords(); ++recordIndex)
    {
        const auto record = tupleBufferRef.readRecord(tupleBufferRef.getAllFieldNames(), recordBuffer, recordIndex);
        ss << record << "\n";
    }

    NES_DEBUG_EXEC(ss.str().c_str());
}

/// Takes a schema, a buffer manager and tuples.
/// Creates a TestTupleBuffer with row layout using the schema and the buffer manager.
/// Unfolds the tuples into the TestTupleBuffer.
/// Example usage (assumes a bufferManager (shared_ptr to BufferManager object) is available):
///     using TestTuple = std::tuple<int, bool>;
///     SchemaPtr schema = Schema::create()->addField("INT", DataType::Type::INT32)->addField("BOOL", DataType::Type::BOOLEAN);
///     auto testTupleBuffer = TestUtil::createTupleBufferFromTuples(schema, *bufferManager,
///         TestTuple(42, true), TestTuple(43, false), TestTuple(44, true), TestTuple(45, false));
template <typename TupleSchema, bool PrintDebug = false>
TupleBuffer createTupleBufferFromTuples(const Schema& schema, BufferManager& bufferManager, const std::vector<TupleSchema>& tuples)
{
    PRECONDITION(bufferManager.getNumberOfAvailableBuffers() != 0, "Cannot create a test tuple buffer, if there are no buffers available");
    auto tupleBufferRef = LowerSchemaProvider::lowerSchema(bufferManager.getBufferSize(), schema, MemoryLayoutType::ROW_LAYOUT);
    auto tupleBuffer = bufferManager.getBufferBlocking();

    for (const auto& tuple : tuples)
    {
        [&tupleBuffer, &tupleBufferRef, &bufferManager]<size_t... Is>(const auto& fields, std::index_sequence<Is...>)
        {
            (writeFieldToBuffer(std::get<Is>(fields), Is, tupleBuffer, *tupleBufferRef, bufferManager), ...);
        }(tuple, std::make_index_sequence<std::tuple_size_v<TupleSchema>>{});
        tupleBuffer.setNumberOfTuples(tupleBuffer.getNumberOfTuples() + 1);
    }

    if constexpr (PrintDebug)
    {
        printTupleBuffer("test tuple buffer is: ", tupleBuffer, *tupleBufferRef);
    }
    return tupleBuffer;
}

/// Gets the actual result buffers and the expected result buffers from the test handle and compares them.
/// Logs both the actual and the expected buffers if 'PrintDebug' is set to true.
template <typename TupleSchemaTemplate, bool PrintDebug>
bool validateResult(TestHandle<TupleSchemaTemplate>& testHandle)
{
    /// check that vectors of vectors contain the same number of vectors.
    bool isValid = true;
    isValid &= (testHandle.resultBuffers->size() == testHandle.expectedResultVectors.size());

    /// iterate over all vectors in the actual results
    for (size_t taskIndex = 0; auto& actualResultVector : *testHandle.resultBuffers)
    {
        /// check that the corresponding vector in the vector of vector containing the expected results is of the same size
        isValid &= (actualResultVector.size() == testHandle.expectedResultVectors[taskIndex].size());
        /// iterate over all buffers in the vector containing the actual results and compare the buffer with the corresponding buffers
        /// in the expected results.
        for (size_t bufferIndex = 0; auto& actualResultBuffer : actualResultVector)
        {
            if (PrintDebug)
            {
                /// If specified, print the contents of the buffers.
                auto tupleBufferRef
                    = LowerSchemaProvider::lowerSchema(actualResultBuffer.getBufferSize(), testHandle.schema, MemoryLayoutType::ROW_LAYOUT);
                printTupleBuffer("\n Actual result buffer:\n", actualResultBuffer, *tupleBufferRef);
                printTupleBuffer(" Expected result buffer:\n", testHandle.expectedResultVectors[taskIndex][bufferIndex], *tupleBufferRef);
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
std::vector<std::vector<TupleBuffer>> createExpectedResults(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    std::vector<std::vector<TupleBuffer>> expectedTupleBuffers(1);
    for (const auto workerThreadResultVector : testHandle.testConfig.expectedResults)
    {
        /// expectedBuffersVector: vector<TupleSchemaTemplate>
        for (const auto& expectedBuffersVector : workerThreadResultVector.expectedResultsForThread)
        {
            expectedTupleBuffers.at(0).emplace_back(createTupleBufferFromTuples<TupleSchemaTemplate, PrintDebug>(
                testHandle.schema, *testHandle.formattedBufferManager, expectedBuffersVector));
        }
    }
    return expectedTupleBuffers;
}

template <typename TupleSchemaTemplate>
TestHandle<TupleSchemaTemplate> setupTest(const TestConfig<TupleSchemaTemplate>& testConfig)
{
    std::shared_ptr<BufferManager> testBufferManager
        = BufferManager::create(testConfig.sizeOfRawBuffers, 2 * testConfig.numRequiredBuffers);
    std::shared_ptr<BufferManager> formattedBufferManager
        = BufferManager::create(testConfig.sizeOfFormattedBuffers, 2 * testConfig.numRequiredBuffers);

    auto resultBuffers = std::make_shared<std::vector<std::vector<TupleBuffer>>>(1);
    auto schema = createSchema(testConfig.testSchema);
    return {
        testConfig,
        testBufferManager,
        formattedBufferManager,
        resultBuffers,
        std::move(schema),
        testConfig.memoryLayoutType,
        std::make_unique<SingleThreadedTestTaskQueue>(formattedBufferManager, resultBuffers),
        {},
        {}};
}

template <typename TupleSchemaTemplate>
std::vector<TestPipelineTask> createTasks(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    auto inputFormatter = createInputFormatter(
        testHandle.testConfig.parserConfig,
        testHandle.schema,
        testHandle.memoryLayoutType,
        testHandle.formattedBufferManager->getBufferSize(),
        false);
    std::vector<TestPipelineTask> tasks;
    tasks.reserve(testHandle.inputBuffers.size());
    for (const auto& inputBuffer : testHandle.inputBuffers)
    {
        tasks.emplace_back(TestPipelineTask{WorkerThreadId(0), inputBuffer, inputFormatter});
    }
    return tasks;
}

template <typename TupleSchemaTemplate>
std::vector<TupleBuffer> createTestTupleBuffers(const TestHandle<TupleSchemaTemplate>& testHandle)
{
    std::vector<TupleBuffer> rawTupleBuffers;
    for (const auto& rawInputBuffer : testHandle.testConfig.rawBytesPerThread)
    {
        if (auto tupleBuffer = testHandle.testBufferManager->getBufferNoBlocking())
        {
            copyStringDataToTupleBuffer(rawInputBuffer.rawBytes, tupleBuffer.value());
            tupleBuffer.value().setSequenceNumber(rawInputBuffer.sequenceNumber);
            tupleBuffer.value().setChunkNumber(INITIAL_CHUNK_NUMBER);
            rawTupleBuffers.emplace_back(tupleBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("Couldn't get buffer from bufferManager. Configure test to use more buffers.");
        }
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
}
}
