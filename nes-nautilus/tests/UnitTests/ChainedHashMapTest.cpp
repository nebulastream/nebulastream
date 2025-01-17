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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <numeric>
#include <ranges>
#include <sstream>
#include <tuple>
#include <vector>

#include <API/Schema.hpp>
#include <Configurations/Enums/NautilusBackend.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMapInterface.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Ranges.hpp>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <BaseUnitTest.hpp>
#include <NautilusTestUtils.hpp>
#include <function.hpp>
#include <magic_enum.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::Nautilus::Interface
{
/// Number of items, Number of buckets, Page Size, Key Data Types, Value Data Types, Backend
using TestParams
    = std::tuple<uint64_t, uint64_t, uint64_t, std::vector<BasicType>, std::vector<BasicType>, QueryCompilation::NautilusBackend>;

/// This function should be used in the tests whenever we want to ensure that a specific value is not inserted/created again in the hash map
#define ASSERT_VIOLATION_FOR_ON_INSERT \
    [&](const nautilus::val<AbstractHashMapEntry*>&) \
    { \
        nautilus::invoke(+[]() \
                         { \
                             NES_ERROR("Should not insert a value here"); \
                             ASSERT_TRUE(false); \
                         }); \
    }

class ChainedHashMapTest : public Testing::BaseUnitTest, public testing::WithParamInterface<TestParams>, public NautilusTestUtils
{
public:
    Memory::BufferManagerPtr bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    SchemaPtr inputSchema;
    std::vector<MemoryProvider::FieldOffsets> fieldKeys, fieldValues;
    std::vector<Record::RecordFieldIdentifier> projectionKeys, projectionValues;
    std::vector<Memory::TupleBuffer> inputBuffers;
    std::shared_ptr<MemoryProvider::TupleBufferMemoryProvider> memoryProviderInputBuffer;
    uint64_t keySize{}, valueSize{}, entriesPerPage{}, entrySize{};
    QueryCompilation::NautilusBackend backend;

    static void SetUpTestSuite()
    {
        Logger::setupLogging("ChainedHashMapTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("Setup ChainedHashMapTest class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        const auto& [numberOfItems, numberOfBuckets, pageSize, keyTypes, valueTypes, backend] = GetParam();


        /// Setting the correct options for the engine, depending on the enum value from the backend
        nautilus::engine::Options options;
        const bool compilation = (backend == QueryCompilation::NautilusBackend::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);
        this->backend = backend;

        /// Creating a combined schema with the keys and value types.
        const auto inputSchemaKey = NautilusTestUtils::createSchemaFromBasicTypes(keyTypes);
        const auto inputSchemaValue = NautilusTestUtils::createSchemaFromBasicTypes(valueTypes, inputSchemaKey->getFieldCount());
        const auto fieldNamesKey = inputSchemaKey->getFieldNames();
        const auto fieldNamesValue = inputSchemaValue->getFieldNames();
        inputSchema = Schema::create();
        inputSchema->copyFields(inputSchemaKey);
        inputSchema->copyFields(inputSchemaValue);
        keySize = inputSchemaKey->getSchemaSizeInBytes();
        valueSize = inputSchemaValue->getSchemaSizeInBytes();
        entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
        entriesPerPage = pageSize / entrySize;

        /// Creating a buffer manager with a buffer size of 4k (default) and enough buffers to hold all input tuples
        /// We set here a minimum number of buffers. This is a heuristic value and if needed, we can increase it.
        /// We use a minimum number of buffers, as some tests (e.g. customValue) creates more tuple buffers from the buffer manager.
        /// We use 3 calls to create the monotonic values, keys+values, updates values and for the compoundKeys
        constexpr auto bufferSize = 4096;
        constexpr auto minimumBuffers = 4000UL;
        constexpr auto callsToCreateMonotonicValues = 3;
        const auto bufferNeeded = callsToCreateMonotonicValues * ((inputSchema->getSchemaSizeInBytes() * numberOfItems) / bufferSize + 1);
        bufferManager = Memory::BufferManager::create(bufferSize, std::max(bufferNeeded, minimumBuffers));

        /// Creating a tuple buffer memory provider for the key and value buffers
        memoryProviderInputBuffer = MemoryProvider::TupleBufferMemoryProvider::create(bufferManager->getBufferSize(), inputSchema);

        /// Creating the fields for the key and value from the schema
        std::tie(fieldKeys, fieldValues)
            = MemoryProvider::ChainedEntryMemoryProvider::createFieldOffsets(*inputSchema, fieldNamesKey, fieldNamesValue);
        projectionKeys
            = fieldKeys | std::views::transform([](const auto& field) { return field.fieldIdentifier; }) | NES::ranges::to<std::vector>();
        projectionValues
            = fieldValues | std::views::transform([](const auto& field) { return field.fieldIdentifier; }) | NES::ranges::to<std::vector>();


        /// Creating the buffers with the values for the keys and values
        inputBuffers = createMonotonicIncreasingValues(inputSchema, numberOfItems, backend, *bufferManager);
    }

    static void TearDownTestSuite() { NES_INFO("Tear down ChainedHashMapTest class."); }


    void checkEntryIterator(ChainedHashMap& hashMap, const std::map<RecordWithFields, Record>& exactMap)
    {
        /// Compiling a function that writes all keys and values to bufferOutput.
        /// To iterate over all key and values, we use the entry iterato. We assume that the bufferOutput is large enough to hold all values.
        /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
        /// ReSharper disable once CppPassValueParameterByConstReference
        auto findAndWriteToOutputBuffer = nautilusEngine->registerFunction(std::function(
            [this](nautilus::val<Memory::TupleBuffer*> bufferOutput, nautilus::val<HashMapInterface*> hashMapVal)
            {
                ChainedHashMapRef const hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                RecordBuffer recordBufferOutput(bufferOutput);
                nautilus::val<uint64_t> outputBufferIndex(0);
                for (auto entry : hashMapRef)
                {
                    /// Writing the read value from the chained hash map into the buffer.
                    Record outputRecord;
                    ChainedEntryRef const entryRef(entry, fieldKeys, fieldValues);
                    const auto keyRecord = entryRef.getKey();
                    const auto valueRecord = entryRef.getValue();
                    outputRecord.combineRecords(keyRecord);
                    outputRecord.combineRecords(valueRecord);
                    memoryProviderInputBuffer->writeRecord(outputBufferIndex, recordBufferOutput, outputRecord);
                    outputBufferIndex = outputBufferIndex + nautilus::static_val<uint64_t>(1);
                    recordBufferOutput.setNumRecords(outputBufferIndex);
                }
            }));


        /// Ensuring that the number of tuples is correct.
        ASSERT_EQ(hashMap.getNumberOfTuples(), exactMap.size());

        /// Calling now the compiled function to write all values of the map to the output buffer.
        const auto numberOfInputTuples = std::accumulate(
            inputBuffers.begin(),
            inputBuffers.end(),
            0,
            [](const auto& sum, const auto& buffer) { return sum + buffer.getNumberOfTuples(); });
        auto bufferOutputOpt = bufferManager->getUnpooledBuffer(numberOfInputTuples * inputSchema->getSchemaSizeInBytes());
        if (not bufferOutputOpt)
        {
            NES_ERROR("Could not allocate buffer for size {}", numberOfInputTuples * inputSchema->getSchemaSizeInBytes());
            ASSERT_TRUE(false);
        }
        auto bufferOutput = bufferOutputOpt.value();
        std::memset(bufferOutput.getBuffer(), 0, bufferOutput.getBufferSize());
        findAndWriteToOutputBuffer(std::addressof(bufferOutput), std::addressof(hashMap));

        /// Checking if the number of items are equal to the number of items in the exact map.
        ASSERT_EQ(bufferOutput.getNumberOfTuples(), exactMap.size());

        /// Now we are iterating over all tuples in the bufferOutput and compare them with the exact values from the map.
        RecordBuffer const recordBufferOutput(nautilus::val<const Memory::TupleBuffer*>(std::addressof(bufferOutput)));
        for (nautilus::val<uint64_t> pos = 0; pos < recordBufferOutput.getNumRecords(); ++pos)
        {
            /// Reading the actual key and value and the exact value
            auto recordKeyActual = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferOutput, pos);
            auto recordValueActual = memoryProviderInputBuffer->readRecord(projectionValues, recordBufferOutput, pos);
            auto recordValueExact = exactMap.find({recordKeyActual, projectionKeys});
            EXPECT_NE(recordValueExact, exactMap.end()) << "Could not find a record for this key";

            /// Printing an error message, if the values are not equal.
            std::stringstream ss;
            ss << compareRecords(recordValueActual, recordValueExact->second, projectionValues);
            if (not ss.str().empty())
            {
                EXPECT_TRUE(false) << ss.str();
            }
        }
    }

    void checkIfValuesAreCorrectViaFindEntry(ChainedHashMap& hashMap, const std::map<RecordWithFields, Record>& exactMap)
    {
        /// Compiling the query that writes the values for all keys in keyBufferRef to outputBufferForKeys.
        /// This enables us to perform a comparison in the c++ code by comparing every value in the record buffer with the exact value.
        /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
        /// ReSharper disable once CppPassValueParameterByConstReference
        auto findAndWriteToOutputBuffer = nautilusEngine->registerFunction(std::function(
            [this](
                nautilus::val<Memory::TupleBuffer*> keyBufferRef,
                nautilus::val<Memory::TupleBuffer*> outputBufferForValues,
                nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
                nautilus::val<HashMapInterface*> hashMapVal)
            {
                ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                const RecordBuffer recordBufferKey(keyBufferRef);
                for (nautilus::val<uint64_t> i = 0; i < recordBufferKey.getNumRecords(); i = i + 1)
                {
                    auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, i);
                    auto foundEntry = hashMapRef.findOrCreateEntry(
                        recordKey, *getMurMurHashFunction(), ASSERT_VIOLATION_FOR_ON_INSERT, bufferManagerVal);

                    const auto castedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(foundEntry);
                    const ChainedEntryRef entry(castedEntry, fieldKeys, fieldValues);

                    /// Writing the read value from the chained hash map into the buffer.
                    Record outputRecord;
                    const auto keyRecord = entry.getKey();
                    const auto valueRecord = entry.getValue();
                    outputRecord.combineRecords(keyRecord);
                    outputRecord.combineRecords(valueRecord);

                    RecordBuffer recordBufferOutput(outputBufferForValues);
                    memoryProviderInputBuffer->writeRecord(i, recordBufferOutput, outputRecord);
                    recordBufferOutput.setNumRecords(i + 1);
                }
            }));

        /// Ensuring that the number of tuples is correct.
        ASSERT_EQ(hashMap.getNumberOfTuples(), exactMap.size());


        for (auto& inputBuffer : inputBuffers)
        {
            /// We assume that the valueBuffer has the corresponding values for the keyBuffer.
            /// Under this assumption, we know that the outputBufferForKeys MUST have the same size as the valueBuffer
            auto bufferOutputOpt = bufferManager->getUnpooledBuffer(inputBuffer.getBufferSize());
            if (not bufferOutputOpt)
            {
                NES_ERROR("Could not allocate buffer for size {}", inputBuffer.getBufferSize());
                ASSERT_TRUE(false);
            }
            auto bufferOutput = bufferOutputOpt.value();
            std::memset(bufferOutput.getBuffer(), 0, bufferOutput.getBufferSize());
            findAndWriteToOutputBuffer(
                std::addressof(inputBuffer), std::addressof(bufferOutput), bufferManager.get(), std::addressof(hashMap));

            RecordBuffer const recordBufferInput(nautilus::val<const Memory::TupleBuffer*>(std::addressof(inputBuffer)));
            RecordBuffer const recordBufferActual(nautilus::val<const Memory::TupleBuffer*>(std::addressof(bufferOutput)));
            for (nautilus::val<uint64_t> i = 0; i < recordBufferInput.getNumRecords(); i = i + 1)
            {
                /// Reading the actual key and value and the exact value
                auto recordKeyActual = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferActual, i);
                auto recordValueActual = memoryProviderInputBuffer->readRecord(projectionValues, recordBufferActual, i);
                auto recordKeyExact = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferInput, i);
                auto recordValueExact = exactMap.find({recordKeyExact, projectionKeys});
                EXPECT_NE(recordValueExact, exactMap.end()) << "Could not find the record with the key";

                /// Printing an error message, if the values are not equal.
                std::stringstream ss;
                ss << compareRecords(recordKeyActual, recordKeyExact, projectionKeys);
                ss << compareRecords(recordValueActual, recordValueExact->second, projectionValues);
                if (not ss.str().empty())
                {
                    EXPECT_TRUE(false) << ss.str();
                }
            }
        }
    }

    void checkInsertAndReadValues(ChainedHashMap& hashMap, const std::map<RecordWithFields, Record>& exactMap)
    {
        /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
        auto findAndInsertQuery = nautilusEngine->registerFunction(std::function(
            /// ReSharper disable once CppPassValueParameterByConstReference
            [this](
                nautilus::val<Memory::TupleBuffer*> inputBufferRef,
                nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
                nautilus::val<HashMapInterface*> hashMapVal)
            {
                ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                const RecordBuffer recordBuffer(inputBufferRef);
                for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
                {
                    auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBuffer, i);
                    auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBuffer, i);
                    auto foundEntry = hashMapRef.findOrCreateEntry(
                        recordKey,
                        *getMurMurHashFunction(),
                        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                        {
                            const auto castedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(entry);
                            const ChainedEntryRef ref(castedEntry, fieldKeys, fieldValues);
                            ref.copyValuesToEntry(recordValue);
                        },
                        bufferManagerVal);
                }
            }));

        /// Now we are calling the compiled query with the random key and value buffers.
        for (auto& buffer : inputBuffers)
        {
            findAndInsertQuery(std::addressof(buffer), bufferManager.get(), std::addressof(hashMap));
        }

        /// Now we are searching for the entries and checking if the values are correct.
        checkIfValuesAreCorrectViaFindEntry(hashMap, exactMap);
    }

    void checkInsertFindAndUpdateValues(
        ChainedHashMap& hashMap, std::vector<Memory::TupleBuffer>& updatedBuffersValues, const std::map<RecordWithFields, Record>& exactMap)
    {
        /// Compiling a function that finds the entry and updates the value.
        /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
        /// ReSharper disable once CppPassValueParameterByConstReference
        auto findAndUpdate = nautilusEngine->registerFunction(std::function(
            [this](
                nautilus::val<Memory::TupleBuffer*> keyBufferRef,
                nautilus::val<Memory::TupleBuffer*> valueBufferUpdatedRef,
                nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
                nautilus::val<HashMapInterface*> hashMapVal)
            {
                ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                const RecordBuffer recordBufferKey(keyBufferRef);
                const RecordBuffer recordBufferValue(valueBufferUpdatedRef);

                for (nautilus::val<uint64_t> i = 0; i < recordBufferKey.getNumRecords(); i = i + 1)
                {
                    auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, i);
                    auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBufferValue, i);
                    auto foundEntry = hashMapRef.findOrCreateEntry(
                        recordKey,
                        *getMurMurHashFunction(),
                        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                        {
                            const auto castedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(entry);
                            const ChainedEntryRef ref(castedEntry, fieldKeys, fieldValues);
                            ref.copyValuesToEntry(recordValue);
                        },
                        bufferManagerVal);
                    hashMapRef.insertOrUpdateEntry(
                        foundEntry,
                        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                        {
                            const ChainedEntryRef ref(entry, fieldKeys, fieldValues);
                            ref.copyValuesToEntry(recordValue);
                        },
                        ASSERT_VIOLATION_FOR_ON_INSERT,
                        bufferManagerVal);
                }
            }));

        /// Calling the compiled function with the random key and value buffers.
        for (const auto& [keyBuffer, valueBuffer] : std::views::zip(inputBuffers, updatedBuffersValues))
        {
            findAndUpdate(std::addressof(keyBuffer), std::addressof(valueBuffer), bufferManager.get(), std::addressof(hashMap));
        }

        /// Now we are searching for the entries and checking if the values are correct.
        checkIfValuesAreCorrectViaFindEntry(hashMap, exactMap);
    }
};

TEST_P(ChainedHashMapTest, fixedDataTypesSingleInsert)
{
    const auto [numberOfItems, numberOfBuckets, pageSize, keyBasicTypes, valueBasicTypes, backend] = GetParam();
    /// With the interpreter backend, we skip all tests that will take more than 5 minutes to run.
    if (backend == QueryCompilation::NautilusBackend::INTERPRETER
        and (numberOfItems >= 5 * 1000 or (numberOfItems >= 1000 and numberOfBuckets <= 10) or (numberOfItems >= 1000 and pageSize <= 4096)))
    {
        NES_INFO(
            "Skipping test with interpreter backend for numberOfItems: {}, numberOfBuckets: {}, pageSize: {} as running this test "
            "would take too long.",
            numberOfItems,
            numberOfBuckets,
            pageSize);
        GTEST_SKIP();
    }

    /// Creating the hash map
    auto hashMap = ChainedHashMap(keySize, valueSize, numberOfBuckets, pageSize);

    /// Check if the hash map is empty.
    ASSERT_EQ(hashMap.getNumberOfTuples(), 0);

    /// We are inserting the records from the random key and value buffers into a map.
    /// Thus, we can check if the provided values are correct.
    std::map<RecordWithFields, Record> exactMap;
    for (const auto& buffer : inputBuffers)
    {
        RecordBuffer const recordBuffer(nautilus::val<const Memory::TupleBuffer*>(std::addressof(buffer)));
        for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
        {
            auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBuffer, i);
            auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBuffer, i);

            /// We are testing here the findOrCreate method that only inserts a value if it does not exist, i.e., no update.
            /// Therefore, we MUST NOT overwrite an existing key in this loop here to be able to test the findOrCreate method.
            exactMap.insert({{recordKey, projectionKeys}, recordValue});
        }
    }

    /// Check if we can insert the entry and then read the values back.
    checkInsertAndReadValues(hashMap, exactMap);

    /// Check if our entry iterator reads all the entries
    checkEntryIterator(hashMap, exactMap);
}

TEST_P(ChainedHashMapTest, fixedDataTypesUpdate)
{
    const auto [numberOfItems, numberOfBuckets, pageSize, keyBasicTypes, valueBasicTypes, backend] = GetParam();
    /// With the interpreter backend, we skip all tests that will take more than 5 minutes to run.
    if (backend == QueryCompilation::NautilusBackend::INTERPRETER
        and (numberOfItems >= 5 * 1000 or (numberOfItems >= 100 and numberOfBuckets <= 10) or (numberOfItems >= 1000 and pageSize <= 4096)))
    {
        NES_INFO(
            "Skipping test with interpreter backend for numberOfItems: {}, numberOfBuckets: {}, pageSize: {} as running this test "
            "would take too long.",
            numberOfItems,
            numberOfBuckets,
            pageSize);
        GTEST_SKIP();
    }

    auto updatedBuffersValues = createMonotonicIncreasingValues(inputSchema, numberOfItems, backend, *bufferManager);

    /// Creating the hash map
    auto hashMap = ChainedHashMap(keySize, valueSize, numberOfBuckets, pageSize);

    /// Check if the hash map is empty.
    ASSERT_EQ(hashMap.getNumberOfTuples(), 0);


    /// We are inserting the records from the random key and value buffers into a map.
    /// Thus, we can check if the provided values are correct.
    std::map<RecordWithFields, Record> exactMap;
    for (const auto& buffer : inputBuffers)
    {
        RecordBuffer const recordBuffer(nautilus::val<const Memory::TupleBuffer*>(std::addressof(buffer)));
        for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
        {
            const auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBuffer, i);
            const auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBuffer, i);

            /// We are testing here the findOrCreate and the insertOrUpdateEntry method, i.e., we are updating the values for existing keys.
            /// Therefore, we MUST overwrite an existing key in this loop here to be able to test the findOrCreate method.
            exactMap[{recordKey, projectionKeys}] = recordValue;
        }
    }

    /// Check if we can update the values.
    checkInsertFindAndUpdateValues(hashMap, updatedBuffersValues, exactMap);


    /// Check if our entry iterator reads all the entries
    checkEntryIterator(hashMap, exactMap);
}

/// Test for inserting compound keys and custom values. We choose a PagedVector as the custom value. This is similar to a multimap.
TEST_P(ChainedHashMapTest, customValue)
{
    const auto [numberOfItems, numberOfBuckets, pageSize, keyBasicTypes, valueBasicTypes, backend] = GetParam();
    constexpr auto valueSize = sizeof(PagedVector);
    /// With the interpreter backend, we skip all tests that will take more than 5 minutes to run.
    if ((backend == QueryCompilation::NautilusBackend::COMPILER and (numberOfItems >= 1000 or pageSize <= 1024))
        or (backend == QueryCompilation::NautilusBackend::INTERPRETER
            and (numberOfItems >= 500 or (numberOfItems >= 100 and numberOfBuckets <= 10) or (numberOfItems >= 100 and pageSize <= 2048))))
    {
        NES_INFO(
            "Skipping test with interpreter backend for numberOfItems: {}, numberOfBuckets: {}, pageSize: {} as running this test "
            "would take too long.",
            numberOfItems,
            numberOfBuckets,
            pageSize);
        GTEST_SKIP();
    }

    /// Creating the destructor callback for each item, i.e. keys and PagedVector
    auto destructorCallback = [&](const ChainedHashMapEntry* entry)
    {
        const auto* memArea = reinterpret_cast<const int8_t*>(entry) + sizeof(ChainedHashMapEntry) + keySize;
        const auto* pagedVector = reinterpret_cast<const PagedVector*>(memArea);
        pagedVector->~PagedVector();
    };

    /// Creating the hash map
    auto hashMap = ChainedHashMap(keySize, valueSize, numberOfBuckets, pageSize);
    hashMap.setDestructorCallback(destructorCallback);

    /// Check if the hash map is empty.
    ASSERT_EQ(hashMap.getNumberOfTuples(), 0);

    /// Creating buffers for each key, which tuples will be written into the paged vector
    std::vector<Memory::TupleBuffer> valuesForPagedVector;
    const auto memoryProvider = MemoryProvider::TupleBufferMemoryProvider::create(bufferManager->getBufferSize(), inputSchema->copy());

    /// We set this value to be large enough so that we have multiple pages for the PagedVector.
    const auto maxNumberOfTuplesPerKey = (memoryProvider->getMemoryLayoutPtr()->getBufferSize() / inputSchema->getSchemaSizeInBytes()) * 3;

    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        const auto randNumberOfTuples = (std::rand() % maxNumberOfTuplesPerKey) + 1;
        auto buffer = bufferManager->getUnpooledBuffer(inputSchema->getSchemaSizeInBytes() * randNumberOfTuples);
        if (not buffer)
        {
            NES_ERROR("Could not allocate buffer for size {}", inputSchema->getSchemaSizeInBytes() * randNumberOfTuples);
            ASSERT_TRUE(false);
        }
        auto inputBuffer = buffer.value();
        callCompiledFunction<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, uint64_t, uint64_t, uint64_t>(
            {CREATE_MONOTONIC_VALUES_FOR_BUFFER, backend}, std::addressof(inputBuffer), bufferManager.get(), randNumberOfTuples, i + 1, 0);
        valuesForPagedVector.emplace_back(inputBuffer);
    }

    /// Resetting the entriesPerPage, as we have a paged vector as the value.
    const auto totalSizeOfEntry = (sizeof(ChainedHashMapEntry) + keySize + valueSize);
    entriesPerPage = pageSize / (totalSizeOfEntry);


    /// Compiling a function that inserts the key from keyPosition and then all values for the PagedVector into the hash map.
    /// We provide the function a buffer that contain the key and one that contains all values for the PagedVector.
    auto projectionKeys
        = fieldKeys | std::views::transform([](const auto& field) { return field.fieldIdentifier; }) | NES::ranges::to<std::vector>();
    auto projectionAllFields = inputSchema->getFieldNames();

    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    auto compiledFindAndInsertIntoPagedVector = nautilusEngine->registerFunction(std::function(
        [this, projectionKeys, projectionAllFields, memoryProvider](
            nautilus::val<Memory::TupleBuffer*> keyBufferRef,
            nautilus::val<uint64_t> keyPositionVal,
            nautilus::val<Memory::TupleBuffer*> valueBufferRef,
            nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<HashMapInterface*> hashMapVal)
        {
            ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(keyBufferRef);
            const RecordBuffer recordBufferValue(valueBufferRef);
            auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, keyPositionVal);
            auto foundEntry = hashMapRef.findOrCreateEntry(
                recordKey,
                *getMurMurHashFunction(),
                [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                {
                    const ChainedEntryRef ref(entry, fieldKeys, fieldValues);
                    nautilus::invoke(
                        +[](int8_t* pagedVectorMemArea)
                        {
                            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                            new (pagedVector) PagedVector();
                        },
                        ref.getValueMemArea());
                },
                bufferManagerVal);
            const ChainedEntryRef ref(foundEntry, fieldKeys, fieldValues);
            const PagedVectorRef pagedVectorRef(ref.getValueMemArea(), memoryProvider, bufferManagerVal);
            for (nautilus::val<uint64_t> idxValues = 0; idxValues < recordBufferValue.getNumRecords(); idxValues = idxValues + 1)
            {
                auto recordValue = memoryProviderInputBuffer->readRecord(projectionAllFields, recordBufferValue, idxValues);
                pagedVectorRef.writeRecord(recordValue);
            }
        }));

    /// Now calling the compiled function with the random key and value buffers.
    /// Additionally, we are writing the values to a hashmap to be able to compare the values later.
    std::multimap<RecordWithFields, Record> exactMap;
    uint64_t keyPosition = 0;
    for (auto& keyBuffer : inputBuffers)
    {
        for (uint64_t key = 0; key < keyBuffer.getNumberOfTuples(); ++key)
        {
            const auto currentKeyPosition = keyPosition + key;
            auto valueBuffer = valuesForPagedVector[currentKeyPosition];
            compiledFindAndInsertIntoPagedVector(
                std::addressof(keyBuffer), currentKeyPosition, std::addressof(valueBuffer), bufferManager.get(), std::addressof(hashMap));

            /// Writing the key and values to the exact map to compare the values later.
            RecordBuffer const recordBufferKey(nautilus::val<const Memory::TupleBuffer*>(std::addressof(keyBuffer)));
            nautilus::val<uint64_t> currentKeyPositionVal = currentKeyPosition;
            auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, currentKeyPositionVal);
            RecordBuffer const recordBufferValue(nautilus::val<const Memory::TupleBuffer*>(std::addressof(valueBuffer)));
            for (nautilus::val<uint64_t> i = 0; i < recordBufferValue.getNumRecords(); i = i + 1)
            {
                auto recordValue = memoryProviderInputBuffer->readRecord(projectionAllFields, recordBufferValue, i);
                exactMap.insert({{recordKey, projectionKeys}, recordValue});
            }
        }
        keyPosition += keyBuffer.getNumberOfTuples();
    }

    /// We are now creating a function that iterates over all keys and write all values for one key to the output buffer.
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    auto compiledIterateOverKeysAndWriteToOutputBuffer = nautilusEngine->registerFunction(std::function(
        [this, projectionKeys, projectionAllFields, memoryProvider](
            nautilus::val<Memory::TupleBuffer*> keyBufferRef,
            nautilus::val<uint64_t> keyPositionVal,
            nautilus::val<Memory::TupleBuffer*> outputBufferRef,
            nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<HashMapInterface*> hashMapVal)
        {
            ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, {}, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(keyBufferRef);
            RecordBuffer recordBufferOutput(outputBufferRef);
            const auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, keyPositionVal);
            const auto foundEntry
                = hashMapRef.findOrCreateEntry(recordKey, *getMurMurHashFunction(), ASSERT_VIOLATION_FOR_ON_INSERT, bufferManagerVal);

            ChainedEntryRef const ref(foundEntry, fieldKeys, fieldValues);
            const PagedVectorRef pagedVectorRef(ref.getValueMemArea(), memoryProvider, bufferManagerVal);
            nautilus::val<uint64_t> recordBufferIndex = 0;
            recordBufferOutput.setNumRecords(recordBufferIndex);
            for (auto it = pagedVectorRef.begin(projectionAllFields); it != pagedVectorRef.end(projectionAllFields); ++it)
            {
                const auto record = *it;
                memoryProviderInputBuffer->writeRecord(recordBufferIndex, recordBufferOutput, record);
                recordBufferIndex = recordBufferIndex + 1;
                recordBufferOutput.setNumRecords(recordBufferIndex);
            }
        }));

    /// Checking if all values for each paged vector can be written back
    keyPosition = 0;
    for (auto& keyBuffer : inputBuffers)
    {
        for (uint64_t key = 0; key < keyBuffer.getNumberOfTuples(); ++key)
        {
            const auto currentKeyPosition = keyPosition + key;
            nautilus::val<uint64_t> currentKeyPositionVal = currentKeyPosition;
            RecordBuffer const recordBufferKey(nautilus::val<const Memory::TupleBuffer*>(std::addressof(keyBuffer)));
            auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, currentKeyPositionVal);
            const auto numberOfExpectedValues = exactMap.count({recordKey, projectionKeys});
            const auto bufferOutputOpt = bufferManager->getUnpooledBuffer(numberOfExpectedValues * inputSchema->getSchemaSizeInBytes());
            if (not bufferOutputOpt)
            {
                NES_ERROR("Could not allocate buffer for size {}", valuesForPagedVector[currentKeyPosition].getBufferSize());
                ASSERT_TRUE(false);
            }
            auto outputBuffer = bufferOutputOpt.value();

            compiledIterateOverKeysAndWriteToOutputBuffer(
                std::addressof(keyBuffer), currentKeyPosition, std::addressof(outputBuffer), bufferManager.get(), std::addressof(hashMap));


            /// Comparing the values from the PagedVector with the exact values.
            RecordBuffer const recordBufferOutput(nautilus::val<const Memory::TupleBuffer*>(std::addressof(outputBuffer)));
            auto [recordValueExactStart, recordValueExactEnd] = exactMap.equal_range({recordKey, projectionKeys});
            ASSERT_EQ(recordBufferOutput.getNumRecords(), std::distance(recordValueExactStart, recordValueExactEnd));

            nautilus::val<uint64_t> currentPosition = 0;
            for (auto exactIt = recordValueExactStart; exactIt != recordValueExactEnd; ++exactIt)
            {
                /// Printing an error message, if the values are not equal.
                auto recordValueActual = memoryProviderInputBuffer->readRecord(projectionAllFields, recordBufferOutput, currentPosition);
                std::stringstream ss;
                ss << compareRecords(recordValueActual, exactIt->second, projectionAllFields);
                if (not ss.str().empty())
                {
                    EXPECT_TRUE(false) << ss.str();
                }
                ++currentPosition;
            }
        }
        keyPosition += keyBuffer.getNumberOfTuples();
    }
}

INSTANTIATE_TEST_CASE_P(
    ChainedHashMapTest,
    ChainedHashMapTest,
    ::testing::Combine(
        ::testing::Values(1, 10000),
        ::testing::Values(1, 10000),
        ::testing::Values(128, 2048, 10240),
        ::testing::ValuesIn<std::vector<BasicType>>(
            {{BasicType::UINT8},
             {BasicType::INT64, BasicType::UINT64, BasicType::INT8, BasicType::INT16, BasicType::INT32},
             {BasicType::INT64,
              BasicType::INT32,
              BasicType::INT16,
              BasicType::INT8,
              BasicType::UINT64,
              BasicType::UINT32,
              BasicType::UINT16,
              BasicType::UINT8}}),
        ::testing::ValuesIn<std::vector<BasicType>>(
            {{BasicType::INT8},
             {BasicType::INT64,
              BasicType::INT32,
              BasicType::INT16,
              BasicType::INT8,
              BasicType::FLOAT32,
              BasicType::UINT64,
              BasicType::UINT32,
              BasicType::UINT16,
              BasicType::UINT8,
              BasicType::FLOAT64}}),
        ::testing::Values(QueryCompilation::NautilusBackend::COMPILER, QueryCompilation::NautilusBackend::INTERPRETER)),
    [](const testing::TestParamInfo<ChainedHashMapTest::ParamType>& info)
    {
        const auto numberOfItems = std::get<0>(info.param);
        const auto numberOfBuckets = std::get<1>(info.param);
        const auto pageSize = std::get<2>(info.param);
        const auto keyBasicTypes = std::get<3>(info.param);
        const auto valueBasicTypes = std::get<4>(info.param);
        const auto backend = std::get<5>(info.param);

        std::stringstream ss;
        ss << "noI_" << numberOfItems << "_ps_" << pageSize << "_noB_" << numberOfBuckets << "_keyTypes_";
        for (const auto& keyBasicType : keyBasicTypes)
        {
            ss << magic_enum::enum_name(keyBasicType) << "_";
        }
        ss << "valTypes_";
        for (const auto& valueBasicType : valueBasicTypes)
        {
            ss << magic_enum::enum_name(valueBasicType) << "_";
        }
        ss << magic_enum::enum_name(backend);
        return ss.str();
    });
}
