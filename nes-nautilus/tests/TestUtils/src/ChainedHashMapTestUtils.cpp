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
#include <ChainedHashMapTestUtils.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <numeric>
#include <random>
#include <sstream>
#include <tuple>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <NautilusTestUtils.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::TestUtils
{

TestParams::TestParams(const MinMaxValue& minMaxNumberOfItems, const MinMaxValue& minMaxNumberOfBuckets, const MinMaxValue& minMaxPageSize)
{
    /// We log the seed to gain reproducibility of the test
    const auto seed = std::random_device()();
    NES_INFO("Seed for creating test params: {}", seed);
    std::srand(seed);

    numberOfItems = std::rand() % (minMaxNumberOfItems.max - minMaxNumberOfItems.min + 1) + minMaxNumberOfItems.min;
    numberOfBuckets = std::rand() % (minMaxNumberOfBuckets.max - minMaxNumberOfBuckets.min + 1) + minMaxNumberOfBuckets.min;
    pageSize = std::rand() % (minMaxPageSize.max - minMaxPageSize.min + 1) + minMaxPageSize.min;

    NES_INFO("Number of items: {}, number of buckets: {}, page size: {}", numberOfItems, numberOfBuckets, pageSize);
}

void ChainedHashMapTestUtils::setUpChainedHashMapTest(
    const std::vector<DataType::Type>& keyTypes,
    const std::vector<DataType::Type>& valueTypes,
    const Nautilus::Configurations::ExecutionMode backend)
{
    /// Setting the correct options for the engine, depending on the enum value from the backend
    nautilus::engine::Options options;
    const bool compilation = (backend == Nautilus::Configurations::ExecutionMode::COMPILER);
#ifndef USE_MLIR
    if (compilation)
    {
        GTEST_SKIP_("Compiler backend not enabled");
    }
#endif
    NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
    options.setOption("engine.Compilation", compilation);
    nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);

    /// Creating a combined schema with the keys and value types.
    auto inputSchemaKey = TestUtils::NautilusTestUtils::createSchemaFromBasicTypes(keyTypes);
    const auto inputSchemaValue = TestUtils::NautilusTestUtils::createSchemaFromBasicTypes(valueTypes, inputSchemaKey.getNumberOfFields());
    const auto fieldNamesKey = inputSchemaKey.getFieldNames();
    const auto fieldNamesValue = inputSchemaValue.getFieldNames();
    inputSchema = Schema{Schema::MemoryLayoutType::ROW_LAYOUT};
    inputSchema = inputSchemaKey;
    inputSchema.appendFieldsFromOtherSchema(inputSchemaValue);

    /// Setting the hash map configurations
    keySize = inputSchemaKey.getSizeOfSchemaInBytes();
    valueSize = inputSchemaValue.getSizeOfSchemaInBytes();
    entrySize = sizeof(Interface::ChainedHashMapEntry) + keySize + valueSize;
    entriesPerPage = params.pageSize / entrySize;

    /// Creating a buffer manager with a buffer size of 4k (default) and enough buffers to hold all input tuples
    /// We set here a minimum number of buffers. This is a heuristic value and if needed, we can increase it.
    /// We use a minimum number of buffers, as some tests (e.g. customValue) creates more tuple buffers from the buffer manager.
    /// We use 3 calls to create the monotonic values, keys+values, updates values and for the compoundKeys
    constexpr auto bufferSize = 4096;
    constexpr auto minimumBuffers = 4000UL;
    constexpr auto callsToCreateMonotonicValues = 3;
    const auto bufferNeeded
        = callsToCreateMonotonicValues * ((inputSchema.getSizeOfSchemaInBytes() * params.numberOfItems) / bufferSize + 1);
    bufferManager = Memory::BufferManager::create(bufferSize, std::max(bufferNeeded, minimumBuffers));

    /// Creating a tuple buffer memory provider for the key and value buffers
    memoryProviderInputBuffer = Interface::MemoryProvider::TupleBufferMemoryProvider::create(bufferManager->getBufferSize(), inputSchema);

    /// Creating the fields for the key and value from the schema
    std::tie(fieldKeys, fieldValues)
        = Interface::MemoryProvider::ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldNamesKey, fieldNamesValue);

    /// Storing the field names for the key and value
    projectionKeys = inputSchemaKey.getFieldNames();
    projectionValues = inputSchemaValue.getFieldNames();

    /// Creating the buffers with the values for the keys and values with a specific seed
    inputBuffers = createMonotonicallyIncreasingValues(inputSchema, params.numberOfItems, *bufferManager);
}

std::string ChainedHashMapTestUtils::compareExpectedWithActual(
    const Memory::TupleBuffer& inputBufferKeys,
    const Memory::TupleBuffer& bufferActual,
    const std::map<TestUtils::RecordWithFields, Record>& exactMap)
{
    PRECONDITION(
        inputBufferKeys.getNumberOfTuples() == bufferActual.getNumberOfTuples(),
        "The number of tuples is not equal {}<--->{}",
        inputBufferKeys.getNumberOfTuples(),
        bufferActual.getNumberOfTuples());

    const RecordBuffer recordBufferInput(nautilus::val<const Memory::TupleBuffer*>(std::addressof(inputBufferKeys)));
    const RecordBuffer recordBufferActual(nautilus::val<const Memory::TupleBuffer*>(std::addressof(bufferActual)));
    std::stringstream ss;
    for (nautilus::val<uint64_t> i = 0; i < recordBufferInput.getNumRecords(); i = i + 1)
    {
        /// Reading the actual key and value and the exact value
        auto recordKeyActual = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferActual, i);
        auto recordValueActual = memoryProviderInputBuffer->readRecord(projectionValues, recordBufferActual, i);
        auto recordKeyExact = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferInput, i);
        auto recordValueExact = exactMap.find({recordKeyExact, projectionKeys});
        EXPECT_NE(recordValueExact, exactMap.end()) << "Could not find the record with the key";

        /// Populating the error message, if the values are not equal.
        ss << NautilusTestUtils::compareRecords(recordKeyActual, recordKeyExact, projectionKeys);
        ss << NautilusTestUtils::compareRecords(recordValueActual, recordValueExact->second, projectionValues);
    }
    return ss.str();
}

std::string ChainedHashMapTestUtils::compareExpectedWithActual(
    const Memory::TupleBuffer& bufferActual,
    const Interface::MemoryProvider::TupleBufferMemoryProvider& memoryProviderInputBuffer,
    const std::map<TestUtils::RecordWithFields, Record>& exactMap)
{
    PRECONDITION(
        exactMap.size() == bufferActual.getNumberOfTuples(),
        "The number of tuples is not equal {}<--->{}",
        exactMap.size(),
        bufferActual.getNumberOfTuples());

    /// Now we are iterating over all tuples in the bufferOutput and compare them with the exact values from the map.
    std::stringstream ss;
    const RecordBuffer recordBufferActual(nautilus::val<const Memory::TupleBuffer*>(std::addressof(bufferActual)));
    for (nautilus::val<uint64_t> pos = 0; pos < recordBufferActual.getNumRecords(); ++pos)
    {
        /// Reading the actual key and value and the exact value
        auto recordKeyActual = memoryProviderInputBuffer.readRecord(projectionKeys, recordBufferActual, pos);
        auto recordValueActual = memoryProviderInputBuffer.readRecord(projectionValues, recordBufferActual, pos);
        auto recordValueExact = exactMap.find({recordKeyActual, projectionKeys});
        EXPECT_NE(recordValueExact, exactMap.end()) << "Could not find a record for this key";

        /// Populating the error message, if the values are not equal.
        ss << NautilusTestUtils::compareRecords(recordValueActual, recordValueExact->second, projectionValues);
    }
    return ss.str();
}

nautilus::engine::CallableFunction<void, Memory::TupleBuffer*, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
ChainedHashMapTestUtils::compileFindAndWriteToOutputBuffer() const
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    return nautilusEngine->registerFunction(std::function(
        [this](
            nautilus::val<Memory::TupleBuffer*> keyBufferRef,
            nautilus::val<Memory::TupleBuffer*> outputBufferForValues,
            nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<Interface::HashMap*> hashMapVal)
        {
            Interface::ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(keyBufferRef);
            for (nautilus::val<uint64_t> i = 0; i < recordBufferKey.getNumRecords(); i = i + 1)
            {
                auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, i);
                auto foundEntry = hashMapRef.findOrCreateEntry(
                    recordKey, *NautilusTestUtils::getMurMurHashFunction(), ASSERT_VIOLATION_FOR_ON_INSERT, bufferManagerVal);

                const auto castedEntry = static_cast<nautilus::val<Interface::ChainedHashMapEntry*>>(foundEntry);
                const Interface::ChainedHashMapRef::ChainedEntryRef entry(castedEntry, fieldKeys, fieldValues);

                /// Writing the read value from the chained hash map into the buffer.
                Record outputRecord;
                const auto keyRecord = entry.getKey();
                const auto valueRecord = entry.getValue();
                outputRecord.reassignFields(keyRecord);
                outputRecord.reassignFields(valueRecord);

                RecordBuffer recordBufferOutput(outputBufferForValues);
                memoryProviderInputBuffer->writeRecord(i, recordBufferOutput, outputRecord, bufferManagerVal);
                recordBufferOutput.setNumRecords(i + 1);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

nautilus::engine::CallableFunction<void, Memory::TupleBuffer*, Interface::HashMap*, Memory::AbstractBufferProvider*>
ChainedHashMapTestUtils::compileFindAndWriteToOutputBufferWithEntryIterator() const
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    return nautilusEngine->registerFunction(std::function(
        [this](
            nautilus::val<Memory::TupleBuffer*> bufferOutput,
            nautilus::val<Interface::HashMap*> hashMapVal,
            nautilus::val<Memory::AbstractBufferProvider*> bufferProvider)
        {
            const Interface::ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            RecordBuffer recordBufferOutput(bufferOutput);
            nautilus::val<uint64_t> outputBufferIndex(0);
            for (auto entry : hashMapRef)
            {
                /// Writing the read value from the chained hash map into the buffer.
                Record outputRecord;
                const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(entry, fieldKeys, fieldValues);
                const auto keyRecord = entryRef.getKey();
                const auto valueRecord = entryRef.getValue();
                outputRecord.reassignFields(keyRecord);
                outputRecord.reassignFields(valueRecord);
                memoryProviderInputBuffer->writeRecord(outputBufferIndex, recordBufferOutput, outputRecord, bufferProvider);
                outputBufferIndex = outputBufferIndex + nautilus::static_val<uint64_t>(1);
                recordBufferOutput.setNumRecords(outputBufferIndex);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

nautilus::engine::CallableFunction<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
ChainedHashMapTestUtils::compileFindAndInsert() const
{
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    return nautilusEngine->registerFunction(std::function(
        /// ReSharper disable once CppPassValueParameterByConstReference
        [this](
            nautilus::val<Memory::TupleBuffer*> inputBufferRef,
            nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<Interface::HashMap*> hashMapVal)
        {
            Interface::ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            const RecordBuffer recordBuffer(inputBufferRef);
            for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
            {
                auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBuffer, i);
                auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBuffer, i);
                auto foundEntry = hashMapRef.findOrCreateEntry(
                    recordKey,
                    *NautilusTestUtils::getMurMurHashFunction(),
                    [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
                    {
                        const auto castedEntry = static_cast<nautilus::val<Interface::ChainedHashMapEntry*>>(entry);
                        const Interface::ChainedHashMapRef::ChainedEntryRef ref(castedEntry, fieldKeys, fieldValues);
                        ref.copyValuesToEntry(recordValue);
                    },
                    bufferManagerVal);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

nautilus::engine::CallableFunction<void, Memory::TupleBuffer*, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
ChainedHashMapTestUtils::compileFindAndUpdate() const
{
    /// Compiling a function that finds the entry and updates the value.
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    return nautilusEngine->registerFunction(std::function(
        [this](
            nautilus::val<Memory::TupleBuffer*> keyBufferRef,
            nautilus::val<Memory::TupleBuffer*> valueBufferUpdatedRef,
            nautilus::val<Memory::AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<Interface::HashMap*> hashMapVal)
        {
            Interface::ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(keyBufferRef);
            const RecordBuffer recordBufferValue(valueBufferUpdatedRef);

            for (nautilus::val<uint64_t> i = 0; i < recordBufferKey.getNumRecords(); i = i + 1)
            {
                auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBufferKey, i);
                auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBufferValue, i);
                auto foundEntry = hashMapRef.findOrCreateEntry(
                    recordKey,
                    *NautilusTestUtils::getMurMurHashFunction(),
                    [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef ref(entry, fieldKeys, fieldValues);
                        ref.copyValuesToEntry(recordValue);
                    },
                    bufferManagerVal);
                hashMapRef.insertOrUpdateEntry(
                    foundEntry,
                    [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef ref(entry, fieldKeys, fieldValues);
                        ref.copyValuesToEntry(recordValue);
                    },
                    ASSERT_VIOLATION_FOR_ON_INSERT,
                    bufferManagerVal);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)
}

std::map<RecordWithFields, Record> ChainedHashMapTestUtils::createExactMap(const ExactMapInsert exactMapInsert)
{
    std::map<TestUtils::RecordWithFields, Record> exactMap;
    for (const auto& buffer : inputBuffers)
    {
        const RecordBuffer recordBuffer(nautilus::val<const Memory::TupleBuffer*>(std::addressof(buffer)));
        for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
        {
            const auto recordKey = memoryProviderInputBuffer->readRecord(projectionKeys, recordBuffer, i);
            const auto recordValue = memoryProviderInputBuffer->readRecord(projectionValues, recordBuffer, i);

            switch (exactMapInsert)
            {
                case ExactMapInsert::INSERT:
                    exactMap.insert({{recordKey, projectionKeys}, recordValue});
                    break;
                case ExactMapInsert::OVERWRITE:
                    exactMap[{recordKey, projectionKeys}].reassignFields(recordValue);
                    break;
            }
        }
    }

    return exactMap;
}


void ChainedHashMapTestUtils::checkIfValuesAreCorrectViaFindEntry(
    Interface::ChainedHashMap& hashMap, const std::map<RecordWithFields, Record>& exactMap)
{
    /// Ensuring that the number of tuples is correct.
    ASSERT_EQ(hashMap.getNumberOfTuples(), exactMap.size());

    /// Calling now the compiled function to write all values of the map to the output buffer.
    const auto numberOfInputTuples = std::accumulate(
        inputBuffers.begin(), inputBuffers.end(), 0, [](const auto& sum, const auto& buffer) { return sum + buffer.getNumberOfTuples(); });
    auto bufferOutputOpt = bufferManager->getUnpooledBuffer(numberOfInputTuples * inputSchema.getSizeOfSchemaInBytes());
    if (not bufferOutputOpt)
    {
        NES_ERROR("Could not allocate buffer for size {}", numberOfInputTuples * inputSchema.getSizeOfSchemaInBytes());
        ASSERT_TRUE(false);
    }
    auto bufferOutput = bufferOutputOpt.value();
    std::memset(bufferOutput.getBuffer(), 0, bufferOutput.getBufferSize());

    /// We are calling the function to find all entries and write them to the output buffer.
    auto findAndWriteToOutputBuffer = compileFindAndWriteToOutputBufferWithEntryIterator();
    findAndWriteToOutputBuffer(std::addressof(bufferOutput), std::addressof(hashMap), bufferManager.get());

    /// Checking if the number of items are equal to the number of items in the exact map.
    ASSERT_EQ(bufferOutput.getNumberOfTuples(), exactMap.size());
    const auto errorMessage = compareExpectedWithActual(bufferOutput, *memoryProviderInputBuffer, exactMap);
    if (not errorMessage.empty())
    {
        EXPECT_TRUE(false) << errorMessage;
    }
}

void ChainedHashMapTestUtils::checkEntryIterator(
    Interface::ChainedHashMap& hashMap, const std::map<TestUtils::RecordWithFields, Record>& exactMap)
{
    /// Ensuring that the number of tuples is correct.
    ASSERT_EQ(hashMap.getNumberOfTuples(), exactMap.size());
    auto findAndWriteToOutputBuffer = compileFindAndWriteToOutputBuffer();
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
        findAndWriteToOutputBuffer(std::addressof(inputBuffer), std::addressof(bufferOutput), bufferManager.get(), std::addressof(hashMap));

        /// Checking if the number of items are equal to the number of items in the exact map.
        const auto errorStream = compareExpectedWithActual(inputBuffer, bufferOutput, exactMap);
        if (not errorStream.empty())
        {
            EXPECT_TRUE(false) << errorStream;
        }
    }
}

}
