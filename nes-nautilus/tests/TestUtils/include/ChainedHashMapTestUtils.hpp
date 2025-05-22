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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <API/Schema.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/NautilusBackend.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <NautilusTestUtils.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::Nautilus::TestUtils
{


/// This function should be used in the tests whenever we want to ensure that a specific value is not inserted/created again in the hash map
#define ASSERT_VIOLATION_FOR_ON_INSERT \
    [&](const nautilus::val<Nautilus::Interface::AbstractHashMapEntry*>&) \
    { \
        nautilus::invoke(+[]() \
                         { \
                             NES_ERROR("Should not insert a value here"); \
                             ASSERT_TRUE(false); \
                         }); \
    }

/// Parameter for one test configuration
struct TestParams
{
    TestParams() = default;
    TestParams(const MinMaxValue& minMaxNumberOfItems, const MinMaxValue& minMaxNumberOfBuckets, const MinMaxValue& minMaxPageSize);
    uint64_t numberOfItems{}, numberOfBuckets{}, pageSize{};
    std::vector<std::shared_ptr<Schema>> keyDataTypes, valueDataTypes;
};

class ChainedHashMapTestUtils : public TestUtils::NautilusTestUtils
{
public:
    std::shared_ptr<Memory::BufferManager> bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    std::shared_ptr<Schema> inputSchema;
    std::vector<Interface::MemoryProvider::FieldOffsets> fieldKeys, fieldValues;
    std::vector<Record::RecordFieldIdentifier> projectionKeys, projectionValues;
    std::vector<Memory::TupleBuffer> inputBuffers;
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProviderInputBuffer;
    uint64_t keySize, valueSize, entriesPerPage, entrySize;
    TestParams params;

    enum ExactMapInsert : uint8_t
    {
        INSERT,
        OVERWRITE
    };

    void setUpChainedHashMapTest(
        const std::vector<BasicType>& keyTypes,
        const std::vector<BasicType>& valueTypes,
        Nautilus::Configurations::NautilusBackend backend);

    std::string compareExpectedWithActual(
        const Memory::TupleBuffer& inputBufferKeys,
        const Memory::TupleBuffer& bufferActual,
        const std::map<TestUtils::RecordWithFields, Record>& exactMap);

    std::string compareExpectedWithActual(
        const Memory::TupleBuffer& bufferActual,
        const Interface::MemoryProvider::TupleBufferMemoryProvider& memoryProviderInputBuffer,
        const std::map<TestUtils::RecordWithFields, Record>& exactMap);

    /// Compiles the query that writes the values for all keys in keyBufferRef to outputBufferForKeys.
    /// This enables us to perform a comparison in the c++ code by comparing every value in the record buffer with the exact value.
    /// We are using findOrCreateEntry() of the hash map interface.
    [[nodiscard]] nautilus::engine::
        CallableFunction<void, Memory::TupleBuffer*, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
        compileFindAndWriteToOutputBuffer() const;

    /// Compiles a function that writes all keys and values to bufferOutput.
    /// To iterate over all key and values, we use the entry iterator. We assume that the bufferOutput is large enough to hold all values.
    /// We are using our EntryIterator of the chained hash map.
    [[nodiscard]] nautilus::engine::CallableFunction<void, Memory::TupleBuffer*, Interface::HashMap*, Memory::AbstractBufferProvider*>
    compileFindAndWriteToOutputBufferWithEntryIterator() const;


    /// Compiles a function that finds the entry and updates the value.
    /// This enables us to perform a comparison in the c++ code by comparing every value in the record buffer with the exact value.
    /// We are using the findOrCreateEntry() of the hash map interface.
    [[nodiscard]] nautilus::engine::CallableFunction<void, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
    compileFindAndInsert() const;

    /// Compiles a function that finds the entry and updates the value.
    /// This enables us to perform a comparison in the c++ code by comparing every value in the record buffer with the exact value.
    /// We are using the findOrCreateEntry() followed by a insertOrUpdateEntry() of the hash map interface.
    [[nodiscard]] nautilus::engine::
        CallableFunction<void, Memory::TupleBuffer*, Memory::TupleBuffer*, Memory::AbstractBufferProvider*, Interface::HashMap*>
        compileFindAndUpdate() const;

    /// Creates an exact map of the inputBuffers.
    /// If overwriteIfExisting is true, we will overwrite an existing key in the map.
    std::map<RecordWithFields, Record> createExactMap(ExactMapInsert exactMapInsert);

    /// Checks if the values in the hash map are correct by comparing them with the exact map.
    /// We call the compiled function to write all values of the map to the output buffer via the EntryIterator
    void checkEntryIterator(Interface::ChainedHashMap& hashMap, const std::map<RecordWithFields, Record>& exactMap);

    /// Checks if the values in the hash map are correct by comparing them with the exact map.
    /// We call the compiled function that finds all entries and writes them to the output buffer.
    void checkIfValuesAreCorrectViaFindEntry(Interface::ChainedHashMap& hashMap, const std::map<RecordWithFields, Record>& exactMap);
};

}
