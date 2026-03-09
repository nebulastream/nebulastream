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
#include <memory>
#include <string>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <NautilusTestUtils.hpp>

namespace NES::TestUtils
{


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

/// Parameter for one test configuration
struct TestParams
{
    TestParams() = default;
    TestParams(const MinMaxValue& minMaxNumberOfItems, const MinMaxValue& minMaxNumberOfBuckets, const MinMaxValue& minMaxPageSize);
    uint64_t numberOfItems{}, numberOfBuckets{}, pageSize{};
    std::vector<Schema> keyDataTypes, valueDataTypes;
};

class ChainedHashMapTestUtils : public TestUtils::NautilusTestUtils
{
public:
    std::shared_ptr<BufferManager> bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    Schema inputSchema;
    std::vector<FieldOffsets> fieldKeys, fieldValues;
    std::vector<Record::RecordFieldIdentifier> projectionKeys, projectionValues;
    std::vector<TupleBuffer> inputBuffers;
    std::shared_ptr<TupleBufferRef> inputBufferRef;
    uint64_t keySize, valueSize, entriesPerPage, entrySize;
    TestParams params;

    void setUpChainedHashMapTest(
        const std::vector<DataType::Type>& keyTypes, const std::vector<DataType::Type>& valueTypes, ExecutionMode backend);

    /// Compiles a function that writes all keys and values to bufferOutput.
    /// To iterate over all key and values, we use the entry iterator. We assume that the bufferOutput is large enough to hold all values.
    /// We are using our EntryIterator of the chained hash map.
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, HashMap*, AbstractBufferProvider*>
    compileFindAndWriteToOutputBufferWithEntryIterator() const;

    /// Compiles a function that finds the entry and inserts the value.
    /// We are using the findOrCreateEntry() of the hash map interface.
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, AbstractBufferProvider*, HashMap*> compileFindAndInsert() const;

    /// Compiles a function that finds the entry and updates the value.
    /// We are using the findOrCreateEntry() followed by a insertOrUpdateEntry() of the hash map interface.
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, TupleBuffer*, AbstractBufferProvider*, HashMap*>
    compileFindAndUpdate() const;
};

}
