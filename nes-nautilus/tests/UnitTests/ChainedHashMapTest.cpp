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
#include <any>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <ranges>
#include <set>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/Engine.hpp>
#include <NautilusTestUtils.hpp>
#include <PropertyTestUtils.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_details.hpp>
#include <val_ptr.hpp>

#include <rapidcheck.h>
#include <rapidcheck/gtest.h>

namespace NES
{

/// NOLINTNEXTLINE(google-build-using-namespace) test code benefits from concise access to test utilities
using namespace TestUtils;

namespace
{

// ===================== TestableChainedHashMap (inlined) =====================

/// Mirrors std::map<AnyVec, AnyVec> but internally operates on a real ChainedHashMap via direct ChainedHashMapRef calls.
class TestableChainedHashMap
{
public:
    TestableChainedHashMap(
        const std::vector<DataType>& keyTypes,
        const std::vector<DataType>& valueTypes,
        uint64_t numberOfBuckets,
        uint64_t pageSize,
        std::shared_ptr<BufferManager> bufferManager)
        : keyDataTypes_(keyTypes),
          valueDataTypes_(valueTypes),
          hashMap_(0, 0, numberOfBuckets, pageSize),
          bufferManager_(std::move(bufferManager))
    {
        auto inputSchemaKey = NautilusTestUtils::createSchemaFromDataTypes(keyDataTypes_);
        const auto inputSchemaValue = NautilusTestUtils::createSchemaFromDataTypes(valueDataTypes_, inputSchemaKey.getNumberOfFields());
        const auto fieldNamesKey = inputSchemaKey.getFieldNames();
        const auto fieldNamesValue = inputSchemaValue.getFieldNames();
        auto inputSchema = inputSchemaKey;
        inputSchema.appendFieldsFromOtherSchema(inputSchemaValue);

        const auto keySize = inputSchemaKey.getSizeOfSchemaInBytes();
        const auto valueSize = inputSchemaValue.getSizeOfSchemaInBytes();
        entrySize_ = sizeof(ChainedHashMapEntry) + keySize + valueSize;
        entriesPerPage_ = pageSize / entrySize_;

        hashMap_ = ChainedHashMap(keySize, valueSize, numberOfBuckets, pageSize);

        std::tie(fieldKeys_, fieldValues_) = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldNamesKey, fieldNamesValue);
        projectionKeys_ = inputSchemaKey.getFieldNames();
        projectionValues_ = inputSchemaValue.getFieldNames();
    }

    void findOrCreate(const AnyVec& key, const AnyVec& value)
    {
        auto recordKey = anyVecToRecord(key, projectionKeys_, keyDataTypes_);
        auto recordValue = anyVecToRecord(value, projectionValues_, valueDataTypes_);
        nautilus::val<HashMap*> hmPtr(&hashMap_);
        ChainedHashMapRef hashMapRef(hmPtr, fieldKeys_, fieldValues_, entriesPerPage_, entrySize_);
        hashMapRef.findOrCreateEntry(
            recordKey,
            *NautilusTestUtils::getMurMurHashFunction(),
            [&](nautilus::val<AbstractHashMapEntry*>& entry)
            {
                const ChainedHashMapRef::ChainedEntryRef ref(static_cast<nautilus::val<ChainedHashMapEntry*>>(entry), hmPtr, fieldKeys_, fieldValues_);
                ref.copyValuesToEntry(recordValue, nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
            },
            nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
    }

    void insertOrUpdate(const AnyVec& key, const AnyVec& value)
    {
        auto recordKey = anyVecToRecord(key, projectionKeys_, keyDataTypes_);
        auto recordValue = anyVecToRecord(value, projectionValues_, valueDataTypes_);
        nautilus::val<HashMap*> hmPtr(&hashMap_);
        ChainedHashMapRef hashMapRef(hmPtr, fieldKeys_, fieldValues_, entriesPerPage_, entrySize_);
        auto foundEntry = hashMapRef.findOrCreateEntry(
            recordKey,
            *NautilusTestUtils::getMurMurHashFunction(),
            [&](nautilus::val<AbstractHashMapEntry*>& entry)
            {
                const ChainedHashMapRef::ChainedEntryRef ref(static_cast<nautilus::val<ChainedHashMapEntry*>>(entry), hmPtr, fieldKeys_, fieldValues_);
                ref.copyValuesToEntry(recordValue, nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
            },
            nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
        hashMapRef.insertOrUpdateEntry(
            foundEntry,
            [&](nautilus::val<AbstractHashMapEntry*>& entry)
            {
                const ChainedHashMapRef::ChainedEntryRef ref(entry, hmPtr, fieldKeys_, fieldValues_);
                ref.copyValuesToEntry(recordValue, nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
            },
            [&](nautilus::val<AbstractHashMapEntry*>&)
            {
                nautilus::invoke(
                    +[]()
                    {
                        NES_ERROR("Should not insert a value here");
                        std::abort();
                    });
            },
            nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
    }

    void mergeFrom(TestableChainedHashMap& source)
    {
        if (source.hashMap_.getNumberOfTuples() == 0)
        {
            return;
        }
        nautilus::val<HashMap*> srcPtr(&source.hashMap_);
        nautilus::val<HashMap*> dstPtr(&hashMap_);
        const ChainedHashMapRef sourceRef(srcPtr, fieldKeys_, fieldValues_, entriesPerPage_, entrySize_);
        ChainedHashMapRef destRef(dstPtr, fieldKeys_, fieldValues_, entriesPerPage_, entrySize_);
        for (const auto entry : sourceRef)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef(entry, srcPtr, fieldKeys_, fieldValues_);
            destRef.insertOrUpdateEntry(
                entryRef.entryRef,
                [&](nautilus::val<AbstractHashMapEntry*>& destEntry)
                {
                    const ChainedHashMapRef::ChainedEntryRef destEntryRef(destEntry, dstPtr, fieldKeys_, fieldValues_);
                    destEntryRef.copyValuesToEntry(entryRef.getValue(), nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
                },
                [&](nautilus::val<AbstractHashMapEntry*>& destEntry)
                {
                    const ChainedHashMapRef::ChainedEntryRef destEntryRef(destEntry, dstPtr, fieldKeys_, fieldValues_);
                    destEntryRef.copyKeysToEntry(entryRef.getKey(), nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
                    destEntryRef.copyValuesToEntry(entryRef.getValue(), nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
                },
                nautilus::val<AbstractBufferProvider*>(bufferManager_.get()));
        }
    }

    AnyVecMap toMap() const
    {
        nautilus::val<HashMap*> hmPtr(const_cast<HashMap*>(static_cast<const HashMap*>(&hashMap_)));
        const ChainedHashMapRef hashMapRef(hmPtr, fieldKeys_, fieldValues_, entriesPerPage_, entrySize_);
        AnyVecMap result(AnyVecLess{keyDataTypes_});
        for (auto entry : hashMapRef)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef(entry, hmPtr, fieldKeys_, fieldValues_);
            auto keyRecord = entryRef.getKey();
            auto valueRecord = entryRef.getValue();
            auto keyVec = recordToAnyVec(keyRecord, projectionKeys_, keyDataTypes_);
            auto valueVec = recordToAnyVec(valueRecord, projectionValues_, valueDataTypes_);
            result.emplace(std::move(keyVec), std::move(valueVec));
        }
        return result;
    }

    uint64_t size() const { return hashMap_.getNumberOfTuples(); }

    ChainedHashMap& raw() { return hashMap_; }

private:
    std::vector<DataType> keyDataTypes_, valueDataTypes_;
    ChainedHashMap hashMap_;
    std::shared_ptr<BufferManager> bufferManager_;
    std::vector<FieldOffsets> fieldKeys_, fieldValues_;
    std::vector<Record::RecordFieldIdentifier> projectionKeys_, projectionValues_;
    uint64_t entriesPerPage_, entrySize_;
};

// ===================== Helpers =====================

/// Writes genAnyVec records into a TupleBuffer, returning the records and the filled buffer.
std::pair<std::vector<std::pair<AnyVec, AnyVec>>, TupleBuffer> fillBuffer(
    const std::vector<DataType>& keyTypes,
    const std::vector<DataType>& valueTypes,
    const std::vector<Record::RecordFieldIdentifier>& projectionKeys,
    const std::vector<Record::RecordFieldIdentifier>& projectionValues,
    const std::shared_ptr<TupleBufferRef>& bufferRef,
    BufferManager& bufferManager,
    uint64_t count)
{
    auto buffer = bufferManager.getBufferBlocking();
    std::vector<std::pair<AnyVec, AnyVec>> records;
    records.reserve(count);

    RecordBuffer recordBuffer(nautilus::val<TupleBuffer*>(std::addressof(buffer)));
    for (nautilus::val<uint64_t> i = 0; i < count; ++i)
    {
        auto key = *genAnyVec(keyTypes);
        auto value = *genAnyVec(valueTypes);
        Record record;
        for (const auto& [val, field, type] : std::views::zip(key, projectionKeys, keyTypes))
        {
            record.write(field, anyToVarVal(val, type));
        }
        for (const auto& [val, field, type] : std::views::zip(value, projectionValues, valueTypes))
        {
            record.write(field, anyToVarVal(val, type));
        }
        bufferRef->writeRecord(i, recordBuffer, record, nautilus::val<AbstractBufferProvider*>(&bufferManager));
        recordBuffer.setNumRecords(i + 1);
        records.emplace_back(std::move(key), std::move(value));
    }
    buffer.setNumberOfTuples(count);
    return {std::move(records), buffer};
}

// ===================== Property Scenarios =====================

/// Insert-and-update property: for each record, randomly chooses between findOrCreate
/// (first-insert-wins) and insertOrUpdate (last-write-wins), then verifies bidirectionally.
void insertAndUpdateProperty()
{
    const auto keyTypes = *genDataTypeSchema(ALL_KEY_TYPES, 1, 4);
    const auto valueTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 4);
    RC_PRE(estimateSchemaSize(keyTypes) + estimateSchemaSize(valueTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(50, 501);
    const auto numberOfBuckets = *rc::gen::inRange<uint64_t>(16, 257);
    const auto pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);

    NES_INFO(
        "Property test insertAndUpdate: N={}, keyFields={}, valueFields={}",
        numberOfItems, keyTypes.size(), valueTypes.size());

    auto bufferManager = BufferManager::create();
    TestableChainedHashMap map(keyTypes, valueTypes, numberOfBuckets, pageSize, bufferManager);

    AnyVecMap referenceMap(AnyVecLess{keyTypes});
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto key = *genAnyVec(keyTypes);
        auto value = *genAnyVec(valueTypes);
        const auto useUpdate = *rc::gen::inRange(0, 2);
        if (useUpdate)
        {
            map.insertOrUpdate(key, value);
            referenceMap[key] = value;
        }
        else
        {
            map.findOrCreate(key, value);
            referenceMap.try_emplace(key, value);
        }
    }

    NES_INFO("insertAndUpdate: map has {} entries, reference has {} entries", map.size(), referenceMap.size());
    RC_ASSERT(map.size() == referenceMap.size());

    auto actual = map.toMap();
    RC_ASSERT(actual.size() == referenceMap.size());
    for (const auto& [key, value] : actual)
    {
        auto it = referenceMap.find(key);
        RC_ASSERT(it != referenceMap.end());
        RC_ASSERT(anyVecsEqual(it->second, value, valueTypes));
    }
    for (const auto& [key, value] : referenceMap)
    {
        RC_ASSERT(actual.contains(key));
    }
}

/// Merge property: creates N source hashmaps (each populated with random records via findOrCreate),
/// then merges them all into a single destination hashmap via insertOrUpdateEntry (last-write-wins).
void mergeProperty()
{
    const auto keyTypes = *genDataTypeSchema(ALL_KEY_TYPES, 1, 4);
    const auto valueTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, 4);
    RC_PRE(estimateSchemaSize(keyTypes) + estimateSchemaSize(valueTypes) < BUFFER_SIZE);

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(50, 501);
    const auto numberOfBuckets = *rc::gen::inRange<uint64_t>(16, 257);
    const auto pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    const auto numSourceMaps = *rc::gen::inRange<size_t>(2, 5);

    NES_INFO(
        "Property test merge: N={}, keyFields={}, valueFields={}, numSources={}",
        numberOfItems, keyTypes.size(), valueTypes.size(), numSourceMaps);

    auto bufferManager = BufferManager::create();

    /// Create source hashmaps and populate via record-at-a-time findOrCreate
    std::vector<TestableChainedHashMap> sourceMaps;
    std::vector<AnyVecMap> sourceRefMaps;
    sourceMaps.reserve(numSourceMaps);
    sourceRefMaps.reserve(numSourceMaps);

    for (size_t m = 0; m < numSourceMaps; ++m)
    {
        sourceMaps.emplace_back(keyTypes, valueTypes, numberOfBuckets, pageSize, bufferManager);
        sourceRefMaps.emplace_back(AnyVecLess{keyTypes});
        const auto itemsInSource = *rc::gen::inRange<uint64_t>(10, numberOfItems + 1);
        for (uint64_t i = 0; i < itemsInSource; ++i)
        {
            auto key = *genAnyVec(keyTypes);
            auto value = *genAnyVec(valueTypes);
            sourceMaps.back().findOrCreate(key, value);
            sourceRefMaps.back().try_emplace(key, value);
        }
    }

    /// Build reference: first-insert-wins within each source, last-write-wins across sources
    AnyVecMap referenceMap(AnyVecLess{keyTypes});
    for (auto& sourceRefMap : sourceRefMaps)
    {
        for (auto& [key, value] : sourceRefMap)
        {
            referenceMap[key] = std::move(value);
        }
    }

    /// Merge all source maps into destination
    TestableChainedHashMap destMap(keyTypes, valueTypes, numberOfBuckets, pageSize, bufferManager);
    for (auto& sourceMap : sourceMaps)
    {
        destMap.mergeFrom(sourceMap);
    }

    NES_INFO("merge: destMap has {} entries, reference map has {} entries", destMap.size(), referenceMap.size());
    RC_ASSERT(destMap.size() == referenceMap.size());

    auto actual = destMap.toMap();
    RC_ASSERT(actual.size() == referenceMap.size());
    for (const auto& [key, value] : actual)
    {
        auto it = referenceMap.find(key);
        RC_ASSERT(it != referenceMap.end());
        RC_ASSERT(anyVecsEqual(it->second, value, valueTypes));
    }
    for (const auto& [key, value] : referenceMap)
    {
        RC_ASSERT(actual.contains(key));
    }
}

// ===================== PagedVector as custom value test =====================

/// Helper to make NOT_NULLABLE DataTypes from a type pool (only needed for pagedVectorProperty
/// which uses RecordWithFields-based multimap verification that doesn't support nullable keys).
std::vector<DataType> genNonNullableSchema(const std::vector<DataType::Type>& pool, size_t minFields, size_t maxFields)
{
    const auto numFields = *rc::gen::inRange(minFields, maxFields + 1);
    std::vector<DataType> schema;
    schema.reserve(numFields);
    for (size_t i = 0; i < numFields; ++i)
    {
        const auto idx = *rc::gen::inRange(static_cast<size_t>(0), pool.size());
        schema.emplace_back(pool[idx], DataType::NULLABLE::NOT_NULLABLE);
    }
    return schema;
}

void pagedVectorProperty(ExecutionMode backend, size_t maxTotalFields)
{
    const auto keyTypes = genNonNullableSchema(ALL_KEY_TYPES, 1, maxTotalFields);
    const auto valueTypes = genNonNullableSchema(ALL_VALUE_TYPES, 1, maxTotalFields);
    RC_PRE(keyTypes.size() + valueTypes.size() <= maxTotalFields);

    /// Set up schemas and projections
    auto inputSchemaKey = NautilusTestUtils::createSchemaFromDataTypes(keyTypes);
    const auto inputSchemaValue = NautilusTestUtils::createSchemaFromDataTypes(valueTypes, inputSchemaKey.getNumberOfFields());
    auto inputSchema = inputSchemaKey;
    inputSchema.appendFieldsFromOtherSchema(inputSchemaValue);
    RC_PRE(inputSchema.getSizeOfSchemaInBytes() < BUFFER_SIZE);
    const auto fieldNamesKey = inputSchemaKey.getFieldNames();
    const auto fieldNamesValue = inputSchemaValue.getFieldNames();

    NES_INFO(
        "Property test pagedVector: keyFields={}, valueFields={}, backend={}",
        keyTypes.size(), valueTypes.size(), magic_enum::enum_name(backend));

    const auto numberOfItems = *rc::gen::inRange<uint64_t>(20, 51);
    const auto numberOfBuckets = *rc::gen::inRange<uint64_t>(1, 257);
    const auto pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    NES_INFO(
        "RC params: numberOfItems={}, numberOfBuckets={}, pageSize={}",
        numberOfItems, numberOfBuckets, pageSize);

    const auto keySize = inputSchemaKey.getSizeOfSchemaInBytes();

    constexpr auto bufferSize = 4096;
    constexpr auto minimumBuffers = 4000UL;
    const auto bufferNeeded = 3 * ((inputSchema.getSizeOfSchemaInBytes() * numberOfItems) / bufferSize + 1);
    auto bufferManager = BufferManager::create(bufferSize, std::max(bufferNeeded, minimumBuffers));

    auto inputBufferRef = LowerSchemaProvider::lowerSchema(bufferManager->getBufferSize(), inputSchema, MemoryLayoutType::ROW_LAYOUT);
    auto [fieldKeys, fieldValues] = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldNamesKey, fieldNamesValue);
    auto projectionKeys = inputSchemaKey.getFieldNames();
    auto projectionValues = inputSchemaValue.getFieldNames();

    nautilus::engine::Options options;
    options.setOption("engine.Compilation", backend == ExecutionMode::COMPILER);
    options.setOption("mlir.enableMultithreading", false);
    auto nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);

    /// Compute entry layout with PagedVector as value
    const auto pvValueSize = static_cast<uint64_t>(sizeof(PagedVector));
    const auto totalSizeOfEntry = sizeof(ChainedHashMapEntry) + keySize + pvValueSize;
    const auto entriesPerPage = pageSize / totalSizeOfEntry;
    const auto entrySize = totalSizeOfEntry;

    auto destructorCallback = [&](const ChainedHashMapEntry* entry)
    {
        const auto* memArea = reinterpret_cast<const int8_t*>(entry) + sizeof(ChainedHashMapEntry) + keySize;
        const auto* pagedVector = reinterpret_cast<const PagedVector*>(memArea);
        pagedVector->~PagedVector();
    };

    auto hashMap = ChainedHashMap(keySize, pvValueSize, numberOfBuckets, pageSize);
    hashMap.setDestructorCallback(destructorCallback);
    RC_ASSERT(hashMap.getNumberOfTuples() == 0);

    std::vector<Record::RecordFieldIdentifier> projectionAllFields;
    std::ranges::copy(projectionKeys, std::back_inserter(projectionAllFields));
    std::ranges::copy(projectionValues, std::back_inserter(projectionAllFields));

    /// Compile findAndInsertIntoPagedVector function
    auto findAndInsertIntoPagedVector = nautilusEngine->registerFunction(std::function(
        [inputBufferRef, fieldKeys, fieldValues, projectionKeys, projectionValues, projectionAllFields, entriesPerPage, entrySize](
            nautilus::val<TupleBuffer*> bufferKey,
            nautilus::val<TupleBuffer*> bufferValue,
            nautilus::val<uint64_t> keyPositionVal,
            nautilus::val<AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<HashMap*> hashMapVal)
        {
            ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(bufferKey);
            auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, keyPositionVal);
            auto foundEntry = hashMapRef.findOrCreateEntry(
                recordKey,
                *NautilusTestUtils::getMurMurHashFunction(),
                [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                {
                    const ChainedHashMapRef::ChainedEntryRef ref(entry, hashMapVal, fieldKeys, fieldValues);
                    nautilus::invoke(
                        +[](int8_t* pagedVectorMemArea)
                        {
                            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                            new (pagedVector) PagedVector();
                        },
                        ref.getValueMemArea());
                },
                bufferManagerVal);
            const ChainedHashMapRef::ChainedEntryRef ref(foundEntry, hashMapVal, fieldKeys, fieldValues);
            const PagedVectorRef pagedVectorRef(ref.getValueMemArea(), inputBufferRef);
            const RecordBuffer recordBufferValue(bufferValue);
            for (nautilus::val<uint64_t> idxValues = 0; idxValues < recordBufferValue.getNumRecords(); idxValues = idxValues + 1)
            {
                auto recordValue = inputBufferRef->readRecord(projectionAllFields, recordBufferValue, idxValues);
                pagedVectorRef.writeRecord(recordValue, bufferManagerVal);
            }
        }));

    /// Compile writeAllRecordsIntoOutputBuffer function
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto writeAllRecordsIntoOutputBuffer = nautilusEngine->registerFunction(std::function(
        [inputBufferRef, fieldKeys, fieldValues, projectionKeys, projectionAllFields, entriesPerPage, entrySize](
            nautilus::val<TupleBuffer*> keyBufferRef,
            nautilus::val<uint64_t> keyPositionVal,
            nautilus::val<TupleBuffer*> outputBufferRef,
            nautilus::val<AbstractBufferProvider*> bufferManagerVal,
            nautilus::val<HashMap*> hashMapVal)
        {
            ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, {}, entriesPerPage, entrySize);
            const RecordBuffer recordBufferKey(keyBufferRef);
            RecordBuffer recordBufferOutput(outputBufferRef);
            const auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, keyPositionVal);

            auto foundEntry = hashMapRef.findOrCreateEntry(
                recordKey,
                *NautilusTestUtils::getMurMurHashFunction(),
                [&](const nautilus::val<AbstractHashMapEntry*>&)
                {
                    nautilus::invoke(
                        +[]()
                        {
                            NES_ERROR("Should not insert a value here");
                            ASSERT_TRUE(false);
                        });
                },
                bufferManagerVal);

            const ChainedHashMapRef::ChainedEntryRef ref(foundEntry, hashMapVal, fieldKeys, fieldValues);
            const PagedVectorRef pagedVectorRef(ref.getValueMemArea(), inputBufferRef);
            nautilus::val<uint64_t> recordBufferIndex = 0;
            for (auto it = pagedVectorRef.begin(projectionAllFields); it != pagedVectorRef.end(projectionAllFields); ++it)
            {
                const auto record = *it;
                inputBufferRef->writeRecord(recordBufferIndex, recordBufferOutput, record, bufferManagerVal);
                recordBufferIndex = recordBufferIndex + 1;
                recordBufferOutput.setNumRecords(recordBufferIndex);
            }
        }));
    /// NOLINTEND(performance-unnecessary-value-param)

    /// Generate input buffers using fillBuffer (replaces createMonotonicallyIncreasingValues)
    const auto capacity = inputBufferRef->getCapacity();
    RC_PRE(capacity > 0);
    std::vector<TupleBuffer> inputBuffers;
    {
        uint64_t remaining = numberOfItems;
        while (remaining > 0)
        {
            const auto count = std::min(remaining, capacity);
            auto [records, buffer] = fillBuffer(keyTypes, valueTypes, projectionKeys, projectionValues, inputBufferRef, *bufferManager, count);
            inputBuffers.push_back(buffer);
            remaining -= count;
        }
    }

    std::vector<uint64_t> allKeyPositions;
    allKeyPositions.reserve(inputBuffers.size());
    for (const auto& bufferKey : inputBuffers)
    {
        allKeyPositions.push_back(*rc::gen::inRange<uint64_t>(0, bufferKey.getNumberOfTuples()));
    }

    /// Insert into hash map and build exact multimap for verification
    std::multimap<TestUtils::RecordWithFields, Record> exactMap;
    size_t bufferIdx = 0;
    for (auto& bufferKey : inputBuffers)
    {
        const auto keyPositionInBuffer = allKeyPositions[bufferIdx++];
        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(bufferKey)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, keyPositionInBufferVal);

        for (auto& bufferValue : inputBuffers)
        {
            findAndInsertIntoPagedVector(
                std::addressof(bufferKey),
                std::addressof(bufferValue),
                keyPositionInBuffer,
                bufferManager.get(),
                std::addressof(hashMap));

            const RecordBuffer recordBufferValue(nautilus::val<const TupleBuffer*>(std::addressof(bufferValue)));
            for (nautilus::val<uint64_t> i = 0; i < recordBufferValue.getNumRecords(); i = i + 1)
            {
                auto recordValue = inputBufferRef->readRecord(projectionAllFields, recordBufferValue, i);
                exactMap.insert({{recordKey, projectionKeys}, recordValue});
            }
        }
    }

    /// Verify all values via lookup
    for (auto [buffer, keyPositionInBuffer] : std::views::zip(inputBuffers, allKeyPositions))
    {
        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(buffer)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, keyPositionInBufferVal);

        auto [recordValueExactStart, recordValueExactEnd] = exactMap.equal_range({recordKey, projectionKeys});
        const auto numberOfRecordsExact = std::distance(recordValueExactStart, recordValueExactEnd);

        const auto neededBytes = inputBufferRef->getTupleSize() * numberOfRecordsExact;
        auto outputBufferOpt = bufferManager->getUnpooledBuffer(neededBytes);
        RC_ASSERT(outputBufferOpt.has_value());
        auto outputBuffer = outputBufferOpt.value();

        writeAllRecordsIntoOutputBuffer(
            std::addressof(buffer), keyPositionInBuffer, std::addressof(outputBuffer), bufferManager.get(), std::addressof(hashMap));
        RC_ASSERT(outputBuffer.getNumberOfTuples() == static_cast<uint64_t>(numberOfRecordsExact));

        nautilus::val<uint64_t> currentPosition = 0;
        for (auto exactIt = recordValueExactStart; exactIt != recordValueExactEnd; ++exactIt)
        {
            const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
            auto recordValueActual = inputBufferRef->readRecord(projectionAllFields, recordBufferOutput, currentPosition);
            const auto errorMessage = TestUtils::NautilusTestUtils::compareRecords(recordValueActual, exactIt->second, projectionAllFields);
            RC_ASSERT(!errorMessage.has_value());
            ++currentPosition;
        }
    }

    hashMap.clear();
}

constexpr size_t MAX_TOTAL_FIELDS_COMPILER = 64;
constexpr size_t MAX_TOTAL_FIELDS_PAGED_VECTOR_INTERPRETER = 16;

} /// anonymous namespace

// ===================== Test Macros =====================

RC_GTEST_PROP(ChainedHashMapPropertyTest, pagedVectorInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    pagedVectorProperty(ExecutionMode::INTERPRETER, MAX_TOTAL_FIELDS_PAGED_VECTOR_INTERPRETER);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, pagedVectorCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    pagedVectorProperty(ExecutionMode::COMPILER, MAX_TOTAL_FIELDS_COMPILER);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndUpdate, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndUpdateProperty();
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, merge, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    mergeProperty();
}

}
