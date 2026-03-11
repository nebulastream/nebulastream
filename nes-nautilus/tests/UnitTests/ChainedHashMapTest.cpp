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
#include <cstring>
#include <functional>
#include <iterator>
#include <map>
#include <memory>
#include <random>
#include <ranges>
#include <sstream>
#include <tuple>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>

#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
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
    uint64_t numberOfItems{}, numberOfBuckets{}, pageSize{};
};

/// Test context that replaces ChainedHashMapTestUtils and ChainedHashMapCustomValueTestUtils.
/// All compile methods and setup logic are inlined here.
struct TestContext
{
    TestParams params;
    std::shared_ptr<BufferManager> bufferManager;
    std::unique_ptr<nautilus::engine::NautilusEngine> nautilusEngine;
    Schema inputSchema;
    std::vector<FieldOffsets> fieldKeys, fieldValues;
    std::vector<Record::RecordFieldIdentifier> projectionKeys, projectionValues;
    std::vector<TupleBuffer> inputBuffers;
    std::shared_ptr<TupleBufferRef> inputBufferRef;
    uint64_t keySize{}, valueSize{}, entriesPerPage{}, entrySize{};

    void setUp(const std::vector<DataType::Type>& keyTypes, const std::vector<DataType::Type>& valueTypes, ExecutionMode backend)
    {
        /// Setting the correct options for the engine, depending on the enum value from the backend
        nautilus::engine::Options options;
        const bool compilation = (backend == ExecutionMode::COMPILER);
        NES_INFO("Backend: {} and compilation: {}", magic_enum::enum_name(backend), compilation);
        options.setOption("engine.Compilation", compilation);
        options.setOption("mlir.enableMultithreading", false);
        nautilusEngine = std::make_unique<nautilus::engine::NautilusEngine>(options);

        /// Creating a combined schema with the keys and value types.
        auto inputSchemaKey = NautilusTestUtils::createSchemaFromBasicTypes(keyTypes);
        const auto inputSchemaValue = NautilusTestUtils::createSchemaFromBasicTypes(valueTypes, inputSchemaKey.getNumberOfFields());
        const auto fieldNamesKey = inputSchemaKey.getFieldNames();
        const auto fieldNamesValue = inputSchemaValue.getFieldNames();
        inputSchema = inputSchemaKey;
        inputSchema.appendFieldsFromOtherSchema(inputSchemaValue);

        /// Setting the hash map configurations
        keySize = inputSchemaKey.getSizeOfSchemaInBytes();
        valueSize = inputSchemaValue.getSizeOfSchemaInBytes();
        entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
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
        bufferManager = BufferManager::create(bufferSize, std::max(bufferNeeded, minimumBuffers));

        /// Creating a tuple buffer memory provider for the key and value buffers
        inputBufferRef = LowerSchemaProvider::lowerSchema(bufferManager->getBufferSize(), inputSchema, MemoryLayoutType::ROW_LAYOUT);

        /// Creating the fields for the key and value from the schema
        std::tie(fieldKeys, fieldValues) = ChainedEntryMemoryProvider::createFieldOffsets(inputSchema, fieldNamesKey, fieldNamesValue);

        /// Storing the field names for the key and value
        projectionKeys = inputSchemaKey.getFieldNames();
        projectionValues = inputSchemaValue.getFieldNames();

        /// Creating the buffers with the values for the keys and values
        NautilusTestUtils nautilusTestUtils;
        inputBuffers = nautilusTestUtils.createMonotonicallyIncreasingValues(
            inputSchema, MemoryLayoutType::ROW_LAYOUT, params.numberOfItems, *bufferManager);
    }

    /// Compiles a function that writes all keys and values to bufferOutput using the EntryIterator.
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, HashMap*, AbstractBufferProvider*>
    compileFindAndWriteToOutputBufferWithEntryIterator() const
    {
        /// NOLINTBEGIN(performance-unnecessary-value-param)
        return nautilusEngine->registerFunction(std::function(
            [this](
                nautilus::val<TupleBuffer*> bufferOutput,
                nautilus::val<HashMap*> hashMapVal,
                nautilus::val<AbstractBufferProvider*> bufferProvider)
            {
                const ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                RecordBuffer recordBufferOutput(bufferOutput);
                nautilus::val<uint64_t> outputBufferIndex(0);
                for (auto entry : hashMapRef)
                {
                    Record outputRecord;
                    const ChainedHashMapRef::ChainedEntryRef entryRef(entry, hashMapVal, fieldKeys, fieldValues);
                    const auto keyRecord = entryRef.getKey();
                    const auto valueRecord = entryRef.getValue();
                    outputRecord.reassignFields(keyRecord);
                    outputRecord.reassignFields(valueRecord);
                    inputBufferRef->writeRecord(outputBufferIndex, recordBufferOutput, outputRecord, bufferProvider);
                    outputBufferIndex = outputBufferIndex + nautilus::static_val<uint64_t>(1);
                    recordBufferOutput.setNumRecords(outputBufferIndex);
                }
            }));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    /// Compiles a function that finds the entry and inserts the value using findOrCreateEntry().
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, AbstractBufferProvider*, HashMap*> compileFindAndInsert() const
    {
        /// NOLINTBEGIN(performance-unnecessary-value-param)
        return nautilusEngine->registerFunction(std::function(
            [this](
                nautilus::val<TupleBuffer*> inputBufferPtr,
                nautilus::val<AbstractBufferProvider*> bufferManagerVal,
                nautilus::val<HashMap*> hashMapVal)
            {
                ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                const RecordBuffer recordBuffer(inputBufferPtr);
                for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
                {
                    auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBuffer, i);
                    auto recordValue = inputBufferRef->readRecord(projectionValues, recordBuffer, i);
                    auto foundEntry = hashMapRef.findOrCreateEntry(
                        recordKey,
                        *NautilusTestUtils::getMurMurHashFunction(),
                        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                        {
                            const auto castedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(entry);
                            const ChainedHashMapRef::ChainedEntryRef ref(castedEntry, hashMapVal, fieldKeys, fieldValues);
                            ref.copyValuesToEntry(recordValue, bufferManagerVal);
                        },
                        bufferManagerVal);
                }
            }));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    /// Compiles a function that finds the entry and updates the value using findOrCreateEntry() + insertOrUpdateEntry().
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, TupleBuffer*, AbstractBufferProvider*, HashMap*>
    compileFindAndUpdate() const
    {
        /// NOLINTBEGIN(performance-unnecessary-value-param)
        return nautilusEngine->registerFunction(std::function(
            [this](
                nautilus::val<TupleBuffer*> keyBufferRef,
                nautilus::val<TupleBuffer*> valueBufferUpdatedRef,
                nautilus::val<AbstractBufferProvider*> bufferManagerVal,
                nautilus::val<HashMap*> hashMapVal)
            {
                ChainedHashMapRef hashMapRef(hashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                const RecordBuffer recordBufferKey(keyBufferRef);
                const RecordBuffer recordBufferValue(valueBufferUpdatedRef);

                for (nautilus::val<uint64_t> i = 0; i < recordBufferKey.getNumRecords(); i = i + 1)
                {
                    auto recordKey = inputBufferRef->readRecord(projectionKeys, recordBufferKey, i);
                    auto recordValue = inputBufferRef->readRecord(projectionValues, recordBufferValue, i);
                    auto foundEntry = hashMapRef.findOrCreateEntry(
                        recordKey,
                        *NautilusTestUtils::getMurMurHashFunction(),
                        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                        {
                            const ChainedHashMapRef::ChainedEntryRef ref(entry, hashMapVal, fieldKeys, fieldValues);
                            ref.copyValuesToEntry(recordValue, bufferManagerVal);
                        },
                        bufferManagerVal);
                    hashMapRef.insertOrUpdateEntry(
                        foundEntry,
                        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
                        {
                            const ChainedHashMapRef::ChainedEntryRef ref(entry, hashMapVal, fieldKeys, fieldValues);
                            ref.copyValuesToEntry(recordValue, bufferManagerVal);
                        },
                        ASSERT_VIOLATION_FOR_ON_INSERT,
                        bufferManagerVal);
                }
            }));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    /// Compiles a function that merges all entries from sourceHashMap into destHashMap via insertOrUpdateEntry (last-write-wins).
    [[nodiscard]] nautilus::engine::CallableFunction<void, HashMap*, HashMap*, AbstractBufferProvider*> compileMerge() const
    {
        /// NOLINTBEGIN(performance-unnecessary-value-param)
        return nautilusEngine->registerFunction(std::function(
            [this](
                nautilus::val<HashMap*> sourceHashMapVal,
                nautilus::val<HashMap*> destHashMapVal,
                nautilus::val<AbstractBufferProvider*> bufferManagerVal)
            {
                const ChainedHashMapRef sourceRef(sourceHashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                ChainedHashMapRef destRef(destHashMapVal, fieldKeys, fieldValues, entriesPerPage, entrySize);
                for (const auto entry : sourceRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRef(entry, sourceHashMapVal, fieldKeys, fieldValues);
                    destRef.insertOrUpdateEntry(
                        entryRef.entryRef,
                        [&](const nautilus::val<AbstractHashMapEntry*>& destEntry)
                        {
                            /// onUpdate: overwrite values in dest with values from source
                            const ChainedHashMapRef::ChainedEntryRef destEntryRef(destEntry, destHashMapVal, fieldKeys, fieldValues);
                            destEntryRef.copyValuesToEntry(entryRef.getValue(), bufferManagerVal);
                        },
                        [&](const nautilus::val<AbstractHashMapEntry*>& destEntry)
                        {
                            /// onInsert: copy keys and values into the new dest entry
                            const ChainedHashMapRef::ChainedEntryRef destEntryRef(destEntry, destHashMapVal, fieldKeys, fieldValues);
                            destEntryRef.copyKeysToEntry(entryRef.getKey(), bufferManagerVal);
                            destEntryRef.copyValuesToEntry(entryRef.getValue(), bufferManagerVal);
                        },
                        bufferManagerVal);
                }
            }));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    /// Compiles a method that finds a key in the hash map and inserts all records from the value buffer into the paged vector.
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, TupleBuffer*, uint64_t, AbstractBufferProvider*, HashMap*>
    compileFindAndInsertIntoPagedVector(const std::vector<Record::RecordFieldIdentifier>& projectionAllFields) const
    {
        return nautilusEngine->registerFunction(std::function(
            [this, projectionAllFields](
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
                                /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
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
    }

    /// Compiles a function that iterates over all keys and writes all values for one key to the output buffer.
    [[nodiscard]] nautilus::engine::CallableFunction<void, TupleBuffer*, uint64_t, TupleBuffer*, AbstractBufferProvider*, HashMap*>
    compileWriteAllRecordsIntoOutputBuffer(const std::vector<Record::RecordFieldIdentifier>& projectionAllFields) const
    {
        /// NOLINTBEGIN(performance-unnecessary-value-param)
        return nautilusEngine->registerFunction(std::function(
            [this, projectionAllFields](
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
                const auto foundEntry
                    = hashMapRef.findOrCreateEntry(recordKey, *NautilusTestUtils::getMurMurHashFunction(), ASSERT_VIOLATION_FOR_ON_INSERT, bufferManagerVal);

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
    }
};

/// Runs the pagedVector property for a specific backend.
/// Tests inserting compound keys with PagedVector as the custom value (multimap behavior).
void pagedVectorProperty(ExecutionMode backend, size_t maxTotalFields)
{
    const auto keyTypes = *genTypeSchema(ALL_KEY_TYPES, 1, maxTotalFields);
    const auto valueTypes = *genTypeSchema(ALL_VALUE_TYPES, 1, maxTotalFields);
    RC_PRE(estimateSchemaSize(keyTypes) + estimateSchemaSize(valueTypes) < BUFFER_SIZE);
    RC_PRE(keyTypes.size() + valueTypes.size() <= maxTotalFields);

    NES_INFO(
        "Property test pagedVector: keyFields={}, valueFields={}, backend={}",
        keyTypes.size(),
        valueTypes.size(),
        magic_enum::enum_name(backend));

    TestContext ctx;
    /// Reduced item counts because the pagedVector test iterates over all buffers x all buffers.
    ctx.params.numberOfItems = *rc::gen::inRange<uint64_t>(50, 201);
    ctx.params.numberOfBuckets = *rc::gen::inRange<uint64_t>(1, 257);
    ctx.params.pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    NES_INFO(
        "RC params: numberOfItems={}, numberOfBuckets={}, pageSize={}",
        ctx.params.numberOfItems,
        ctx.params.numberOfBuckets,
        ctx.params.pageSize);
    ctx.setUp(keyTypes, valueTypes, backend);

    /// Creating the destructor callback for each item, i.e. keys and PagedVector
    auto destructorCallback = [&](const ChainedHashMapEntry* entry)
    {
        const auto* memArea = reinterpret_cast<const int8_t*>(entry) + sizeof(ChainedHashMapEntry) + ctx.keySize;
        const auto* pagedVector = reinterpret_cast<const PagedVector*>(memArea);
        pagedVector->~PagedVector();
    };

    /// Resetting the entriesPerPage, as we have a paged vector as the value.
    ctx.valueSize = sizeof(PagedVector);
    const auto totalSizeOfEntry = sizeof(ChainedHashMapEntry) + ctx.keySize + ctx.valueSize;
    ctx.entriesPerPage = ctx.params.pageSize / totalSizeOfEntry;

    auto hashMap = ChainedHashMap(ctx.keySize, ctx.valueSize, ctx.params.numberOfBuckets, ctx.params.pageSize);
    hashMap.setDestructorCallback(destructorCallback);
    RC_ASSERT(hashMap.getNumberOfTuples() == 0);

    /// Create a projection for all fields
    std::vector<Record::RecordFieldIdentifier> projectionAllFields;
    std::ranges::copy(ctx.projectionKeys, std::back_inserter(projectionAllFields));
    std::ranges::copy(ctx.projectionValues, std::back_inserter(projectionAllFields));
    auto findAndInsertIntoPagedVector = ctx.compileFindAndInsertIntoPagedVector(projectionAllFields);

    /// Generate all keyPositionInBuffer values via RapidCheck for full reproducibility.
    std::vector<uint64_t> allKeyPositions;
    allKeyPositions.reserve(ctx.inputBuffers.size());
    for (const auto& bufferKey : ctx.inputBuffers)
    {
        allKeyPositions.push_back(*rc::gen::inRange<uint64_t>(0, bufferKey.getNumberOfTuples()));
    }

    /// Insert into hash map and build exact multimap for verification
    std::multimap<TestUtils::RecordWithFields, Record> exactMap;
    size_t bufferIdx = 0;
    for (auto& bufferKey : ctx.inputBuffers)
    {
        const auto keyPositionInBuffer = allKeyPositions[bufferIdx++];

        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(bufferKey)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = ctx.inputBufferRef->readRecord(ctx.projectionKeys, recordBufferKey, keyPositionInBufferVal);

        for (auto& bufferValue : ctx.inputBuffers)
        {
            findAndInsertIntoPagedVector(
                std::addressof(bufferKey),
                std::addressof(bufferValue),
                keyPositionInBuffer,
                ctx.bufferManager.get(),
                std::addressof(hashMap));

            const RecordBuffer recordBufferValue(nautilus::val<const TupleBuffer*>(std::addressof(bufferValue)));
            for (nautilus::val<uint64_t> i = 0; i < recordBufferValue.getNumRecords(); i = i + 1)
            {
                auto recordValue = ctx.inputBufferRef->readRecord(projectionAllFields, recordBufferValue, i);
                exactMap.insert({{recordKey, ctx.projectionKeys}, recordValue});
            }
        }
    }

    /// Verify all values via lookup
    auto writeAllRecordsIntoOutputBuffer = ctx.compileWriteAllRecordsIntoOutputBuffer(projectionAllFields);
    for (auto [buffer, keyPositionInBuffer] : std::views::zip(ctx.inputBuffers, allKeyPositions))
    {
        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(buffer)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = ctx.inputBufferRef->readRecord(ctx.projectionKeys, recordBufferKey, keyPositionInBufferVal);

        auto [recordValueExactStart, recordValueExactEnd] = exactMap.equal_range({recordKey, ctx.projectionKeys});
        const auto numberOfRecordsExact = std::distance(recordValueExactStart, recordValueExactEnd);

        const auto neededBytes = ctx.inputBufferRef->getTupleSize() * numberOfRecordsExact;
        auto outputBufferOpt = ctx.bufferManager->getUnpooledBuffer(neededBytes);
        RC_ASSERT(outputBufferOpt.has_value());
        auto outputBuffer = outputBufferOpt.value();

        writeAllRecordsIntoOutputBuffer(
            std::addressof(buffer), keyPositionInBuffer, std::addressof(outputBuffer), ctx.bufferManager.get(), std::addressof(hashMap));

        RC_ASSERT(outputBuffer.getNumberOfTuples() == static_cast<uint64_t>(numberOfRecordsExact));

        nautilus::val<uint64_t> currentPosition = 0;
        for (auto exactIt = recordValueExactStart; exactIt != recordValueExactEnd; ++exactIt)
        {
            const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
            auto recordValueActual = ctx.inputBufferRef->readRecord(projectionAllFields, recordBufferOutput, currentPosition);
            const auto errorMessage = TestUtils::NautilusTestUtils::compareRecords(recordValueActual, exactIt->second, projectionAllFields);
            RC_ASSERT(!errorMessage.has_value());
            ++currentPosition;
        }
    }

    hashMap.clear();
}

/// Insert-and-update property: for each buffer, randomly chooses between findAndInsert
/// (first-insert-wins) and findAndUpdate (last-write-wins), then verifies bidirectionally.
void insertAndUpdateProperty(ExecutionMode backend, uint64_t minItems, uint64_t maxItems)
{
    const auto numberOfItems = *rc::gen::inRange(minItems, maxItems + 1);
    const auto keyTypes = *genTypeSchema(ALL_KEY_TYPES, 1, 4);
    const auto valueTypes = *genTypeSchema(ALL_VALUE_TYPES, 1, 4);
    RC_PRE(estimateSchemaSize(keyTypes) + estimateSchemaSize(valueTypes) < BUFFER_SIZE);

    NES_INFO(
        "Property test insertAndUpdate: N={}, keyFields={}, valueFields={}, backend={}",
        numberOfItems,
        keyTypes.size(),
        valueTypes.size(),
        magic_enum::enum_name(backend));

    TestContext ctx;
    ctx.params.numberOfItems = numberOfItems;
    ctx.params.numberOfBuckets = *rc::gen::inRange<uint64_t>(64, 2049);
    ctx.params.pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    NES_INFO(
        "RC params: numberOfItems={}, numberOfBuckets={}, pageSize={}",
        ctx.params.numberOfItems,
        ctx.params.numberOfBuckets,
        ctx.params.pageSize);
    ctx.setUp(keyTypes, valueTypes, backend);

    ChainedHashMap hashMap{ctx.keySize, ctx.valueSize, ctx.params.numberOfBuckets, ctx.params.pageSize};
    RC_ASSERT(hashMap.getNumberOfTuples() == 0);

    /// For each buffer, randomly decide: INSERT (findAndInsert) or UPDATE (findAndUpdate).
    /// For update operations, optionally use a different buffer as the value source.
    enum class Op : uint8_t
    {
        INSERT,
        UPDATE
    };
    struct BufferOp
    {
        size_t keyBufferIdx;
        size_t valueBufferIdx; /// only used for UPDATE
        Op op;
    };

    const auto numBuffers = ctx.inputBuffers.size();
    std::vector<BufferOp> ops;
    for (size_t i = 0; i < numBuffers; ++i)
    {
        const auto useUpdate = *rc::gen::inRange(0, 2);
        if (useUpdate)
        {
            const auto valueIdx = *rc::gen::inRange<size_t>(0, numBuffers);
            ops.push_back({i, valueIdx, Op::UPDATE});
        }
        else
        {
            ops.push_back({i, i, Op::INSERT});
        }
    }

    /// Build the reference map according to the chosen operations.
    const AnyVecLess keyComparator{keyTypes};
    std::map<std::vector<std::any>, std::vector<std::any>, AnyVecLess> referenceMap(keyComparator);
    for (const auto& [keyIdx, valueIdx, op] : ops)
    {
        const auto& keyBuffer = ctx.inputBuffers[keyIdx];
        const auto& valueBuffer = ctx.inputBuffers[valueIdx];
        const auto tuplesToProcess = std::min(keyBuffer.getNumberOfTuples(), valueBuffer.getNumberOfTuples());
        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(keyBuffer)));
        const RecordBuffer recordBufferValue(nautilus::val<const TupleBuffer*>(std::addressof(valueBuffer)));
        for (nautilus::val<uint64_t> i = 0; i < tuplesToProcess; i = i + 1)
        {
            auto recordKey = ctx.inputBufferRef->readRecord(ctx.projectionKeys, recordBufferKey, i);
            auto recordValue = ctx.inputBufferRef->readRecord(ctx.projectionValues, recordBufferValue, i);
            auto keyVec = recordToAnyVec(recordKey, ctx.projectionKeys, keyTypes);
            auto valueVec = recordToAnyVec(recordValue, ctx.projectionValues, valueTypes);
            switch (op)
            {
                case Op::INSERT:
                    /// findAndInsert uses findOrCreate: first insert wins.
                    referenceMap.try_emplace(std::move(keyVec), std::move(valueVec));
                    break;
                case Op::UPDATE:
                    /// findAndUpdate uses findOrCreate + insertOrUpdateEntry: last write wins.
                    referenceMap[std::move(keyVec)] = std::move(valueVec);
                    break;
            }
        }
    }

    /// Execute the operations against the ChainedHashMap.
    auto findAndInsert = ctx.compileFindAndInsert();
    auto findAndUpdate = ctx.compileFindAndUpdate();
    for (const auto& [keyIdx, valueIdx, op] : ops)
    {
        auto& keyBuffer = ctx.inputBuffers[keyIdx];
        auto& valueBuffer = ctx.inputBuffers[valueIdx];
        const auto tuplesToProcess = std::min(keyBuffer.getNumberOfTuples(), valueBuffer.getNumberOfTuples());
        const auto originalKeyCount = keyBuffer.getNumberOfTuples();
        keyBuffer.setNumberOfTuples(tuplesToProcess);
        switch (op)
        {
            case Op::INSERT:
                findAndInsert(std::addressof(keyBuffer), ctx.bufferManager.get(), std::addressof(hashMap));
                break;
            case Op::UPDATE:
                findAndUpdate(std::addressof(keyBuffer), std::addressof(valueBuffer), ctx.bufferManager.get(), std::addressof(hashMap));
                break;
        }
        keyBuffer.setNumberOfTuples(originalKeyCount);
    }

    NES_INFO(
        "insertAndUpdate: ChainedHashMap has {} entries, reference map has {} entries",
        hashMap.getNumberOfTuples(),
        referenceMap.size());
    RC_ASSERT(hashMap.getNumberOfTuples() == referenceMap.size());

    /// === Forward check: every entry in ChainedHashMap must be in the reference map ===
    auto outputBufferOpt = ctx.bufferManager->getUnpooledBuffer(hashMap.getNumberOfTuples() * ctx.inputSchema.getSizeOfSchemaInBytes());
    RC_ASSERT(outputBufferOpt.has_value());
    auto outputBuffer = outputBufferOpt.value();
    std::ranges::fill(outputBuffer.getAvailableMemoryArea(), std::byte{0});

    auto entryIteratorFn = ctx.compileFindAndWriteToOutputBufferWithEntryIterator();
    entryIteratorFn(std::addressof(outputBuffer), std::addressof(hashMap), ctx.bufferManager.get());
    RC_ASSERT(outputBuffer.getNumberOfTuples() == hashMap.getNumberOfTuples());

    std::set<std::vector<std::any>, AnyVecLess> keysFoundInHashMap(keyComparator);
    const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
    for (nautilus::val<uint64_t> pos = 0; pos < recordBufferOutput.getNumRecords(); ++pos)
    {
        auto keyRecord = ctx.inputBufferRef->readRecord(ctx.projectionKeys, recordBufferOutput, pos);
        auto valueRecord = ctx.inputBufferRef->readRecord(ctx.projectionValues, recordBufferOutput, pos);
        auto keyVec = recordToAnyVec(keyRecord, ctx.projectionKeys, keyTypes);
        auto valueVec = recordToAnyVec(valueRecord, ctx.projectionValues, valueTypes);

        auto it = referenceMap.find(keyVec);
        RC_ASSERT(it != referenceMap.end());
        RC_ASSERT(anyVecsEqual(it->second, valueVec, valueTypes));
        keysFoundInHashMap.insert(std::move(keyVec));
    }

    /// === Reverse check: every entry in the reference map must be in the ChainedHashMap ===
    for (const auto& [key, value] : referenceMap)
    {
        RC_ASSERT(keysFoundInHashMap.contains(key));
    }
}

/// Merge property: creates N source hashmaps (each populated with a random subset of buffers),
/// then merges them all into a single destination hashmap via insertOrUpdateEntry (last-write-wins).
/// Mirrors the aggregation probe operator pattern: createNewMapWithSameConfiguration + merge.
void mergeProperty(ExecutionMode backend, uint64_t minItems, uint64_t maxItems)
{
    const auto numberOfItems = *rc::gen::inRange(minItems, maxItems + 1);
    const auto keyTypes = *genTypeSchema(ALL_KEY_TYPES, 1, 4);
    const auto valueTypes = *genTypeSchema(ALL_VALUE_TYPES, 1, 4);
    RC_PRE(estimateSchemaSize(keyTypes) + estimateSchemaSize(valueTypes) < BUFFER_SIZE);

    NES_INFO(
        "Property test merge: N={}, keyFields={}, valueFields={}, backend={}",
        numberOfItems,
        keyTypes.size(),
        valueTypes.size(),
        magic_enum::enum_name(backend));

    TestContext ctx;
    ctx.params.numberOfItems = numberOfItems;
    ctx.params.numberOfBuckets = *rc::gen::inRange<uint64_t>(64, 2049);
    ctx.params.pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    NES_INFO(
        "RC params: numberOfItems={}, numberOfBuckets={}, pageSize={}",
        ctx.params.numberOfItems,
        ctx.params.numberOfBuckets,
        ctx.params.pageSize);
    ctx.setUp(keyTypes, valueTypes, backend);

    /// Randomly partition input buffers across 2-4 source hashmaps.
    const auto numBuffers = ctx.inputBuffers.size();
    const auto numSourceMaps = *rc::gen::inRange<size_t>(2, std::max<size_t>(3, std::min<size_t>(5, numBuffers + 1)));
    std::vector<std::vector<size_t>> partitions(numSourceMaps);
    for (size_t i = 0; i < numBuffers; ++i)
    {
        const auto targetMap = *rc::gen::inRange<size_t>(0, numSourceMaps);
        partitions[targetMap].push_back(i);
    }

    /// Populate source hashmaps via findAndInsert.
    std::vector<ChainedHashMap> sourceMaps;
    sourceMaps.reserve(numSourceMaps);
    for (size_t m = 0; m < numSourceMaps; ++m)
    {
        sourceMaps.emplace_back(ctx.keySize, ctx.valueSize, ctx.params.numberOfBuckets, ctx.params.pageSize);
    }
    auto findAndInsert = ctx.compileFindAndInsert();
    for (size_t m = 0; m < numSourceMaps; ++m)
    {
        for (const auto bufIdx : partitions[m])
        {
            findAndInsert(std::addressof(ctx.inputBuffers[bufIdx]), ctx.bufferManager.get(), std::addressof(sourceMaps[m]));
        }
    }

    /// Build reference map: first-insert-wins within each source map (findAndInsert semantics),
    /// then last-write-wins across source maps (insertOrUpdateEntry merge semantics).
    const AnyVecLess keyComparator{keyTypes};
    std::map<std::vector<std::any>, std::vector<std::any>, AnyVecLess> referenceMap(keyComparator);
    for (size_t m = 0; m < numSourceMaps; ++m)
    {
        /// Per-source reference map: first-insert-wins (matching findAndInsert / findOrCreateEntry).
        std::map<std::vector<std::any>, std::vector<std::any>, AnyVecLess> sourceRefMap(keyComparator);
        for (const auto bufIdx : partitions[m])
        {
            const auto& buffer = ctx.inputBuffers[bufIdx];
            const RecordBuffer recordBuffer(nautilus::val<const TupleBuffer*>(std::addressof(buffer)));
            for (nautilus::val<uint64_t> i = 0; i < recordBuffer.getNumRecords(); i = i + 1)
            {
                auto recordKey = ctx.inputBufferRef->readRecord(ctx.projectionKeys, recordBuffer, i);
                auto recordValue = ctx.inputBufferRef->readRecord(ctx.projectionValues, recordBuffer, i);
                auto keyVec = recordToAnyVec(recordKey, ctx.projectionKeys, keyTypes);
                auto valueVec = recordToAnyVec(recordValue, ctx.projectionValues, valueTypes);
                sourceRefMap.try_emplace(std::move(keyVec), std::move(valueVec));
            }
        }
        /// Merge into global reference: last-write-wins (matching insertOrUpdateEntry semantics).
        for (auto& [key, value] : sourceRefMap)
        {
            referenceMap[key] = std::move(value);
        }
    }

    /// Create destination hashmap and merge all source maps into it.
    ChainedHashMap destMap{ctx.keySize, ctx.valueSize, ctx.params.numberOfBuckets, ctx.params.pageSize};
    auto mergeFn = ctx.compileMerge();
    for (auto& sourceMap : sourceMaps)
    {
        if (sourceMap.getNumberOfTuples() == 0)
        {
            continue;
        }
        mergeFn(std::addressof(sourceMap), std::addressof(destMap), ctx.bufferManager.get());
    }

    NES_INFO("merge: destMap has {} entries, reference map has {} entries", destMap.getNumberOfTuples(), referenceMap.size());
    RC_ASSERT(destMap.getNumberOfTuples() == referenceMap.size());

    /// === Forward check: every entry in destMap must be in the reference map ===
    auto outputBufferOpt = ctx.bufferManager->getUnpooledBuffer(destMap.getNumberOfTuples() * ctx.inputSchema.getSizeOfSchemaInBytes());
    RC_ASSERT(outputBufferOpt.has_value());
    auto outputBuffer = outputBufferOpt.value();
    std::ranges::fill(outputBuffer.getAvailableMemoryArea(), std::byte{0});

    auto entryIteratorFn = ctx.compileFindAndWriteToOutputBufferWithEntryIterator();
    entryIteratorFn(std::addressof(outputBuffer), std::addressof(destMap), ctx.bufferManager.get());
    RC_ASSERT(outputBuffer.getNumberOfTuples() == destMap.getNumberOfTuples());

    std::set<std::vector<std::any>, AnyVecLess> keysFoundInHashMap(keyComparator);
    const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
    for (nautilus::val<uint64_t> pos = 0; pos < recordBufferOutput.getNumRecords(); ++pos)
    {
        auto keyRecord = ctx.inputBufferRef->readRecord(ctx.projectionKeys, recordBufferOutput, pos);
        auto valueRecord = ctx.inputBufferRef->readRecord(ctx.projectionValues, recordBufferOutput, pos);
        auto keyVec = recordToAnyVec(keyRecord, ctx.projectionKeys, keyTypes);
        auto valueVec = recordToAnyVec(valueRecord, ctx.projectionValues, valueTypes);

        auto it = referenceMap.find(keyVec);
        RC_ASSERT(it != referenceMap.end());
        RC_ASSERT(anyVecsEqual(it->second, valueVec, valueTypes));
        keysFoundInHashMap.insert(std::move(keyVec));
    }

    /// === Reverse check: every entry in the reference map must be in destMap ===
    for (const auto& [key, value] : referenceMap)
    {
        RC_ASSERT(keysFoundInHashMap.contains(key));
    }
}

/// By default, RapidCheck runs 100 iterations per property. Each iteration generates
/// a random combination of key types (1-256 fields), value types (1-256 fields),
/// number of items, buckets, and page size.
/// To adjust, set RC_PARAMS="max_success=20" in the environment.
///
/// Interpreter tests allow up to 512 total fields (fast execution).
/// Compiler tests allow up to 64 total fields (JIT compilation is expensive for wide schemas).
/// PagedVector interpreter tests use a reduced limit because the O(buffers^2) insert loop is expensive.
constexpr size_t MAX_TOTAL_FIELDS_INTERPRETER = 512;
constexpr size_t MAX_TOTAL_FIELDS_COMPILER = 64;
constexpr size_t MAX_TOTAL_FIELDS_PAGED_VECTOR_INTERPRETER = 128;

} /// anonymous namespace

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

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndUpdateInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndUpdateProperty(ExecutionMode::INTERPRETER, 1000, 10000);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, insertAndUpdateCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    insertAndUpdateProperty(ExecutionMode::COMPILER, 1000, 5000);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, mergeInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    mergeProperty(ExecutionMode::INTERPRETER, 1000, 10000);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, mergeCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    mergeProperty(ExecutionMode::COMPILER, 1000, 5000);
}

}
