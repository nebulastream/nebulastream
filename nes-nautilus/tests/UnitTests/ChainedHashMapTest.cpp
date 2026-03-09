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
#include <cstdlib>
#include <cstring>
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
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/Engine.hpp>
#include <ChainedHashMapCustomValueTestUtils.hpp>
#include <ChainedHashMapTestUtils.hpp>
#include <NautilusTestUtils.hpp>
#include <PropertyTestUtils.hpp>
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

/// Helper that configures ChainedHashMapCustomValueTestUtils for a given combination.
/// Uses smaller item counts since the pagedVector test has O(buffers^2) complexity.
/// All random parameters are generated via RapidCheck for full reproducibility.
void configureCustomValueTestUtils(
    TestUtils::ChainedHashMapCustomValueTestUtils& utils,
    const std::vector<DataType::Type>& keyTypes,
    const std::vector<DataType::Type>& valueTypes,
    ExecutionMode backend)
{
    /// Reduced item counts because the pagedVector test iterates over all buffers × all buffers.
    utils.params.numberOfItems = *rc::gen::inRange<uint64_t>(50, 201);
    utils.params.numberOfBuckets = *rc::gen::inRange<uint64_t>(1, 257);
    utils.params.pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    NES_INFO(
        "RC params: numberOfItems={}, numberOfBuckets={}, pageSize={}",
        utils.params.numberOfItems,
        utils.params.numberOfBuckets,
        utils.params.pageSize);
    utils.setUpChainedHashMapTest(keyTypes, valueTypes, backend);
}

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

    TestUtils::ChainedHashMapCustomValueTestUtils utils;
    configureCustomValueTestUtils(utils, keyTypes, valueTypes, backend);

    /// Creating the destructor callback for each item, i.e. keys and PagedVector
    auto destructorCallback = [&](const ChainedHashMapEntry* entry)
    {
        const auto* memArea = reinterpret_cast<const int8_t*>(entry) + sizeof(ChainedHashMapEntry) + utils.keySize;
        const auto* pagedVector = reinterpret_cast<const PagedVector*>(memArea);
        pagedVector->~PagedVector();
    };

    /// Resetting the entriesPerPage, as we have a paged vector as the value.
    utils.valueSize = sizeof(PagedVector);
    const auto totalSizeOfEntry = sizeof(ChainedHashMapEntry) + utils.keySize + utils.valueSize;
    utils.entriesPerPage = utils.params.pageSize / totalSizeOfEntry;

    auto hashMap = ChainedHashMap(utils.keySize, utils.valueSize, utils.params.numberOfBuckets, utils.params.pageSize);
    hashMap.setDestructorCallback(destructorCallback);
    RC_ASSERT(hashMap.getNumberOfTuples() == 0);

    /// Create a projection for all fields
    std::vector<Record::RecordFieldIdentifier> projectionAllFields;
    std::ranges::copy(utils.projectionKeys, std::back_inserter(projectionAllFields));
    std::ranges::copy(utils.projectionValues, std::back_inserter(projectionAllFields));
    auto findAndInsertIntoPagedVector = utils.compileFindAndInsertIntoPagedVector(projectionAllFields);

    /// Generate all keyPositionInBuffer values via RapidCheck for full reproducibility.
    std::vector<uint64_t> allKeyPositions;
    allKeyPositions.reserve(utils.inputBuffers.size());
    for (const auto& bufferKey : utils.inputBuffers)
    {
        allKeyPositions.push_back(*rc::gen::inRange<uint64_t>(0, bufferKey.getNumberOfTuples()));
    }

    /// Insert into hash map and build exact multimap for verification
    std::multimap<TestUtils::RecordWithFields, Record> exactMap;
    size_t bufferIdx = 0;
    for (auto& bufferKey : utils.inputBuffers)
    {
        const auto keyPositionInBuffer = allKeyPositions[bufferIdx++];

        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(bufferKey)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = utils.inputBufferRef->readRecord(utils.projectionKeys, recordBufferKey, keyPositionInBufferVal);

        for (auto& bufferValue : utils.inputBuffers)
        {
            findAndInsertIntoPagedVector(
                std::addressof(bufferKey),
                std::addressof(bufferValue),
                keyPositionInBuffer,
                utils.bufferManager.get(),
                std::addressof(hashMap));

            const RecordBuffer recordBufferValue(nautilus::val<const TupleBuffer*>(std::addressof(bufferValue)));
            for (nautilus::val<uint64_t> i = 0; i < recordBufferValue.getNumRecords(); i = i + 1)
            {
                auto recordValue = utils.inputBufferRef->readRecord(projectionAllFields, recordBufferValue, i);
                exactMap.insert({{recordKey, utils.projectionKeys}, recordValue});
            }
        }
    }

    /// Verify all values via lookup
    auto writeAllRecordsIntoOutputBuffer = utils.compileWriteAllRecordsIntoOutputBuffer(projectionAllFields);
    for (auto [buffer, keyPositionInBuffer] : std::views::zip(utils.inputBuffers, allKeyPositions))
    {
        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(buffer)));
        nautilus::val<uint64_t> keyPositionInBufferVal = keyPositionInBuffer;
        auto recordKey = utils.inputBufferRef->readRecord(utils.projectionKeys, recordBufferKey, keyPositionInBufferVal);

        auto [recordValueExactStart, recordValueExactEnd] = exactMap.equal_range({recordKey, utils.projectionKeys});
        const auto numberOfRecordsExact = std::distance(recordValueExactStart, recordValueExactEnd);

        const auto neededBytes = utils.inputBufferRef->getTupleSize() * numberOfRecordsExact;
        auto outputBufferOpt = utils.bufferManager->getUnpooledBuffer(neededBytes);
        RC_ASSERT(outputBufferOpt.has_value());
        auto outputBuffer = outputBufferOpt.value();

        writeAllRecordsIntoOutputBuffer(
            std::addressof(buffer), keyPositionInBuffer, std::addressof(outputBuffer), utils.bufferManager.get(), std::addressof(hashMap));

        RC_ASSERT(outputBuffer.getNumberOfTuples() == static_cast<uint64_t>(numberOfRecordsExact));

        nautilus::val<uint64_t> currentPosition = 0;
        for (auto exactIt = recordValueExactStart; exactIt != recordValueExactEnd; ++exactIt)
        {
            const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
            auto recordValueActual = utils.inputBufferRef->readRecord(projectionAllFields, recordBufferOutput, currentPosition);
            const auto errorMessage = TestUtils::NautilusTestUtils::compareRecords(recordValueActual, exactIt->second, projectionAllFields);
            RC_ASSERT(!errorMessage.has_value());
            ++currentPosition;
        }
    }

    hashMap.clear();
}

/// Configures ChainedHashMapTestUtils for a given combination.
/// All random parameters are generated via RapidCheck for full reproducibility.
void configureTestUtils(
    TestUtils::ChainedHashMapTestUtils& utils,
    const std::vector<DataType::Type>& keyTypes,
    const std::vector<DataType::Type>& valueTypes,
    ExecutionMode backend,
    uint64_t numberOfItems)
{
    utils.params.numberOfItems = numberOfItems;
    utils.params.numberOfBuckets = *rc::gen::inRange<uint64_t>(64, 2049);
    utils.params.pageSize = *rc::gen::inRange<uint64_t>(4096, 1048577);
    NES_INFO(
        "RC params: numberOfItems={}, numberOfBuckets={}, pageSize={}",
        utils.params.numberOfItems,
        utils.params.numberOfBuckets,
        utils.params.pageSize);
    utils.setUpChainedHashMapTest(keyTypes, valueTypes, backend);
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

    TestUtils::ChainedHashMapTestUtils utils;
    configureTestUtils(utils, keyTypes, valueTypes, backend, numberOfItems);

    ChainedHashMap hashMap{utils.keySize, utils.valueSize, utils.params.numberOfBuckets, utils.params.pageSize};
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

    const auto numBuffers = utils.inputBuffers.size();
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
        const auto& keyBuffer = utils.inputBuffers[keyIdx];
        const auto& valueBuffer = utils.inputBuffers[valueIdx];
        const auto tuplesToProcess = std::min(keyBuffer.getNumberOfTuples(), valueBuffer.getNumberOfTuples());
        const RecordBuffer recordBufferKey(nautilus::val<const TupleBuffer*>(std::addressof(keyBuffer)));
        const RecordBuffer recordBufferValue(nautilus::val<const TupleBuffer*>(std::addressof(valueBuffer)));
        for (nautilus::val<uint64_t> i = 0; i < tuplesToProcess; i = i + 1)
        {
            auto recordKey = utils.inputBufferRef->readRecord(utils.projectionKeys, recordBufferKey, i);
            auto recordValue = utils.inputBufferRef->readRecord(utils.projectionValues, recordBufferValue, i);
            auto keyVec = recordToAnyVec(recordKey, utils.projectionKeys, keyTypes);
            auto valueVec = recordToAnyVec(recordValue, utils.projectionValues, valueTypes);
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
    auto findAndInsert = utils.compileFindAndInsert();
    auto findAndUpdate = utils.compileFindAndUpdate();
    for (const auto& [keyIdx, valueIdx, op] : ops)
    {
        auto& keyBuffer = utils.inputBuffers[keyIdx];
        auto& valueBuffer = utils.inputBuffers[valueIdx];
        const auto tuplesToProcess = std::min(keyBuffer.getNumberOfTuples(), valueBuffer.getNumberOfTuples());
        const auto originalKeyCount = keyBuffer.getNumberOfTuples();
        keyBuffer.setNumberOfTuples(tuplesToProcess);
        switch (op)
        {
            case Op::INSERT:
                findAndInsert(std::addressof(keyBuffer), utils.bufferManager.get(), std::addressof(hashMap));
                break;
            case Op::UPDATE:
                findAndUpdate(std::addressof(keyBuffer), std::addressof(valueBuffer), utils.bufferManager.get(), std::addressof(hashMap));
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
    auto outputBufferOpt = utils.bufferManager->getUnpooledBuffer(hashMap.getNumberOfTuples() * utils.inputSchema.getSizeOfSchemaInBytes());
    RC_ASSERT(outputBufferOpt.has_value());
    auto outputBuffer = outputBufferOpt.value();
    std::ranges::fill(outputBuffer.getAvailableMemoryArea(), std::byte{0});

    auto entryIteratorFn = utils.compileFindAndWriteToOutputBufferWithEntryIterator();
    entryIteratorFn(std::addressof(outputBuffer), std::addressof(hashMap), utils.bufferManager.get());
    RC_ASSERT(outputBuffer.getNumberOfTuples() == hashMap.getNumberOfTuples());

    std::set<std::vector<std::any>, AnyVecLess> keysFoundInHashMap(keyComparator);
    const RecordBuffer recordBufferOutput(nautilus::val<const TupleBuffer*>(std::addressof(outputBuffer)));
    for (nautilus::val<uint64_t> pos = 0; pos < recordBufferOutput.getNumRecords(); ++pos)
    {
        auto keyRecord = utils.inputBufferRef->readRecord(utils.projectionKeys, recordBufferOutput, pos);
        auto valueRecord = utils.inputBufferRef->readRecord(utils.projectionValues, recordBufferOutput, pos);
        auto keyVec = recordToAnyVec(keyRecord, utils.projectionKeys, keyTypes);
        auto valueVec = recordToAnyVec(valueRecord, utils.projectionValues, valueTypes);

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

}
