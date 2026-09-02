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
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Runtime/BufferManager.hpp> /// NOLINT(misc-include-cleaner)
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>
#include <TestableChainedHashMap.hpp>
#include <static.hpp> /// NOLINT(misc-include-cleaner)
#include <val_arith.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

/// Umbrella header: rapidcheck spreads rc::gen and the DefaultArbitrary specialisations over impl headers
/// (gen/*.hpp) that cannot be included directly, so the umbrella is the only supported entry point.
#include <rapidcheck.h>

#include <fmt/ranges.h>
#include <rapidcheck/gtest.h>

namespace NES
{

namespace
{

/// Buffer-size pool drawn from per property to exercise both extremes:
/// - tiny buffers (64-512 B) force long page chains, which surfaces correctness issues in the
///   getPageIndex binary search and the last-page cumulative-sum special case;
/// - the large 2 MiB buffer keeps everything on a single page and exercises the no-paging path.
/// Schemas whose tuple size doesn't fit must be discarded via RC_PRE.
constexpr std::array<uint64_t, 4> BUFFER_SIZE_POOL = {128, 512, 4096, 2ULL * 1024 * 1024};

/// Number of buckets pool drawn from property to test different numbers of buckets for the chained hash map.
constexpr std::array<uint64_t, 6> NUM_BUCKETS_POOL = {32, 64, 128, 256, 512, 1024};

/// Number of entries per page — multiplied by entrySize to derive pageSize.
/// Small values (1, 2) force long page chains; large values (64, 512) exercise the bulk path.
constexpr std::array<uint64_t, 6> ENTRIES_PER_PAGE_POOL = {1, 2, 4, 16, 64, 512};
constexpr uint64_t MAX_VARSIZED_MEMORY_BUDGET = 64ULL * 1024 * 1024;

/// A real std::unordered_map<TestUtils::AnyVec, TestUtils::AnyVec> reference, so property tests compare the chained hash map against
/// the standard library's own hash map semantics rather than a hand-rolled linear-scan association list.
using KeyValueReference = std::unordered_map<TestUtils::AnyVec, TestUtils::AnyVec, TestUtils::AnyVecHash, TestUtils::AnyVecKeyEqual>;

/// Constructs an empty reference map, with AnyVecHash/AnyVecKeyEqual bound to chainedHashMap's key schema.
KeyValueReference makeEmptyReference(const TestUtils::TestableChainedHashMap& chainedHashMap)
{
    const auto& keyTypes = chainedHashMap.getKeyDataTypes();
    return KeyValueReference(0, TestUtils::AnyVecHash{&keyTypes}, TestUtils::AnyVecKeyEqual{&keyTypes});
}

/// Adds numberOfItems freshly generated (key, value) pairs to both `reference` and the map under test,
/// keeping only the first-seen value per unique key (try_emplace mirrors CHM's first-write-wins deduplication).
/// Callers may invoke this repeatedly on the same reference/chainedHashMap to interleave writes and reads.
void populateReference(
    TestUtils::TestableChainedHashMap& chainedHashMap,
    const std::vector<DataType>& fieldTypes,
    uint64_t numberOfItems,
    KeyValueReference& reference,
    const TestUtils::VarSizedMemoryBudget& varSizedMemoryBudget)
{
    const auto numKeys = static_cast<TestUtils::AnyVec::difference_type>(chainedHashMap.numKeyFields());
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *TestUtils::genAnyVec(fieldTypes, varSizedMemoryBudget);
        TestUtils::AnyVec key(record.begin(), record.begin() + numKeys);
        TestUtils::AnyVec value(record.begin() + numKeys, record.end());
        chainedHashMap.put(key, value);
        reference.try_emplace(std::move(key), std::move(value));
    }
}

/// Verifies at() against a random sample of known-present keys (must hit, with the first-seen value) and a
/// handful of independently-random keys (must miss, for whichever candidates don't happen to collide with
/// the reference) - the miss path is new coverage the old index-based readAt() could never express.
void verifyLookups(
    TestUtils::TestableChainedHashMap& chainedHashMap,
    const KeyValueReference& reference,
    const std::vector<DataType>& fieldTypes,
    const TestUtils::VarSizedMemoryBudget& varSizedMemoryBudget)
{
    const auto numKeys = chainedHashMap.numKeyFields();
    const auto& valueTypes = chainedHashMap.getValueDataTypes();
    const std::vector<DataType> keyOnlyTypes(
        fieldTypes.begin(), fieldTypes.begin() + numKeys); /// NOLINT(cppcoreguidelines-narrowing-conversions)

    if (!reference.empty())
    {
        /// Materialised once so indices can be sampled by position; reference itself stays an unordered_map.
        const std::vector<std::pair<TestUtils::AnyVec, TestUtils::AnyVec>> entries(reference.begin(), reference.end());
        const auto indices = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, entries.size()));
        for (const auto idx : indices)
        {
            const auto& [key, expectedValue] = entries[idx];
            const auto actual = chainedHashMap.at(key);
            RC_ASSERT(actual.has_value());
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): the RC_ASSERT above aborts the property on nullopt.
            RC_ASSERT(TestUtils::anyVecsEqual(*actual, expectedValue, valueTypes));
        }
    }

    constexpr int numMissCandidates = 5;
    for (int i = 0; i < numMissCandidates; ++i)
    {
        const auto candidateKey = *TestUtils::genAnyVec(keyOnlyTypes, varSizedMemoryBudget);
        if (!reference.contains(candidateKey))
        {
            RC_ASSERT(!chainedHashMap.at(candidateKey).has_value());
        }
    }
}

/// Verify put()/at() round-trip: every reference key is found with its first-seen value, and independently
/// generated keys that don't collide with the reference correctly report absent. Writes and reads are
/// interleaved across several iterations (rather than write-everything-then-verify-once) so lookups are also
/// exercised against partially-populated intermediate hash-map states, not just the final one.
void putAndLookupKeysProperty(TestUtils::EngineMode mode)
{
    constexpr uint64_t maxIterations = 5;

    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto numIterations = *rc::gen::inRange<uint64_t>(1, maxIterations + 1);
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);

    NES_INFO(
        "Property putAndLookupKeys: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "iterations={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        numIterations,
        fmt::join(fieldTypes, ", "));

    auto bufferManager
        = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestableChainedHashMap chainedHashMap{fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage};
    auto reference = makeEmptyReference(chainedHashMap);

    const auto itemsPerIteration = numberOfItems / numIterations;
    for (uint64_t iteration = 0; iteration < numIterations; ++iteration)
    {
        populateReference(chainedHashMap, fieldTypes, itemsPerIteration, reference, varSizedMemoryBudget);
        NES_INFO(
            "putAndLookupKeys: iteration {}/{}, CHM has {} entries, {} unique keys",
            iteration + 1,
            numIterations,
            chainedHashMap.size(),
            reference.size());
        RC_ASSERT(chainedHashMap.size() == reference.size());
        verifyLookups(chainedHashMap, reference, fieldTypes, varSizedMemoryBudget);
    }
}

/// Verify getAll() returns every stored entry with the correct key+value pair.
void putAndGetAllProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);

    NES_INFO(
        "Property putAndGetAll: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        fmt::join(fieldTypes, ", "));

    auto bufferManager
        = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestableChainedHashMap chainedHashMap{fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage};
    auto reference = makeEmptyReference(chainedHashMap);
    populateReference(chainedHashMap, fieldTypes, numberOfItems, reference, varSizedMemoryBudget);

    NES_INFO("putAndGetAll: CHM has {} entries, {} unique keys", chainedHashMap.size(), reference.size());
    RC_ASSERT(chainedHashMap.size() == reference.size());

    const auto& keyTypes = chainedHashMap.getKeyDataTypes();
    const auto& valueTypes = chainedHashMap.getValueDataTypes();
    auto actual = chainedHashMap.getAll();
    RC_ASSERT(actual.size() == reference.size());
    for (const auto& [expectedKey, expectedValue] : reference)
    {
        const bool found = std::ranges::any_of(
            actual,
            [&](const auto& entry)
            {
                return TestUtils::anyVecsEqual(entry.first, expectedKey, keyTypes)
                    && TestUtils::anyVecsEqual(entry.second, expectedValue, valueTypes);
            });
        RC_ASSERT(found);
    }
}

/// Verify put()/at() with the chained hash map used purely as a HashSet: every field is a key and there are no
/// value fields at all. Unlike putAndLookupKeysProperty, where numKeyFields is drawn from a range and the
/// all-keys/no-values case only turns up incidentally whenever that draw happens to land on fieldTypes.size(),
/// here that condition is forced deterministically on every run, while everything else (schema, buffer size,
/// bucket count, page size, item count, iteration count) stays randomised via property testing as usual.
void putAndLookupHashSetProperty(TestUtils::EngineMode mode)
{
    constexpr uint64_t maxIterations = 5;

    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto numIterations = *rc::gen::inRange<uint64_t>(1, maxIterations + 1);
    const auto initialVarSizedMemoryBudget = *rc::gen::inRange<uint64_t>(0, MAX_VARSIZED_MEMORY_BUDGET + 1);
    auto varSizedMemoryBudget = std::make_shared<uint64_t>(initialVarSizedMemoryBudget);
    /// Deterministic, not drawn: every field is a key, so the map under test is exercised purely as a HashSet.
    const auto numKeyFields = fieldTypes.size();

    NES_INFO(
        "Property putAndLookupHashSet: fields={}, N={}, bufferSize={}, numBuckets={}, entriesPerPage={}, iterations={}, "
        "field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numberOfBuckets,
        numEntriesPerPage,
        numIterations,
        fmt::join(fieldTypes, ", "));

    auto bufferManager
        = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize, initialVarSizedMemoryBudget));
    TestUtils::TestableChainedHashMap chainedHashMap{fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage};
    RC_ASSERT(chainedHashMap.getValueDataTypes().empty());
    auto reference = makeEmptyReference(chainedHashMap);

    const auto itemsPerIteration = numberOfItems / numIterations;
    for (uint64_t iteration = 0; iteration < numIterations; ++iteration)
    {
        populateReference(chainedHashMap, fieldTypes, itemsPerIteration, reference, varSizedMemoryBudget);
        NES_INFO(
            "putAndLookupHashSet: iteration {}/{}, CHM has {} entries, {} unique keys",
            iteration + 1,
            numIterations,
            chainedHashMap.size(),
            reference.size());
        RC_ASSERT(chainedHashMap.size() == reference.size());
        verifyLookups(chainedHashMap, reference, fieldTypes, varSizedMemoryBudget);
    }
}
} /// anonymous namespace

/// One RC_GTEST_PROP per (property, backend) combination so that a failure on one backend doesn't mask the other and
/// rapidcheck's shrinking chases each backend's failing input independently.
RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupKeysCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupKeysProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupKeysInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupKeysProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndGetAllCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndGetAllProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndGetAllInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndGetAllProperty(TestUtils::EngineMode::Interpreter);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupHashSetCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupHashSetProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupHashSetInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupHashSetProperty(TestUtils::EngineMode::Interpreter);
}

TEST(ChainedHashMapIteratorTest, emptyMapIsAnEmptyRange)
{
    constexpr uint64_t bufferSize = 4096;
    constexpr uint64_t numberOfBuckets = 1;
    constexpr uint64_t entrySize = sizeof(ChainedHashMapEntry);
    constexpr uint64_t entriesPerPage = 1;
    constexpr uint64_t numberOfPooledBuffers = 16;

    auto bufferManager = TestUtils::createBufferManager(bufferSize, numberOfPooledBuffers);
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): .value() throws on nullopt, which fails the test.
    auto hashMapBuffer = bufferManager->getUnpooledBuffer(ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets)).value();
    ChainedHashMap::init(hashMapBuffer, entrySize, numberOfBuckets, entrySize * entriesPerPage);

    auto engine = TestUtils::makeEngine(TestUtils::EngineMode::Interpreter);
    auto iterate = engine.registerFunction(std::function(
        /// NOLINTNEXTLINE(performance-unnecessary-value-param): registerFunction requires val<FunctionArguments> by value.
        [](nautilus::val<TupleBuffer*> buffer)
        {
            const ChainedHashMapRef ref{buffer, {}, {}, nautilus::val<uint64_t>{entriesPerPage}, nautilus::val<uint64_t>{entrySize}};
            for (const auto entry : ref)
            {
                std::ignore = entry;
            }
        }));

    EXPECT_NO_THROW(iterate(&hashMapBuffer));
}

}
