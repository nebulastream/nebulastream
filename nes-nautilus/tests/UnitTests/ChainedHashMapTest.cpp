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
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/NautilusBuffer.hpp>
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

/// A real std::unordered_map<TestUtils::AnyVec, TestUtils::AnyVec> reference, so property tests compare the chained hash map against
/// the standard library's own hash map semantics rather than a hand-rolled linear-scan association list.
using KeyValueReference = std::unordered_map<TestUtils::AnyVec, TestUtils::AnyVec, TestUtils::AnyVecHash, TestUtils::AnyVecKeyEqual>;

/// Constructs an empty reference map, with AnyVecHash/AnyVecKeyEqual bound to chainedHashMap's key schema.
KeyValueReference makeEmptyReference(const TestUtils::TestableChainedHashMap& chainedHashMap)
{
    const auto& keyTypes = chainedHashMap.getKeyDataTypes();
    return KeyValueReference(0, TestUtils::AnyVecHash{&keyTypes}, TestUtils::AnyVecKeyEqual{&keyTypes});
}

/// Sizing inputs bracketing the collision spectrum: saturated (mightContain constant-true, so every chain is
/// traversed) through near-empty (~0.2% FP at the 500-entry cap, so negatives take the skip path).
/// Capped at ~2^18 bits (32KB): the bit area is inline in the map's unpooled TupleBuffer and the allocator
/// reserves 10x the rolling average per chunk, so a larger map buffer blows the tests' unpooled budget.
/// Not constexpr: the validating constructor is not a constant expression.
const std::array<Nautilus::Interface::BloomFilterParams, 4> BLOOM_COLLISION_EXTREMES = {
    Nautilus::Interface::BloomFilterParams{4, 5e-4}, /// 64 bits, 11 hashes
    Nautilus::Interface::BloomFilterParams{44, 0.9}, /// 64 bits, 1 hash
    Nautilus::Interface::BloomFilterParams{181'000, 0.5}, /// ~2^18 bits, 1 hash
    Nautilus::Interface::BloomFilterParams{4, 1e-21}, /// 403 bits, hashCount clamped 70 -> 64: deepest trace-time unroll
};

/// Generates random *enabled* BloomFilter sizing, or one of the collision extremes above. The lower
/// bit-count end is deliberately tiny (saturated -> high false-positive rate) and the upper end sparse,
/// stressing that correctness holds at any FP rate: a BloomFilter must never yield a false negative, so a
/// CHM with the filter enabled must dedup and iterate to exactly the same result as the disabled run for
/// any sizing.
Nautilus::Interface::BloomFilterParams genBloomFilterParams()
{
    /// One index past the pool means "draw a random sizing instead of an extreme".
    if (const auto idx = *rc::gen::inRange<size_t>(0, BLOOM_COLLISION_EXTREMES.size() + 1); idx < BLOOM_COLLISION_EXTREMES.size())
    {
        return BLOOM_COLLISION_EXTREMES.at(idx);
    }
    /// At the smallest drawable fpRate (~14.4 bits per entry), 18'000 entries stay under the 2^18-bit cap above.
    const auto expectedEntries = *rc::gen::inRange<uint64_t>(64, 18'000);
    const auto fpRate = static_cast<double>(*rc::gen::inRange<uint64_t>(1, 999)) / 1000.0;
    return Nautilus::Interface::BloomFilterParams{expectedEntries, fpRate};
}

/// Snapshots a map's contents as a KeyValueReference, so one CHM can act as the oracle for another.
/// Keys are unique within a map, so the snapshot is lossless.
KeyValueReference toReference(TestUtils::TestableChainedHashMap& chainedHashMap)
{
    auto reference = makeEmptyReference(chainedHashMap);
    for (auto& [key, value] : chainedHashMap.getAll())
    {
        reference.try_emplace(std::move(key), std::move(value));
    }
    return reference;
}

/// Draws numberOfItems random records and splits each at numKeyFields into a (key, value) pair. Returning
/// them instead of inserting directly lets a caller replay the very same sequence into more than one map.
std::vector<std::pair<TestUtils::AnyVec, TestUtils::AnyVec>>
genRecords(const std::vector<DataType>& fieldTypes, size_t numKeyFields, uint64_t numberOfItems)
{
    const auto splitPoint = static_cast<TestUtils::AnyVec::difference_type>(numKeyFields);
    std::vector<std::pair<TestUtils::AnyVec, TestUtils::AnyVec>> records;
    records.reserve(numberOfItems);
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *TestUtils::genAnyVec(fieldTypes);
        records.emplace_back(
            TestUtils::AnyVec(record.begin(), record.begin() + splitPoint), TestUtils::AnyVec(record.begin() + splitPoint, record.end()));
    }
    return records;
}

/// Adds numberOfItems freshly generated (key, value) pairs to both `reference` and the map under test,
/// keeping only the first-seen value per unique key (try_emplace mirrors CHM's first-write-wins deduplication).
/// Callers may invoke this repeatedly on the same reference/chainedHashMap to interleave writes and reads.
void populateReference(
    TestUtils::TestableChainedHashMap& chainedHashMap,
    const std::vector<DataType>& fieldTypes,
    uint64_t numberOfItems,
    KeyValueReference& reference)
{
    for (auto& [key, value] : genRecords(fieldTypes, chainedHashMap.numKeyFields(), numberOfItems))
    {
        chainedHashMap.put(key, value);
        reference.try_emplace(std::move(key), std::move(value));
    }
}

/// Verifies at() against a random sample of known-present keys (must hit, with the first-seen value) and a
/// handful of independently-random keys (must miss, for whichever candidates don't happen to collide with
/// the reference) - the miss path is new coverage the old index-based readAt() could never express.
void verifyLookups(
    TestUtils::TestableChainedHashMap& chainedHashMap, const KeyValueReference& reference, const std::vector<DataType>& fieldTypes)
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
        const auto candidateKey = *TestUtils::genAnyVec(keyOnlyTypes);
        if (!reference.contains(candidateKey))
        {
            RC_ASSERT(!chainedHashMap.at(candidateKey).has_value());
        }
    }
}

/// Verifies getAll() against the reference. Keys are unique in both, so "same entry count and every stored
/// entry is in the reference with the same value" is an exact set comparison, independent of iteration order.
void verifyGetAll(TestUtils::TestableChainedHashMap& chainedHashMap, const KeyValueReference& reference)
{
    const auto& valueTypes = chainedHashMap.getValueDataTypes();
    const auto actual = chainedHashMap.getAll();
    RC_ASSERT(actual.size() == reference.size());
    for (const auto& [key, value] : actual)
    {
        const auto found = reference.find(key);
        RC_ASSERT(found != reference.end());
        RC_ASSERT(TestUtils::anyVecsEqual(value, found->second, valueTypes));
    }
}

/// Verify put()/at() round-trip: every reference key is found with its first-seen value, and independently
/// generated keys that don't collide with the reference correctly report absent. Writes and reads are
/// interleaved across several iterations (rather than write-everything-then-verify-once) so lookups are also
/// exercised against partially-populated intermediate hash-map states, not just the final one.
/// When enableBloomFilter is set, the in-map BloomFilter is enabled at random sizing; a pass proves the
/// lookups are identical to the disabled run, i.e. the filter never produced a false negative.
/// NOLINTNEXTLINE(fuchsia-default-arguments-declarations): matches the pre-existing default here.
void putAndLookupKeysProperty(TestUtils::EngineMode mode, bool enableBloomFilter = false)
{
    constexpr uint64_t maxIterations = 5;

    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto numIterations = *rc::gen::inRange<uint64_t>(1, maxIterations + 1);
    const std::optional<Nautilus::Interface::BloomFilterParams> bloomFilterParams
        = enableBloomFilter ? std::optional{genBloomFilterParams()} : std::nullopt;
    const uint64_t loggedBloomBits = bloomFilterParams ? bloomFilterParams->getBitCount() : 0;
    const uint64_t loggedBloomHashes = bloomFilterParams ? bloomFilterParams->getHashCount() : 0;

    NES_INFO(
        "Property putAndLookupKeys: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "iterations={}, bloomBits={}, bloomHashes={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        numIterations,
        loggedBloomBits,
        loggedBloomHashes,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestableChainedHashMap chainedHashMap{
        fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage, bloomFilterParams};
    auto reference = makeEmptyReference(chainedHashMap);

    const auto itemsPerIteration = numberOfItems / numIterations;
    for (uint64_t iteration = 0; iteration < numIterations; ++iteration)
    {
        populateReference(chainedHashMap, fieldTypes, itemsPerIteration, reference);
        NES_INFO(
            "putAndLookupKeys: iteration {}/{}, CHM has {} entries, {} unique keys",
            iteration + 1,
            numIterations,
            chainedHashMap.size(),
            reference.size());
        RC_ASSERT(chainedHashMap.size() == reference.size());
        verifyLookups(chainedHashMap, reference, fieldTypes);
    }
}

/// Verify getAll() returns every stored entry with the correct key+value pair.
/// When enableBloomFilter is set, the in-map BloomFilter is enabled at random sizing; a pass proves the
/// stored entries are identical to the disabled run, i.e. the filter never produced a false negative.
/// NOLINTNEXTLINE(fuchsia-default-arguments-declarations): matches the pre-existing default here.
void putAndGetAllProperty(TestUtils::EngineMode mode, bool enableBloomFilter = false)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const std::optional<Nautilus::Interface::BloomFilterParams> bloomFilterParams
        = enableBloomFilter ? std::optional{genBloomFilterParams()} : std::nullopt;
    const uint64_t loggedBloomBits = bloomFilterParams ? bloomFilterParams->getBitCount() : 0;
    const uint64_t loggedBloomHashes = bloomFilterParams ? bloomFilterParams->getHashCount() : 0;

    NES_INFO(
        "Property putAndGetAll: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "bloomBits={}, bloomHashes={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        loggedBloomBits,
        loggedBloomHashes,
        fmt::join(fieldTypes, ", "));

    auto bufferManager = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestableChainedHashMap chainedHashMap{
        fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage, bloomFilterParams};
    auto reference = makeEmptyReference(chainedHashMap);
    populateReference(chainedHashMap, fieldTypes, numberOfItems, reference);

    NES_INFO("putAndGetAll: CHM has {} entries, {} unique keys", chainedHashMap.size(), reference.size());
    RC_ASSERT(chainedHashMap.size() == reference.size());
    verifyGetAll(chainedHashMap, reference);
}

/// Differential property: a CHM with the BloomFilter enabled must be observationally indistinguishable from
/// one without it. Both maps get the same schema, the same sizing and the same insert sequence, and then
/// every observation must agree: entry count, at() on inserted keys, at() on independently drawn keys, and
/// the full getAll() contents. Unlike the properties above (which compare against a std::unordered_map
/// reference and therefore only see BloomFilter bugs that also break unordered_map semantics), this one
/// pins the two configurations directly to each other, so any behavioural difference the filter introduces
/// fails here regardless of what the reference would have said.
void bloomFilterMatchesDisabledProperty(TestUtils::EngineMode mode)
{
    const auto fieldTypes = *TestUtils::genDataTypeSchema(TestUtils::ALL_VALUE_TYPES, 1, TestUtils::MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, TestUtils::MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto bloomFilterParams = genBloomFilterParams();

    NES_INFO(
        "Property bloomFilterMatchesDisabled: fields={}, N={}, bufferSize={}, numKeyFields={}, numBuckets={}, entriesPerPage={}, "
        "bloomBits={}, bloomHashes={}, field_types={}",
        fieldTypes.size(),
        numberOfItems,
        bufferSize,
        numKeyFields,
        numberOfBuckets,
        numEntriesPerPage,
        bloomFilterParams.getBitCount(),
        bloomFilterParams.getHashCount(),
        fmt::join(fieldTypes, ", "));

    /// Drawn once up front, then replayed into both maps in the same order, so the filter is the only difference.
    const auto records = genRecords(fieldTypes, numKeyFields, numberOfItems);

    /// One buffer manager per map: the two allocate pages independently and must not compete for the pool.
    auto withFilterBuffers = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    auto withoutFilterBuffers = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestableChainedHashMap withFilter{
        fieldTypes, *withFilterBuffers, mode, numberOfBuckets, numKeyFields, numEntriesPerPage, bloomFilterParams};
    TestUtils::TestableChainedHashMap withoutFilter{
        fieldTypes, *withoutFilterBuffers, mode, numberOfBuckets, numKeyFields, numEntriesPerPage};

    for (const auto& [key, value] : records)
    {
        withFilter.put(key, value);
        withoutFilter.put(key, value);
    }

    /// The disabled map is the oracle: same entry count, same stored contents, and the same at() answers
    /// (hits with the same value, misses reported absent) for the keys verifyLookups samples.
    const auto oracle = toReference(withoutFilter);
    RC_ASSERT(withFilter.size() == withoutFilter.size());
    verifyGetAll(withFilter, oracle);
    verifyLookups(withFilter, oracle, fieldTypes);
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

    auto bufferManager = TestUtils::createBufferManager(bufferSize, TestUtils::pooledBufferCountFor(bufferSize));
    TestUtils::TestableChainedHashMap chainedHashMap{fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage};
    RC_ASSERT(chainedHashMap.getValueDataTypes().empty());
    auto reference = makeEmptyReference(chainedHashMap);

    const auto itemsPerIteration = numberOfItems / numIterations;
    for (uint64_t iteration = 0; iteration < numIterations; ++iteration)
    {
        populateReference(chainedHashMap, fieldTypes, itemsPerIteration, reference);
        NES_INFO(
            "putAndLookupHashSet: iteration {}/{}, CHM has {} entries, {} unique keys",
            iteration + 1,
            numIterations,
            chainedHashMap.size(),
            reference.size());
        RC_ASSERT(chainedHashMap.size() == reference.size());
        verifyLookups(chainedHashMap, reference, fieldTypes);
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

    const ChainedHashMapConfig hashMapConfig{
        .entrySize = entrySize, .numberOfBuckets = numberOfBuckets, .pageSize = entrySize * entriesPerPage};

    auto bufferManager = TestUtils::createBufferManager(bufferSize, numberOfPooledBuffers);
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): .value() throws on nullopt, which fails the test.
    auto hashMapBuffer = bufferManager->getUnpooledBuffer(hashMapConfig.bufferSize()).value();
    ChainedHashMap::init(hashMapBuffer, hashMapConfig);

    auto engine = TestUtils::makeEngine(TestUtils::EngineMode::Interpreter);
    auto iterate = engine.registerFunction(std::function(
        /// NOLINTNEXTLINE(performance-unnecessary-value-param): registerFunction requires val<FunctionArguments> by value.
        [](nautilus::val<TupleBuffer*> buffer)
        {
            const ChainedHashMapRef ref{
                BorrowedNautilusBuffer::from(buffer),
                {},
                {},
                nautilus::val<uint64_t>{entriesPerPage},
                nautilus::val<uint64_t>{entrySize},
                std::nullopt};
            for (const auto entry : ref)
            {
                std::ignore = entry;
            }
        }));

    EXPECT_NO_THROW(iterate(&hashMapBuffer));
}

/// Same properties, but with the in-map BloomFilter enabled at random sizing. findOrCreateEntry consults
/// the filter in findChain; a false negative would re-insert an existing key and break dedup, so a pass
/// proves the results are identical to the disabled runs above for the randomly-chosen filter sizing.
RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupKeysWithBloomFilterCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupKeysProperty(TestUtils::EngineMode::Compiler, true);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupKeysWithBloomFilterInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupKeysProperty(TestUtils::EngineMode::Interpreter, true);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndGetAllWithBloomFilterCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndGetAllProperty(TestUtils::EngineMode::Compiler, true);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndGetAllWithBloomFilterInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndGetAllProperty(TestUtils::EngineMode::Interpreter, true);
}

/// Differential runs: enabled vs disabled filter on the same inputs, so the filter is pinned directly to
/// the no-filter behaviour rather than to the unordered_map reference.
RC_GTEST_PROP(ChainedHashMapPropertyTest, bloomFilterMatchesDisabledCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    bloomFilterMatchesDisabledProperty(TestUtils::EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, bloomFilterMatchesDisabledInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    bloomFilterMatchesDisabledProperty(TestUtils::EngineMode::Interpreter);
}

}
