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
#include <DataTypes/Schema.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp> /// NOLINT(misc-include-cleaner)
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h> /// NOLINT(misc-include-cleaner): consumed via macros expanded from rapidcheck/gtest.h
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp> /// NOLINT(misc-include-cleaner)
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

#include <rapidcheck.h> /// NOLINT(misc-include-cleaner)

#include <fmt/ranges.h>
#include <rapidcheck/gtest.h>

/// NOLINTBEGIN(misc-include-cleaner, bugprone-unchecked-optional-access, google-build-using-namespace)
namespace NES
{

namespace
{
using namespace NES::TestUtils;

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

/// Splits a FieldOffsets vector into parallel (name, type) vectors for buildRecordFromAnyVec/storeRecordToAnyVec.
std::pair<std::vector<Record::RecordFieldIdentifier>, std::vector<DataType>> splitFieldOffsets(const std::vector<FieldOffsets>& fields)
{
    std::vector<Record::RecordFieldIdentifier> names;
    std::vector<DataType> types;
    names.reserve(fields.size());
    types.reserve(fields.size());
    for (const auto& field : fields)
    {
        names.push_back(field.fieldIdentifier);
        types.push_back(field.type);
    }
    return {std::move(names), std::move(types)};
}

class TestableChainedHashMap
{
public:
    TestableChainedHashMap(
        const std::vector<DataType>& fieldTypes,
        AbstractBufferProvider& bufferManager,
        EngineMode mode,
        uint64_t numberOfBuckets,
        size_t numKeyFields,
        uint64_t numEntriesPerPage)
        : dataTypes(fieldTypes), entriesPerPage(numEntriesPerPage), bufferManager(bufferManager)
    {
        const auto schema = createSchemaFromDataTypes(dataTypes);
        projections = getOrderedFieldNames(schema);
        auto offsets = computeFieldOffsets(fieldTypes, numKeyFields);
        fieldKeys = std::move(offsets.keys); /// NOLINT(cppcoreguidelines-prefer-member-initializer): projections must be set first
        fieldValues = std::move(offsets.values); /// NOLINT(cppcoreguidelines-prefer-member-initializer): projections must be set first
        std::tie(fieldKeyNames, fieldKeyTypes) = splitFieldOffsets(fieldKeys);
        std::tie(fieldValueNames, fieldValueTypes) = splitFieldOffsets(fieldValues);

        engine = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));

        keyDataTypes = std::vector<DataType>(
            fieldTypes.begin(), fieldTypes.begin() + numKeyFields); /// NOLINT(cppcoreguidelines-narrowing-conversions)
        valueDataTypes = std::vector<DataType>(
            fieldTypes.begin() + numKeyFields, fieldTypes.end()); /// NOLINT(cppcoreguidelines-narrowing-conversions)

        const uint64_t keySize = createSchemaFromDataTypes(keyDataTypes).getSizeInBytes();
        const uint64_t valueSize = createSchemaFromDataTypes(valueDataTypes).getSizeInBytes();
        entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
        const uint64_t pageSize = entrySize * numEntriesPerPage;
        /// A probe entry never stores value bytes: findEntry() only reads the key region + hash off the
        /// entry it's given, and key-field offsets don't depend on the value fields at all (see
        /// ChainedEntryMemoryProvider::createFieldOffsets), so fieldKeys is reusable as-is for probe entries.
        probeEntrySize = sizeof(ChainedHashMapEntry) + keySize;

        auto chainedHashMapBufferOpt = bufferManager.getUnpooledBuffer(ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets));
        if (not chainedHashMapBufferOpt.has_value())
        {
            throw BufferAllocationFailure(
                "No unpooled TupleBuffer of size {} available!", ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets));
        }
        chainedHashMapBuffer = chainedHashMapBufferOpt.value();
        ChainedHashMap::init(chainedHashMapBuffer, entrySize, numberOfBuckets, pageSize);

        const auto numKeyProjections = keyDataTypes.size();
        const auto keyProjections = std::vector<Record::RecordFieldIdentifier>(
            projections.begin(), projections.begin() + numKeyProjections); /// NOLINT(cppcoreguidelines-narrowing-conversions)

        /// NOLINTBEGIN(performance-unnecessary-value-param)
        insertFn.emplace(engine->registerFunction(std::function(
            [dataTypes = dataTypes,
             projections = projections,
             fieldKeys = fieldKeys,
             fieldValues = fieldValues,
             capturedEntriesPerPage = entriesPerPage,
             capturedEntrySize = entrySize,
             hashFn = MurMur3HashFunction{}](
                nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<AbstractBufferProvider*> bm, nautilus::val<AnyVec*> rec)
            {
                const Record record = buildRecordFromAnyVec(rec, projections, dataTypes);
                ChainedHashMapRef chmRef(
                    chainedHashMapBuffer,
                    fieldKeys,
                    fieldValues,
                    nautilus::val<uint64_t>(capturedEntriesPerPage),
                    nautilus::val<uint64_t>(capturedEntrySize));
                std::ignore = chmRef.findOrCreateEntry(
                    record,
                    hashFn,
                    [&](const nautilus::val<AbstractHashMapEntry*>& newEntry)
                    {
                        const auto chainedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(newEntry);
                        const ChainedHashMapRef::ChainedEntryRef newEntryRef(chainedEntry, chainedHashMapBuffer, fieldKeys, fieldValues);
                        newEntryRef.copyValuesToEntry(record, bm);
                    },
                    bm);
            })));

        /// Looks a key up without mutating the map under test: materialises the probe key into a disposable
        /// one-entry probe map (findOrCreateEntry on an always-empty map is just "format this entry" - it
        /// never touches the map under test), then does a genuine no-mutation ChainedHashMapRef::findEntry()
        /// against the real map. This mirrors the entry-to-entry probing idiom used by the hash-join and
        /// aggregation probe operators in production (they probe one map with an entry materialised in another).
        lookupFn.emplace(engine->registerFunction(std::function(
            [keyDataTypes = keyDataTypes,
             keyProjections = keyProjections,
             fieldKeys = fieldKeys,
             fieldValues = fieldValues,
             fieldValueNames = fieldValueNames,
             fieldValueTypes = fieldValueTypes,
             capturedEntriesPerPage = entriesPerPage,
             capturedEntrySize = entrySize,
             capturedProbeEntrySize = probeEntrySize,
             hashFn = MurMur3HashFunction{}](
                nautilus::val<TupleBuffer*> chainedHashMapBuffer,
                nautilus::val<TupleBuffer*> probeBuffer,
                nautilus::val<AbstractBufferProvider*> bm,
                nautilus::val<AnyVec*> keyIn,
                nautilus::val<AnyVec*> out) -> nautilus::val<bool>
            {
                const Record keyRecord = buildRecordFromAnyVec(keyIn, keyProjections, keyDataTypes);

                ChainedHashMapRef probeMapRef(
                    probeBuffer, fieldKeys, {}, nautilus::val<uint64_t>(1), nautilus::val<uint64_t>(capturedProbeEntrySize));
                const auto probeEntry
                    = probeMapRef.findOrCreateEntry(keyRecord, hashFn, [](const nautilus::val<AbstractHashMapEntry*>&) { }, bm);

                ChainedHashMapRef chmRef(
                    chainedHashMapBuffer,
                    fieldKeys,
                    fieldValues,
                    nautilus::val<uint64_t>(capturedEntriesPerPage),
                    nautilus::val<uint64_t>(capturedEntrySize));
                const auto foundEntry = chmRef.findEntry(probeEntry);
                const nautilus::val<bool> found = (foundEntry != nullptr);
                if (found)
                {
                    const auto chainedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(foundEntry);
                    const ChainedHashMapRef::ChainedEntryRef entryRef(chainedEntry, chainedHashMapBuffer, fieldKeys, fieldValues);
                    const auto valueRecord = entryRef.getValue();
                    storeRecordToAnyVec(out, valueRecord, fieldValueNames, fieldValueTypes);
                }
                return found;
            })));

        readAllFn.emplace(engine->registerFunction(std::function(
            [fieldKeys = fieldKeys,
             fieldValues = fieldValues,
             fieldKeyNames = fieldKeyNames,
             fieldKeyTypes = fieldKeyTypes,
             fieldValueNames = fieldValueNames,
             fieldValueTypes = fieldValueTypes,
             capturedEntriesPerPage = entriesPerPage,
             capturedEntrySize = entrySize](nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<std::vector<AnyVec>*> outVector)
            {
                /// begin() calls getPage(0) via invoke which fails on an empty CHM, so guard first.
                const auto numTuples = nautilus::invoke(
                    +[](TupleBuffer* buf) { return ChainedHashMap::load(*buf).getTotalNumberOfRecords(); }, chainedHashMapBuffer);
                if (numTuples == nautilus::val<uint64_t>(0))
                {
                    return;
                }

                const ChainedHashMapRef chmRef(
                    chainedHashMapBuffer,
                    fieldKeys,
                    fieldValues,
                    nautilus::val<uint64_t>(capturedEntriesPerPage),
                    nautilus::val<uint64_t>(capturedEntrySize));

                for (const auto entry : chmRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRef(entry, chainedHashMapBuffer, fieldKeys, fieldValues);

                    auto out = anyVecPushBack(outVector, nautilus::val<size_t>(fieldKeys.size() + fieldValues.size()));

                    const auto keyRecord = entryRef.getKey();
                    const auto valueRecord = entryRef.getValue();

                    storeRecordToAnyVec(out, keyRecord, fieldKeyNames, fieldKeyTypes);
                    storeRecordToAnyVec(out, valueRecord, fieldValueNames, fieldValueTypes, fieldKeys.size());
                }
            })));
        /// NOLINTEND(performance-unnecessary-value-param)
    }

    ~TestableChainedHashMap() = default;
    TestableChainedHashMap(const TestableChainedHashMap&) = delete;
    TestableChainedHashMap& operator=(const TestableChainedHashMap&) = delete;
    TestableChainedHashMap(TestableChainedHashMap&&) = default;
    TestableChainedHashMap& operator=(TestableChainedHashMap&&) = delete;

    /// Inserts a key/value pair. Matches ChainedHashMap's own findOrCreateEntry semantics: if the key
    /// already exists, the existing value is kept (first-write-wins) rather than overwritten.
    void put(const AnyVec& key, const AnyVec& value)
    {
        AnyVec combined;
        combined.reserve(key.size() + value.size());
        combined.insert(combined.end(), key.begin(), key.end());
        combined.insert(combined.end(), value.begin(), value.end());
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        (*insertFn)(&chainedHashMapBuffer, &bufferManager, &combined);
    }

    /// Looks up a key without mutating the map. Returns the value if present, nullopt otherwise.
    std::optional<AnyVec> at(const AnyVec& key)
    {
        auto probeBufferOpt = bufferManager.getUnpooledBuffer(ChainedHashMap::calculateBufferSizeFromBuckets(1));
        if (not probeBufferOpt.has_value())
        {
            throw BufferAllocationFailure(
                "No unpooled TupleBuffer of size {} available!", ChainedHashMap::calculateBufferSizeFromBuckets(1));
        }
        auto probeBuffer = probeBufferOpt.value();
        ChainedHashMap::init(probeBuffer, probeEntrySize, 1, probeEntrySize);

        AnyVec out(valueDataTypes.size());
        /// const_cast: lookupFn's signature requires AnyVec* even though the trace lambda only reads from it.
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast, bugprone-unchecked-optional-access)
        const bool found = (*lookupFn)(&chainedHashMapBuffer, &probeBuffer, &bufferManager, const_cast<AnyVec*>(&key), &out);
        if (!found)
        {
            return std::nullopt;
        }
        return out;
    }

    /// Returns every stored (key, value) pair in page-iteration order.
    std::vector<std::pair<AnyVec, AnyVec>> getAll()
    {
        const auto numEntries = ChainedHashMap::load(chainedHashMapBuffer).getTotalNumberOfRecords();
        std::vector<AnyVec> combined;
        combined.reserve(numEntries);
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
        (*readAllFn)(&chainedHashMapBuffer, &combined);

        std::vector<std::pair<AnyVec, AnyVec>> out;
        out.reserve(combined.size());
        const auto numKeys = numKeyFields();
        for (const auto& record : combined)
        {
            AnyVec key(record.begin(), record.begin() + numKeys); /// NOLINT(cppcoreguidelines-narrowing-conversions)
            AnyVec value(record.begin() + numKeys, record.end()); /// NOLINT(cppcoreguidelines-narrowing-conversions)
            out.emplace_back(std::move(key), std::move(value));
        }
        return out;
    }

    [[nodiscard]] uint64_t size() const { return ChainedHashMap::load(chainedHashMapBuffer).getTotalNumberOfRecords(); }

    ChainedHashMap raw() { return ChainedHashMap::load(chainedHashMapBuffer); }

    [[nodiscard]] size_t numKeyFields() const { return keyDataTypes.size(); }

    [[nodiscard]] const std::vector<DataType>& getKeyDataTypes() const { return keyDataTypes; }

    [[nodiscard]] const std::vector<DataType>& getValueDataTypes() const { return valueDataTypes; }

private:
    std::vector<DataType> dataTypes;
    std::vector<DataType> keyDataTypes;
    std::vector<DataType> valueDataTypes;
    std::vector<FieldOffsets> fieldKeys;
    std::vector<FieldOffsets> fieldValues;
    /// (name, type) parallel to fieldKeys/fieldValues, for buildRecordFromAnyVec/storeRecordToAnyVec.
    std::vector<Record::RecordFieldIdentifier> fieldKeyNames;
    std::vector<DataType> fieldKeyTypes;
    std::vector<Record::RecordFieldIdentifier> fieldValueNames;
    std::vector<DataType> fieldValueTypes;
    uint64_t entrySize{0};
    uint64_t probeEntrySize{0};
    uint64_t entriesPerPage{0};
    TupleBuffer chainedHashMapBuffer;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferManager;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, AbstractBufferProvider*, AnyVec*)>> insertFn;
    std::optional<nautilus::engine::CompiledFunction<bool(TupleBuffer*, TupleBuffer*, AbstractBufferProvider*, AnyVec*, AnyVec*)>> lookupFn;
    std::optional<nautilus::engine::CompiledFunction<void(TupleBuffer*, std::vector<AnyVec>*)>> readAllFn;

    struct FieldOffsets
    {
        decltype(fieldKeys) keys;
        decltype(fieldValues) values;
    };

    FieldOffsets computeFieldOffsets(const std::vector<DataType>& fieldTypes, size_t numKeyFields)
    {
        const auto splitPoint = std::next(projections.begin(), numKeyFields); /// NOLINT(cppcoreguidelines-narrowing-conversions)
        const auto keyProjections = std::vector<Record::RecordFieldIdentifier>(projections.begin(), splitPoint);
        const auto valueProjections = std::vector<Record::RecordFieldIdentifier>(splitPoint, projections.end());
        auto [fk, fv]
            = ChainedEntryMemoryProvider::createFieldOffsets(createSchemaFromDataTypes(fieldTypes), keyProjections, valueProjections);
        return {.keys = std::move(fk), .values = std::move(fv)};
    }
};

/// Hashes an AnyVec key field-by-field via hashAnyField, bound to the key schema of the map under test.
struct AnyVecHash
{
    const std::vector<DataType>* keyTypes;

    size_t operator()(const AnyVec& key) const
    {
        size_t seed = 0;
        for (size_t i = 0; i < keyTypes->size(); ++i)
        {
            /// Matches boost::hash_combine's mixing constant.
            /// NOLINTNEXTLINE(readability-magic-numbers, hicpp-signed-bitwise)
            seed ^= hashAnyField(key[i], (*keyTypes)[i]) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }
        return seed;
    }
};

/// Compares two AnyVec keys field-by-field via compareAnyField, bound to the key schema of the map under test.
struct AnyVecKeyEqual
{
    const std::vector<DataType>* keyTypes;

    bool operator()(const AnyVec& lhs, const AnyVec& rhs) const
    {
        for (size_t k = 0; k < keyTypes->size(); ++k)
        {
            if (compareAnyField(lhs[k], rhs[k], (*keyTypes)[k]) != 0)
            {
                return false;
            }
        }
        return true;
    }
};

/// A real std::unordered_map<AnyVec, AnyVec> reference, so property tests compare the chained hash map against
/// the standard library's own hash map semantics rather than a hand-rolled linear-scan association list.
using KeyValueReference = std::unordered_map<AnyVec, AnyVec, AnyVecHash, AnyVecKeyEqual>;

/// Constructs an empty reference map, with AnyVecHash/AnyVecKeyEqual bound to chainedHashMap's key schema.
KeyValueReference makeEmptyReference(const TestableChainedHashMap& chainedHashMap)
{
    const auto& keyTypes = chainedHashMap.getKeyDataTypes();
    return KeyValueReference(0, AnyVecHash{&keyTypes}, AnyVecKeyEqual{&keyTypes});
}

/// Adds numberOfItems freshly generated (key, value) pairs to both `reference` and the map under test,
/// keeping only the first-seen value per unique key (try_emplace mirrors CHM's first-write-wins deduplication).
/// Callers may invoke this repeatedly on the same reference/chainedHashMap to interleave writes and reads.
void populateReference(
    TestableChainedHashMap& chainedHashMap, const std::vector<DataType>& fieldTypes, uint64_t numberOfItems, KeyValueReference& reference)
{
    const auto numKeys = chainedHashMap.numKeyFields();
    for (uint64_t i = 0; i < numberOfItems; ++i)
    {
        auto record = *genAnyVec(fieldTypes);
        AnyVec key(record.begin(), record.begin() + numKeys); /// NOLINT(cppcoreguidelines-narrowing-conversions)
        AnyVec value(
            record.begin() + numKeys,
            record.end()); /// NOLINT(cppcoreguidelines-narrowing-conversions, misc-const-correctness): moved into try_emplace below
        chainedHashMap.put(key, value);
        reference.try_emplace(std::move(key), std::move(value));
    }
}

/// Verifies at() against a random sample of known-present keys (must hit, with the first-seen value) and a
/// handful of independently-random keys (must miss, for whichever candidates don't happen to collide with
/// the reference) - the miss path is new coverage the old index-based readAt() could never express.
void verifyLookups(TestableChainedHashMap& chainedHashMap, const KeyValueReference& reference, const std::vector<DataType>& fieldTypes)
{
    const auto numKeys = chainedHashMap.numKeyFields();
    const auto& valueTypes = chainedHashMap.getValueDataTypes();
    const std::vector<DataType> keyOnlyTypes(
        fieldTypes.begin(), fieldTypes.begin() + numKeys); /// NOLINT(cppcoreguidelines-narrowing-conversions)

    if (!reference.empty())
    {
        /// Materialised once so indices can be sampled by position; reference itself stays an unordered_map.
        const std::vector<std::pair<AnyVec, AnyVec>> entries(reference.begin(), reference.end());
        const auto indices = *rc::gen::container<std::vector<uint64_t>>(rc::gen::inRange<uint64_t>(0, entries.size()));
        for (const auto idx : indices)
        {
            const auto& [key, expectedValue] = entries[idx];
            const auto actual = chainedHashMap.at(key);
            RC_ASSERT(actual.has_value());
            RC_ASSERT(anyVecsEqual(*actual, expectedValue, valueTypes));
        }
    }

    constexpr int numMissCandidates = 5;
    for (int i = 0; i < numMissCandidates; ++i)
    {
        const auto candidateKey = *genAnyVec(keyOnlyTypes);
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
void putAndLookupKeysProperty(EngineMode mode)
{
    constexpr uint64_t maxIterations = 5;

    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);
    const auto numIterations = *rc::gen::inRange<uint64_t>(1, maxIterations + 1);

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

    auto bufferManager = createBufferManager(bufferSize, pooledBufferCountFor(bufferSize));
    TestableChainedHashMap chainedHashMap(fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage);
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
void putAndGetAllProperty(EngineMode mode)
{
    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);
    const auto numberOfBuckets = *rc::gen::elementOf(NUM_BUCKETS_POOL);
    const auto numKeyFields = *rc::gen::inRange<size_t>(1, fieldTypes.size() + 1);
    const auto numEntriesPerPage = *rc::gen::elementOf(ENTRIES_PER_PAGE_POOL);

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

    auto bufferManager = createBufferManager(bufferSize, pooledBufferCountFor(bufferSize));
    TestableChainedHashMap chainedHashMap(fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage);
    auto reference = makeEmptyReference(chainedHashMap);
    populateReference(chainedHashMap, fieldTypes, numberOfItems, reference);

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
            { return anyVecsEqual(entry.first, expectedKey, keyTypes) && anyVecsEqual(entry.second, expectedValue, valueTypes); });
        RC_ASSERT(found);
    }
}

/// Verify put()/at() with the chained hash map used purely as a HashSet: every field is a key and there are no
/// value fields at all. Unlike putAndLookupKeysProperty, where numKeyFields is drawn from a range and the
/// all-keys/no-values case only turns up incidentally whenever that draw happens to land on fieldTypes.size(),
/// here that condition is forced deterministically on every run, while everything else (schema, buffer size,
/// bucket count, page size, item count, iteration count) stays randomised via property testing as usual.
void putAndLookupHashSetProperty(EngineMode mode)
{
    constexpr uint64_t maxIterations = 5;

    const auto fieldTypes = *genDataTypeSchema(ALL_VALUE_TYPES, 1, MAX_SCHEMA_FIELDS);
    const auto bufferSize = *rc::gen::elementOf(BUFFER_SIZE_POOL);
    const auto numberOfItems = *rc::gen::inRange<uint64_t>(0, MAX_ITEMS_PER_PROPERTY);
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

    auto bufferManager = createBufferManager(bufferSize, pooledBufferCountFor(bufferSize));
    TestableChainedHashMap chainedHashMap(fieldTypes, *bufferManager, mode, numberOfBuckets, numKeyFields, numEntriesPerPage);
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
    putAndLookupKeysProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupKeysInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupKeysProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndGetAllCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndGetAllProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndGetAllInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndGetAllProperty(EngineMode::Interpreter);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupHashSetCompiler, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupHashSetProperty(EngineMode::Compiler);
}

RC_GTEST_PROP(ChainedHashMapPropertyTest, putAndLookupHashSetInterpreter, ())
{
    Logger::setupLogging("ChainedHashMapPropertyTest.log", LogLevel::LOG_DEBUG);
    putAndLookupHashSetProperty(EngineMode::Interpreter);
}

TEST(ChainedHashMapIteratorTest, emptyMapIsAnEmptyRange)
{
    constexpr uint64_t bufferSize = 4096;
    constexpr uint64_t numberOfBuckets = 1;
    constexpr uint64_t entrySize = sizeof(ChainedHashMapEntry);
    constexpr uint64_t entriesPerPage = 1;
    constexpr uint64_t numberOfPooledBuffers = 16;

    auto bufferManager = createBufferManager(bufferSize, numberOfPooledBuffers);
    auto hashMapBuffer = bufferManager->getUnpooledBuffer(ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets)).value();
    ChainedHashMap::init(hashMapBuffer, entrySize, numberOfBuckets, entrySize * entriesPerPage);

    auto engine = makeEngine(EngineMode::Interpreter);
    auto iterate = engine.registerFunction(std::function(
        /// NOLINTNEXTLINE(performance-unnecessary-value-param): registerFunction requires val<FunctionArguments> by value.
        [](nautilus::val<TupleBuffer*> buffer)
        {
            const ChainedHashMapRef ref(buffer, {}, {}, nautilus::val<uint64_t>{entriesPerPage}, nautilus::val<uint64_t>{entrySize});
            for (const auto entry : ref)
            {
                std::ignore = entry;
            }
        }));

    EXPECT_NO_THROW(iterate(&hashMapBuffer));
}

}

/// NOLINTEND(misc-include-cleaner, bugprone-unchecked-optional-access, google-build-using-namespace)
