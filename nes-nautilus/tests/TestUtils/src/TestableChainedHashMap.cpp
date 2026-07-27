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

#include <TestableChainedHashMap.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/MurMur3HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES::TestUtils
{

namespace
{
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
}

/// NOLINTBEGIN(bugprone-unchecked-optional-access, performance-unnecessary-value-param)
TestableChainedHashMap::TestableChainedHashMap(
    const std::vector<DataType>& fieldTypes,
    AbstractBufferProvider& bufferManager,
    EngineMode mode,
    uint64_t numberOfBuckets,
    size_t numKeyFields,
    uint64_t numEntriesPerPage,
    const std::optional<Nautilus::Interface::BloomFilterParams>& bloomFilterParams)
    : dataTypes(fieldTypes), bufferManager(bufferManager), bloomFilterParams(bloomFilterParams)
{
    PRECONDITION(
        numKeyFields <= fieldTypes.size(),
        "numKeyFields ({}) must not exceed the number of fields ({}); key fields are expected to be a prefix of fieldTypes.",
        numKeyFields,
        fieldTypes.size());

    const auto schema = createSchemaFromDataTypes(dataTypes);
    projections = getOrderedFieldNames(schema);
    auto offsets = computeFieldOffsets(fieldTypes, numKeyFields);
    fieldKeys = std::move(offsets.keys); /// NOLINT(cppcoreguidelines-prefer-member-initializer): projections must be set first
    fieldValues = std::move(offsets.values); /// NOLINT(cppcoreguidelines-prefer-member-initializer): projections must be set first
    std::tie(fieldKeyNames, fieldKeyTypes) = splitFieldOffsets(fieldKeys);
    std::tie(fieldValueNames, fieldValueTypes) = splitFieldOffsets(fieldValues);

    engine = std::make_unique<nautilus::engine::NautilusEngine>(makeEngine(mode));

    keyDataTypes
        = std::vector<DataType>(fieldTypes.begin(), fieldTypes.begin() + numKeyFields); /// NOLINT(cppcoreguidelines-narrowing-conversions)
    valueDataTypes
        = std::vector<DataType>(fieldTypes.begin() + numKeyFields, fieldTypes.end()); /// NOLINT(cppcoreguidelines-narrowing-conversions)

    const uint64_t keySize = getSizeInBytes(createSchemaFromDataTypes(keyDataTypes));
    const uint64_t valueSize = getSizeInBytes(createSchemaFromDataTypes(valueDataTypes));
    const uint64_t entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    /// A probe entry never stores value bytes: findEntry() only reads the key region + hash off the
    /// entry it's given, and key-field offsets don't depend on the value fields at all (see
    /// ChainedEntryMemoryProvider::createFieldOffsets), so fieldKeys is reusable as-is for probe entries.
    const uint64_t probeEntrySize = sizeof(ChainedHashMapEntry) + keySize;

    const auto hashFunction = std::make_shared<MurMur3HashFunction>();
    hashMapConfig = ChainedHashMapConfig{
        .entrySize = entrySize,
        .numberOfBuckets = numberOfBuckets,
        .pageSize = entrySize * numEntriesPerPage,
        .bloomFilterParams = bloomFilterParams,
        .fieldKeys = fieldKeys,
        .fieldValues = fieldValues,
        .hashFunction = hashFunction};
    /// The throwaway probe map only ever holds the single key being looked up, so it needs no BloomFilter, and
    /// it stores no value bytes: findEntry() reads only the key region and the hash off the entry it is given.
    probeConfig = ChainedHashMapConfig{
        .entrySize = probeEntrySize,
        .numberOfBuckets = 1,
        .pageSize = probeEntrySize,
        .bloomFilterParams = std::nullopt,
        .fieldKeys = fieldKeys,
        .fieldValues = {},
        .hashFunction = hashFunction};

    const auto hashMapBufferSize
        = ChainedHashMap::calculateBufferSize(hashMapConfig.numberOfBuckets, hashMapConfig.bloomFilterMemAreaSize());
    auto chainedHashMapBufferOpt = bufferManager.getUnpooledBuffer(hashMapBufferSize);
    if (not chainedHashMapBufferOpt.has_value())
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer of size {} available!", hashMapBufferSize);
    }
    chainedHashMapBuffer = chainedHashMapBufferOpt.value();
    ChainedHashMap::init(chainedHashMapBuffer, hashMapConfig);

    insertFn.emplace(engine->registerFunction(std::function(
        [dataTypes = dataTypes, projections = projections, fieldKeys = fieldKeys, fieldValues = fieldValues, hashMapConfig = hashMapConfig](
            nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<AbstractBufferProvider*> bm, nautilus::val<AnyVec*> rec)
        {
            const Record record = buildRecordFromAnyVec(rec, projections, dataTypes);
            const BorrowedNautilusBuffer borrowedBuffer = BorrowedNautilusBuffer::from(chainedHashMapBuffer);
            ChainedHashMapRef chmRef{borrowedBuffer, hashMapConfig};
            std::ignore = chmRef.findOrCreateEntry(
                record,
                [&](const nautilus::val<AbstractHashMapEntry*>& newEntry)
                {
                    const auto chainedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(newEntry);
                    const ChainedHashMapRef::ChainedEntryRef newEntryRef{chainedEntry, borrowedBuffer, fieldKeys, fieldValues};
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
         keyProjections = fieldKeyNames,
         fieldKeys = fieldKeys,
         fieldValues = fieldValues,
         fieldValueNames = fieldValueNames,
         fieldValueTypes = fieldValueTypes,
         hashMapConfig = hashMapConfig,
         probeConfig = probeConfig](
            nautilus::val<TupleBuffer*> chainedHashMapBuffer,
            nautilus::val<TupleBuffer*> probeBuffer,
            nautilus::val<AbstractBufferProvider*> bm,
            nautilus::val<AnyVec*> keyIn,
            nautilus::val<AnyVec*> out) -> nautilus::val<bool>
        {
            const Record keyRecord = buildRecordFromAnyVec(keyIn, keyProjections, keyDataTypes);
            const BorrowedNautilusBuffer borrowedBuffer = BorrowedNautilusBuffer::from(chainedHashMapBuffer);
            const BorrowedNautilusBuffer borrowedProbeBuffer = BorrowedNautilusBuffer::from(probeBuffer);

            ChainedHashMapRef probeMapRef{borrowedProbeBuffer, probeConfig};
            const auto probeEntry = probeMapRef.findOrCreateEntry(keyRecord, [](const nautilus::val<AbstractHashMapEntry*>&) { }, bm);

            ChainedHashMapRef chmRef{borrowedBuffer, hashMapConfig};
            const auto foundEntry = chmRef.findEntry(probeEntry);
            const nautilus::val<bool> found = (foundEntry != nullptr);
            if (found)
            {
                const auto chainedEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(foundEntry);
                const ChainedHashMapRef::ChainedEntryRef entryRef{chainedEntry, borrowedBuffer, fieldKeys, fieldValues};
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
         hashMapConfig = hashMapConfig](nautilus::val<TupleBuffer*> chainedHashMapBuffer, nautilus::val<std::vector<AnyVec>*> outVector)
        {
            /// begin() calls getPage(0) via invoke which fails on an empty CHM, so guard first.
            const auto numTuples = nautilus::invoke(
                +[](TupleBuffer* buf) { return ChainedHashMap::load(*buf).getTotalNumberOfRecords(); }, chainedHashMapBuffer);
            if (numTuples == nautilus::val<uint64_t>{0})
            {
                return;
            }

            const BorrowedNautilusBuffer borrowedBuffer = BorrowedNautilusBuffer::from(chainedHashMapBuffer);
            const ChainedHashMapRef chmRef{borrowedBuffer, hashMapConfig};

            for (const auto entry : chmRef)
            {
                const ChainedHashMapRef::ChainedEntryRef entryRef{entry, borrowedBuffer, fieldKeys, fieldValues};

                auto out = anyVecPushBack(outVector, nautilus::val<size_t>{fieldKeys.size() + fieldValues.size()});

                const auto keyRecord = entryRef.getKey();
                const auto valueRecord = entryRef.getValue();

                storeRecordToAnyVec(out, keyRecord, fieldKeyNames, fieldKeyTypes);
                storeRecordToAnyVec(out, valueRecord, fieldValueNames, fieldValueTypes, fieldKeys.size());
            }
        })));
}

/// NOLINTEND(bugprone-unchecked-optional-access, performance-unnecessary-value-param)

void TestableChainedHashMap::put(const AnyVec& key, const AnyVec& value)
{
    AnyVec combined;
    combined.reserve(key.size() + value.size());
    combined.insert(combined.end(), key.begin(), key.end());
    combined.insert(combined.end(), value.begin(), value.end());
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
    (*insertFn)(&chainedHashMapBuffer, &bufferManager, &combined);
}

std::optional<AnyVec> TestableChainedHashMap::at(const AnyVec& key)
{
    /// probeConfig is the member the lookup trace was built with, not a fresh one: the map carries no sizing,
    /// so the config init() sees here has to be the exact one the compiled probe assumes.
    const auto probeBufferSize = ChainedHashMap::calculateBufferSize(probeConfig.numberOfBuckets, probeConfig.bloomFilterMemAreaSize());
    auto probeBufferOpt = bufferManager.getUnpooledBuffer(probeBufferSize);
    if (not probeBufferOpt.has_value())
    {
        throw BufferAllocationFailure("No unpooled TupleBuffer of size {} available!", probeBufferSize);
    }
    auto probeBuffer = probeBufferOpt.value();
    ChainedHashMap::init(probeBuffer, probeConfig);

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

std::vector<std::pair<AnyVec, AnyVec>> TestableChainedHashMap::getAll()
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

uint64_t TestableChainedHashMap::size() const
{
    return ChainedHashMap::load(chainedHashMapBuffer).getTotalNumberOfRecords();
}

ChainedHashMap TestableChainedHashMap::raw()
{
    return ChainedHashMap::load(chainedHashMapBuffer);
}

size_t TestableChainedHashMap::numKeyFields() const
{
    return keyDataTypes.size();
}

const std::vector<DataType>& TestableChainedHashMap::getKeyDataTypes() const
{
    return keyDataTypes;
}

const std::vector<DataType>& TestableChainedHashMap::getValueDataTypes() const
{
    return valueDataTypes;
}

TestableChainedHashMap::FieldOffsets
TestableChainedHashMap::computeFieldOffsets(const std::vector<DataType>& fieldTypes, size_t numKeyFields)
{
    const auto splitPoint = std::next(projections.begin(), numKeyFields); /// NOLINT(cppcoreguidelines-narrowing-conversions)
    const auto keyProjections = std::vector<Record::RecordFieldIdentifier>(projections.begin(), splitPoint);
    const auto valueProjections = std::vector<Record::RecordFieldIdentifier>(splitPoint, projections.end());
    auto [fk, fv] = ChainedEntryMemoryProvider::createFieldOffsets(createSchemaFromDataTypes(fieldTypes), keyProjections, valueProjections);
    return {.keys = std::move(fk), .values = std::move(fv)};
}

}
