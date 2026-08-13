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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Buffer.hpp>
#include <nautilus/Engine.hpp>
#include <DataStructureTestUtils.hpp>

namespace NES::TestUtils
{

/// Wraps a ChainedHashMap for property testing: put()/at()/getAll() are Nautilus-compiled and invoked
/// via function-pointer dispatch, mirroring how the hash-join and aggregation operators use the map in production.
class TestableChainedHashMap
{
public:
    TestableChainedHashMap(
        const std::vector<DataType>& fieldTypes,
        AbstractBufferProvider& bufferManager,
        EngineMode mode,
        uint64_t numberOfBuckets,
        size_t numKeyFields,
        uint64_t numEntriesPerPage,
        /// NOLINTNEXTLINE(fuchsia-default-arguments-declarations): matches the pre-existing default here.
        const std::optional<Nautilus::Interface::BloomFilterParams>& bloomFilterParams = std::nullopt);

    ~TestableChainedHashMap() = default;
    TestableChainedHashMap(const TestableChainedHashMap&) = delete;
    TestableChainedHashMap& operator=(const TestableChainedHashMap&) = delete;
    TestableChainedHashMap(TestableChainedHashMap&&) = default;
    TestableChainedHashMap& operator=(TestableChainedHashMap&&) = delete;

    /// Inserts a key/value pair. Matches ChainedHashMap's own findOrCreateEntry semantics: if the key
    /// already exists, the existing value is kept (first-write-wins) rather than overwritten.
    void put(const AnyVec& key, const AnyVec& value);

    /// Looks up a key without mutating the map. Returns the value if present, nullopt otherwise.
    std::optional<AnyVec> at(const AnyVec& key);

    /// Returns every stored (key, value) pair in page-iteration order.
    std::vector<std::pair<AnyVec, AnyVec>> getAll();

    [[nodiscard]] uint64_t size() const;

    ChainedHashMap raw();

    [[nodiscard]] size_t numKeyFields() const;

    [[nodiscard]] const std::vector<DataType>& getKeyDataTypes() const;

    [[nodiscard]] const std::vector<DataType>& getValueDataTypes() const;

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
    Buffer chainedHashMapBuffer;
    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
    AbstractBufferProvider& bufferManager;
    /// Sizing for the optional in-map BloomFilter. Defaults to nullopt, so the CHM behaves exactly as
    /// before unless a test opts in via the constructor.
    std::optional<Nautilus::Interface::BloomFilterParams> bloomFilterParams;
    std::vector<Record::RecordFieldIdentifier> projections;
    std::unique_ptr<nautilus::engine::NautilusEngine> engine;
    std::optional<nautilus::engine::CompiledFunction<void(Buffer*, AbstractBufferProvider*, AnyVec*)>> insertFn;
    std::optional<nautilus::engine::CompiledFunction<bool(Buffer*, Buffer*, AbstractBufferProvider*, AnyVec*, AnyVec*)>> lookupFn;
    std::optional<nautilus::engine::CompiledFunction<void(Buffer*, std::vector<AnyVec>*)>> readAllFn;

    struct FieldOffsets
    {
        decltype(fieldKeys) keys;
        decltype(fieldValues) values;
    };

    FieldOffsets computeFieldOffsets(const std::vector<DataType>& fieldTypes, size_t numKeyFields);
};

}
