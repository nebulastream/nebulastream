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
#include <functional>
#include <memory>
#include <span>
#include <utility>
#include <vector>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES
{
/// Forward declaration of the ChainedHashMapRef, to avoid cyclic dependencies between ChainedHashMap and ChainedHashMapRef
class ChainedHashMapRef;

/// Each entry contains a ptr to the next element, the hash of the current value and the keys and values.
/// The physical layout of the storage space is the following
/// | --- Entry* --- | --- hash --- | --- keys ---     | --- values ---    |
/// | --- 64bit ---  | --- 64bit ---  | --- keySize ---  | --- valueSize ---  |
class ChainedHashMapEntry final : public AbstractHashMapEntry
{
public:
    ChainedHashMapEntry* next{nullptr};
    HashFunction::HashValue::raw_type hash;
    explicit ChainedHashMapEntry(const HashFunction::HashValue::raw_type hash) : hash(hash) { };
};

/// Implementation of a single thread chained HashMap.
/// To operate on the hash-map, {@refitem ChainedHashMapRef.hpp} provides a Nautilus wrapper.
/// The implementation origins from Kersten et al. https://github.com/TimoKersten/db-engine-paradigms and Leis et.al
/// https://db.in.tum.de/~leis/papers/morsels.pdf.
///
/// The HashMap is distinguishing two memory areas:
///
/// Entry Space:
/// The entry space is fixed size and contains pointers into the storage space. The entry space operates as a starting point for each chain.
/// This means that the entry space can be thought of buckets in a hash table.
///
/// Storage Space:
/// The storage space contains individual key-value pairs. It does not support variable length keys or values for now.
/// For keys, one could project them beforehand to a fixed length representation, e.g., uin64_t, and then use the newly mapped key.
///
/// IMPORTANT:
/// 1. This hash map is *NOT* thread save and allows for no concurrent accesses, as it does not use any locking, atomics or synchronization primitives.
/// 2. This hash map does not clear the content of the entry. So it is up to the user to initialize values correctly.
class ChainedHashMap final : public HashMap
{
public:
    ChainedHashMap(AbstractBufferProvider* bufferProvider, uint64_t entrySize, uint64_t numberOfBuckets, uint64_t pageSize);
    ChainedHashMap(AbstractBufferProvider* bufferProvider, uint64_t keySize, uint64_t valueSize, uint64_t numberOfBuckets, uint64_t pageSize);
    // ~ChainedHashMap() override;
    std::span<std::byte> allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, size_t neededSize);
    void appendPage(AbstractBufferProvider* bufferProvider);
    AbstractHashMapEntry* insertEntry(HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider) override;
    [[nodiscard]] uint64_t numberOfTuples() const override;
    [[nodiscard]] const TupleBuffer getPage(uint64_t pageIndex) const;
    [[nodiscard]] const TupleBuffer getVarSizedPage(uint64_t pageIndex) const;
    [[nodiscard]] static uint64_t calculateMainBufferSize(uint64_t numberOfChains);
    [[nodiscard]] uint64_t getNumberOfPages() const;
    [[nodiscard]] uint64_t getNumberOfVarSizedPages() const;
    [[nodiscard]] uint64_t numberOfChains() const;
    [[nodiscard]] uint64_t entrySize() const;
    [[nodiscard]] uint64_t entriesPerPage() const;
    [[nodiscard]] uint64_t pageSize() const;
    [[nodiscard]] uint64_t mask() const;
    [[nodiscard]] VariableSizedAccess::Index storageSpaceChildBufferIndex() const;
    [[nodiscard]] VariableSizedAccess::Index varSizedSpaceChildBufferIndex() const;
    [[nodiscard]] ChainedHashMapEntry* getChain(uint64_t pos) const;

    /// Clears and deletes all entries in the hash map. It also releases the memory of any allocated buffers or other memory.
    // void clear() noexcept;

    /// Creates a new chained hash map with the same configuration, i.e., pageSize, entrySize, entriesPerPage and numberOfChains
    static std::unique_ptr<ChainedHashMap> createNewMapWithSameConfiguration(AbstractBufferProvider* bufferProvider, const ChainedHashMap& other);

private:
    friend class ChainedHashMapRef;

    /// Metadata for accessing the main buffer
    struct Offsets
    {
        /// Specifies the number of pre-allocated var sized
        static constexpr auto NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS = 100;
        // for main buffer size calculation and code clarity
        static constexpr auto MAIN_BUFFER_UINT64_FIELDS_NUM = 8;
        static constexpr auto MAIN_BUFFER_UINT32_FIELDS_NUM = 2;
        /// main buffer metadata field position indices (uint64 fields)
        /// always cast to uint64_t when using the offsets below
        static constexpr auto NUM_CHAINS_POS = 0;
        static constexpr auto PAGE_SIZE_POS = 1;
        static constexpr auto ENTRY_SIZE_POS = 2;
        static constexpr auto ENTRIES_PER_PAGE_POS = 3;
        static constexpr auto NUM_TUPLES_POS = 4;
        static constexpr auto MASK_POS = 5;
        static constexpr auto NUM_PAGES_POS = 6;           // actively maintained as pages are inserted
        static constexpr auto NUM_VARSIZED_PAGES_POS = 7;           // actively maintained as varsized pages are inserted
        /// always cast to VariableSizedAccess::Index when using the offsets below
        static constexpr uint32_t STORAGE_SPACE_INDEX_POS = (NUM_VARSIZED_PAGES_POS + 1) * sizeof(uint64_t) / sizeof(VariableSizedAccess::Index);
        static constexpr uint32_t VARSIZED_SPACE_INDEX_POS = STORAGE_SPACE_INDEX_POS + 1; // points to the first varsized space page
        /// always cast to uint64_t when using the offset below
        static constexpr auto CHAINS_BEGIN_POS = (VARSIZED_SPACE_INDEX_POS + 1) * sizeof(VariableSizedAccess::Index) / sizeof(uint64_t);
    };

    inline static const VariableSizedAccess::Index NEXT_PAGE_CHILD_BUFFER_INDEX {
        0
    };

    /// the main tuple buffer containing everything
    TupleBuffer mainBuffer;
};
}
