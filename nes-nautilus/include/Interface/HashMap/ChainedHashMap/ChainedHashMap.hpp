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
#include <optional>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Buffer.hpp>

namespace NES
{
/// Forward declaration of the ChainedHashMapRef, to avoid cyclic dependencies between ChainedHashMap and ChainedHashMapRef
class ChainedHashMapRef;

/// Sizing for a ChainedHashMap view, passed to init(). An engaged bloomFilter enables the in-map BloomFilter
/// and reserves its bit area inline in the map's buffer; std::nullopt allocates nothing for it.
struct ChainedHashMapConfig
{
    uint64_t entrySize;
    uint64_t numberOfBuckets;
    uint64_t pageSize;
    std::optional<Nautilus::Interface::BloomFilterParams> bloomFilter = std::nullopt;

    [[nodiscard]] uint64_t bloomFilterMemAreaSize() const { return bloomFilter ? bloomFilter->allocationByteCount() : 0; }

    /// Bytes the Buffer backing a map with this config must provide.
    [[nodiscard]] uint64_t bufferSize() const;
};

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
/// IMPORTANT — differences from std::unordered_map:
/// 1. This hash map is *NOT* thread safe and allows for no concurrent accesses, as it does not use any locking, atomics or synchronization primitives.
/// 2. This hash map does not clear the content of the entry. So it is up to the user to initialize values correctly.
/// 3. There is no erase()/element removal: once inserted, an entry cannot be removed from the map.
/// 4. There is no automatic rehashing: the number of buckets is fixed at init() and never grows to bound the load factor, unlike
///    std::unordered_map's max_load_factor-triggered rehash. Callers must size numberOfBuckets for the expected cardinality upfront.
/// 5. findOrCreateEntry() (see {@refitem HashMapRef.hpp}) is first-write-wins: on a colliding key, the existing entry is returned
///    unmodified rather than overwritten as std::unordered_map::operator[]/insert_or_assign would. Use insertOrUpdateEntry() for
///    update-on-collision semantics.
class ChainedHashMap final : public HashMap
{
public:
    /// @brief Use init to initialize a ChainedHashMap view on a pre-allocated Buffer
    /// Constructors are private
    /// The buffer must be at least config.bufferSize() bytes; entries built from a key/value split size their
    /// entrySize as sizeof(ChainedHashMapEntry) + keySize + valueSize.
    static void init(Buffer& tupleBuffer, const ChainedHashMapConfig& config);

    /// @brief Loads a ChainedHashMap view from a pre-filled Buffer
    static ChainedHashMap load(const Buffer& tupleBuffer);

    std::span<std::byte> allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, size_t neededSize);
    AbstractHashMapEntry* insertEntry(HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider) override;

    [[nodiscard]] uint64_t getTotalNumberOfRecords() const override { return header().numRecords; }

    [[nodiscard]] Buffer getPage(uint64_t pageIndex) const;
    [[nodiscard]] Buffer getVarSizedPage(uint64_t pageIndex) const;

    /// Size of the buffer a ChainedHashMap view needs: header, chains array and the in-map BloomFilter bit
    /// area. The bloom size is a parameter rather than an afterthought so no caller can allocate a buffer
    /// that init() then overruns.
    [[nodiscard]] static uint64_t calculateBufferSizeFromBuckets(uint64_t numberOfBuckets, uint64_t bloomFilterMemAreaSize);
    [[nodiscard]] static uint64_t calculateBufferSizeFromChains(uint64_t numberOfChains, uint64_t bloomFilterMemAreaSize);
    [[nodiscard]] uint64_t getNumberOfPages() const;
    [[nodiscard]] uint64_t getNumberOfVarSizedPages() const;

    [[nodiscard]] uint64_t getStatus() const { return header().status; }

    [[nodiscard]] uint64_t getNumberOfBuckets() const { return header().numBuckets; }

    [[nodiscard]] uint64_t getNumberOfChains() const { return header().numChains; }

    [[nodiscard]] uint64_t getEntrySize() const { return header().entrySize; }

    [[nodiscard]] uint64_t getEntriesPerPage() const { return header().entriesPerPage; }

    [[nodiscard]] uint64_t getPageSize() const { return header().pageSize; }

    [[nodiscard]] uint64_t getMask() const { return header().mask; }

    /// Sizing of the optional in-map BloomFilter (nullopt when disabled). Used to propagate the filter when a
    /// derived map is created from an existing one (e.g. the aggregation final map).
    [[nodiscard]] const std::optional<Nautilus::Interface::BloomFilterParams>& getBloomFilterParams() const { return header().bloomFilter; }

    [[nodiscard]] ChildBufferIndex getStorageBufferIdx() const;
    [[nodiscard]] ChildBufferIndex getVarSizedBufferIdx() const;
    [[nodiscard]] ChainedHashMapEntry* getChain(uint64_t pos);

    /// Pointer to the optional in-map BloomFilter bit area, consulted by ChainedHashMapRef::findChain to
    /// short-circuit chain traversal. The area lives inline in this buffer right behind the chains array and
    /// is zeroed by init(), so it is valid from construction on. Returns nullptr when the filter is disabled.
    [[nodiscard]] uint64_t* getBloomFilterMemArea();

    /// @warning Be super careful with this. Sometimes you need a pointer to the Buffer but you should never alter it outside of this
    /// view and without using its access methods
    [[nodiscard]] Buffer* getBuffer() { return std::addressof(buffer); }

    /// HashMapSlice magic numbers
    static constexpr auto VALID_CHM = 82543427462775423;
    static constexpr auto INVALID_CHM = 0;
    static constexpr auto FIXED_STORAGE_SPACE_BUFFER_SIZE = 4;
    static constexpr auto VARSIZED_STORAGE_SPACE_BUFFER_SIZE = 4;

protected:
    void appendPage(AbstractBufferProvider* bufferProvider);
    void allocateNewVarSizedPage(AbstractBufferProvider* bufferProvider, size_t neededSize);

private:
    /// private constructor that takes a pre-filled buffer
    explicit ChainedHashMap(Buffer buffer) : buffer(std::move(buffer)) { }

    friend class ChainedHashMapRef;

    /// Header structure stored at the beginning of the buffer
    struct Header
    {
        uint64_t status;
        uint64_t numBuckets;
        uint64_t numChains;
        uint64_t pageSize;
        uint64_t entrySize;
        uint64_t entriesPerPage;
        uint64_t numRecords = 0;
        uint64_t mask;
        ChildBufferIndex storageSpaceIndex;
        ChildBufferIndex varSizedSpaceIndex;
        /// Sizing of the optional in-map BloomFilter, nullopt when disabled. The bit area itself sits inline
        /// behind the chains array, so no index into a child buffer is needed.
        std::optional<Nautilus::Interface::BloomFilterParams> bloomFilter;

        /// Chains array starts immediately after this header, followed by the BloomFilter bit area.
        /// Both are dynamically sized (based on numChains and bloomFilterMemAreaSize), so nothing to store
        /// in here. Conceptually, it is like below:
        /// uint64_t chains[numChains + 1];
        /// uint64_t bloomBits[bloomFilterMemAreaSize / sizeof(uint64_t)];
        Header(
            uint64_t numBuckets,
            uint64_t numChains,
            uint64_t pageSize,
            uint64_t entrySize,
            uint64_t entriesPerPage,
            uint64_t mask,
            std::optional<Nautilus::Interface::BloomFilterParams> bloomFilter)
            : status(VALID_CHM)
            , numBuckets(numBuckets)
            , numChains(numChains)
            , pageSize(pageSize)
            , entrySize(entrySize)
            , entriesPerPage(entriesPerPage)
            , mask(mask)
            , storageSpaceIndex(Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
            , varSizedSpaceIndex(Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
            , bloomFilter(bloomFilter)
        {
        }
    };

    static_assert(std::is_trivially_destructible_v<Header>, "Header must be trivially destructible");

    /// Helper util methods for safe access
    [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

    [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<const Header>().data(); }

    [[nodiscard]] std::span<ChainedHashMapEntry*> chains()
    {
        auto* data = buffer.getAvailableMemoryArea<uint8_t>().data();

        /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
        auto* entries = reinterpret_cast<ChainedHashMapEntry**>(data + sizeof(Header));
        return {entries, getNumberOfChains() + 1};
        /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

    /// The in-map BloomFilter bit area, sitting inline right behind the chains array. Empty when the filter
    /// is disabled. allocationByteCount() rounds to whole words, so the span is always word-aligned in size.
    [[nodiscard]] std::span<uint64_t> bloomBits()
    {
        const auto& bloomFilter = header().bloomFilter;
        const auto memAreaSize = bloomFilter ? bloomFilter->allocationByteCount() : 0;
        if (memAreaSize == 0)
        {
            return {};
        }
        auto* data = buffer.getAvailableMemoryArea<uint8_t>().data();

        /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
        auto* bits = reinterpret_cast<uint64_t*>(data + sizeof(Header) + ((getNumberOfChains() + 1) * sizeof(ChainedHashMapEntry*)));
        return {bits, memAreaSize / sizeof(uint64_t)};
        /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
    }

    /// the main tuple buffer for this chained hash map
    Buffer buffer;
};
}
