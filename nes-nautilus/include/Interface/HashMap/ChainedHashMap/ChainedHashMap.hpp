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
#include <type_traits>
#include <utility>
#include <vector>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

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
/// IMPORTANT — differences from std::unordered_map:
/// 1. This hash map is *NOT* thread safe and allows for no concurrent accesses, as it does not use any locking, atomics or synchronization primitives.
/// 2. This hash map does not clear the content of the entry. So it is up to the user to initialize values correctly.
/// 3. There is no erase()/element removal: once inserted, an entry cannot be removed from the map.
/// 4. There is no automatic rehashing: the number of buckets is fixed at query-compilation time and never grows to bound the load
///    factor, unlike std::unordered_map's max_load_factor-triggered rehash. Callers must size numberOfBuckets for the expected
///    cardinality upfront. A map may therefore hold far more entries than it has buckets; the chains simply get longer.
/// 5. findOrCreateEntry() (see {@refitem HashMapRef.hpp}) is first-write-wins: on a colliding key, the existing entry is returned
///    unmodified rather than overwritten as std::unordered_map::operator[]/insert_or_assign would. Use insertOrUpdateEntry() for
///    update-on-collision semantics.
class ChainedHashMap final : public HashMap
{
public:
    /// @brief Use init to initialize a ChainedHashMap view on a pre-allocated TupleBuffer
    /// Constructors are private
    /// The buffer must be at least bufferSize() bytes; entries built from a key/value split size their
    /// entrySize as sizeof(ChainedHashMapEntry) + keySize + valueSize.
    /// The scalar overload exists for traced code, which cannot receive a ChainedHashMapConfig through a
    /// non-capturing invoke lambda. Both must be handed the numbers of one and the same config.
    /// entrySize and pageSize are only ever read by preconditions, so they are compiled away with them.
    static void init(
        TupleBuffer& tupleBuffer IF_PRECONDITION(, uint64_t entrySize),
        uint64_t numberOfBuckets IF_PRECONDITION(, uint64_t pageSize),
        uint64_t bloomBytes);
    static void init(TupleBuffer& tupleBuffer, const ChainedHashMapConfig& config);

    /// Power of two >= numberOfBuckets / ChainedHashMapConfig::assumedLoadFactor: the real size of the chains
    /// array. Lives here rather than on the config because it is a property of this map's memory layout.
    [[nodiscard]] static uint64_t calculateNumberOfChains(uint64_t numberOfBuckets);

    /// Mask applied to a hash to pick a chain. Always calculateNumberOfChains() - 1, since that is a power of 2.
    [[nodiscard]] static uint64_t calculateMask(uint64_t numberOfBuckets);

    /// Bytes the TupleBuffer backing a map with this sizing must provide: header, chains array and the
    /// BloomFilter bit area.
    [[nodiscard]] static uint64_t calculateBufferSize(uint64_t numberOfBuckets, uint64_t bloomBytes);


    /// @brief Loads a ChainedHashMap view from a pre-filled TupleBuffer
    static ChainedHashMap load(const TupleBuffer& tupleBuffer);

    std::span<std::byte> allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, size_t neededSize);

    /// Scalar sizing because the only caller is a traced invoke proxy. The numbers must come from the very
    /// config the map was init()ed with: nothing validates that any more, since the map no longer carries its
    /// sizing, and a mismatched entrySize or mask corrupts the map silently.
    AbstractHashMapEntry* insertEntry(
        HashFunction::HashValue::raw_type hash,
        AbstractBufferProvider* bufferProvider,
        uint64_t entrySize,
        uint64_t entriesPerPage,
        uint64_t pageSize,
        uint64_t mask);

    [[nodiscard]] uint64_t getTotalNumberOfRecords() const override { return header().numRecords; }

    [[nodiscard]] TupleBuffer getPage(uint64_t pageIndex) const;
    [[nodiscard]] TupleBuffer getVarSizedPage(uint64_t pageIndex) const;

    [[nodiscard]] uint64_t getNumberOfPages() const;
    [[nodiscard]] uint64_t getNumberOfVarSizedPages() const;

    [[nodiscard]] ChildBufferIndex getStorageBufferIdx() const;
    [[nodiscard]] ChildBufferIndex getVarSizedBufferIdx() const;

    /// The chain head for an already-masked position. The caller masks, because the mask is a
    /// query-compile-time constant and folds into an immediate in the traced probe path.
    [[nodiscard]] ChainedHashMapEntry* getChain(uint64_t pos);

    /// Pointer to the in-map BloomFilter bit area, consulted by ChainedHashMapRef::findChain to short-circuit
    /// chain traversal. The area lives inline in this buffer right behind the chains array and is zeroed by
    /// init(), so it is valid from construction on. Raw pointer rather than the span below, because the sole
    /// caller is a traced invoke proxy. Requires a non-zero bloomBytes: a disabled filter has no area.
    [[nodiscard]] uint64_t* getBloomFilterMemArea(uint64_t numberOfChains, uint64_t bloomBytes);

    /// @warning Be super careful with this. Sometimes you need a pointer to the TupleBuffer but you should never alter it outside of this
    /// view and without using its access methods
    [[nodiscard]] TupleBuffer* getBuffer() { return std::addressof(buffer); }

    /// HashMapSlice magic numbers
    static constexpr auto VALID_CHM = 82543427462775423;
    static constexpr auto FIXED_STORAGE_SPACE_BUFFER_SIZE = 4;
    static constexpr auto VARSIZED_STORAGE_SPACE_BUFFER_SIZE = 4;

protected:
    void appendPage(AbstractBufferProvider* bufferProvider, uint64_t pageSize);
    void allocateNewVarSizedPage(AbstractBufferProvider* bufferProvider, size_t neededSize);

private:
    /// private constructor that takes a pre-filled buffer
    explicit ChainedHashMap(TupleBuffer buffer) : buffer(std::move(buffer)) { }

    friend class ChainedHashMapRef;

    /// Header structure stored at the beginning of the buffer. It holds only state that genuinely changes
    /// while the query runs — all sizing lives in the ChainedHashMapConfig the caller passes in.
    ///
    /// The chains array starts immediately after this header, followed by the BloomFilter bit area. Both are
    /// sized from the config, so nothing about them is stored here. Conceptually:
    /// ChainedHashMapEntry* chains[numberOfChains + 1];
    /// uint64_t bloomBits[bloomFilterMemAreaSize / sizeof(uint64_t)];
    struct Header
    {
        uint64_t status = VALID_CHM;
        uint64_t numRecords = 0;
        ChildBufferIndex storageSpaceIndex = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
        ChildBufferIndex varSizedSpaceIndex = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    };

    static_assert(std::is_trivially_destructible_v<Header>, "Header must be trivially destructible");
    static_assert(sizeof(Header) % alignof(ChainedHashMapEntry*) == 0, "The chains array follows the header directly");

    /// Helper util methods for safe access
    [[nodiscard]] Header& header() { return *buffer.getAvailableMemoryArea<Header>().data(); }

    [[nodiscard]] const Header& header() const { return *buffer.getAvailableMemoryArea<const Header>().data(); }

    /// The chains array, sitting inline right behind the header. The length is not stored anywhere, so only
    /// the callers that know the chain count can form a bounded span; getChain() needs the base address only.
    [[nodiscard]] ChainedHashMapEntry** chainsBegin()
    {
        auto* data = buffer.getAvailableMemoryArea<uint8_t>().data();

        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
        return reinterpret_cast<ChainedHashMapEntry**>(data + sizeof(Header));
    }

    /// The in-map BloomFilter bit area, sitting inline right behind the chains array. Empty when the filter is
    /// disabled, where data() is one past the end of the buffer — hence a span, which a caller cannot walk off.
    /// allocationByteCount() rounds to whole words, so the area is always word-aligned in size.
    [[nodiscard]] std::span<uint64_t> bloomBits(const uint64_t numberOfChains, const uint64_t bloomBytes)
    {
        auto* data = buffer.getAvailableMemoryArea<uint8_t>().data();

        /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
        auto* bits = reinterpret_cast<uint64_t*>(data + sizeof(Header) + ((numberOfChains + 1) * sizeof(ChainedHashMapEntry*)));
        return {bits, bloomBytes / sizeof(uint64_t)};
    }

    /// the main tuple buffer for this chained hash map
    TupleBuffer buffer;
};
}
