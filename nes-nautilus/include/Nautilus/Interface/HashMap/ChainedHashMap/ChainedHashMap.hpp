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
    ChainedHashMap(TupleBuffer &buffer, uint64_t entrySize, uint64_t numberOfBuckets, uint64_t pageSize);
    ChainedHashMap(TupleBuffer &buffer, uint64_t keySize, uint64_t valueSize, uint64_t numberOfBuckets, uint64_t pageSize);
    ChainedHashMap(TupleBuffer &buffer);

    std::span<std::byte> allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, size_t neededSize);
    void appendPage(AbstractBufferProvider* bufferProvider);
    AbstractHashMapEntry* insertEntry(HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider) override;

    [[nodiscard]] uint64_t numberOfTuples() const override { return header().numTuples; }
    [[nodiscard]] const TupleBuffer getPage(uint64_t pageIndex) const;
    [[nodiscard]] const TupleBuffer getVarSizedPage(uint64_t pageIndex) const;
    [[nodiscard]] static uint64_t calculateBufferSizeFromBuckets(uint64_t numberOfBuckets);
    [[nodiscard]] static uint64_t calculateBufferSizeFromChains(uint64_t numberOfChains);
    [[nodiscard]] uint64_t getNumberOfPages() const { return header().numPages; }
    [[nodiscard]] uint64_t getNumberOfVarSizedPages() const { return header().numVarSizedPages; }
    [[nodiscard]] uint64_t numberOfBuckets() const { return header().numBuckets; }
    [[nodiscard]] uint64_t numberOfChains() const { return header().numChains; }
    [[nodiscard]] uint64_t entrySize() const { return header().entrySize; }
    [[nodiscard]] uint64_t entriesPerPage() const { return header().entriesPerPage; }
    [[nodiscard]] uint64_t pageSize() const { return header().pageSize; }
    [[nodiscard]] uint64_t mask() const { return header().mask; }
    [[nodiscard]] VariableSizedAccess::Index storageSpaceChildBufferIndex() const { return header().storageSpaceIndex; }
    [[nodiscard]] VariableSizedAccess::Index varSizedSpaceChildBufferIndex() const { return header().varSizedSpaceIndex; }
    [[nodiscard]] ChainedHashMapEntry* getChain(uint64_t pos) const;
    [[nodiscard]] static VariableSizedAccess::Index getNextPageChildBufferIndex();
    /// @warning Be super careful with this. Sometimes you need a pointer to the TupleBuffer but you should never alter it outside of this
    /// View and without using its access methods
    [[nodiscard]] TupleBuffer* getBuffer() const { return std::addressof(buffer); }

private:
    friend class ChainedHashMapRef;

    /// Header structure stored at the beginning of the buffer
    struct Header {
        uint64_t numBuckets;
        uint64_t numChains;
        uint64_t pageSize;
        uint64_t entrySize;
        uint64_t entriesPerPage;
        uint64_t numTuples;
        uint64_t mask;
        uint64_t numPages;
        uint64_t numVarSizedPages;
        VariableSizedAccess::Index storageSpaceIndex;
        VariableSizedAccess::Index varSizedSpaceIndex;
        /// Chains array starts immediately after this header
        /// it is dynamically sized based on numChains, so nothing to store in here.
        /// Conceptually, it is like below:
        // uint64_t chains[numChains + 1];
    };

    static constexpr auto NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS = 100;
    inline static const VariableSizedAccess::Index NEXT_PAGE_CHILD_BUFFER_INDEX{0};

    /// Helper util methods for safe access
    [[nodiscard]] Header& header() const {
        return *buffer.getAvailableMemoryArea<Header>().data();
    }

    [[nodiscard]] uint64_t* chains() const {
        auto* data = buffer.getAvailableMemoryArea<uint8_t>().data();
        return reinterpret_cast<uint64_t*>(data + sizeof(Header));
    }

    /// the main tuple buffer containing everything
    TupleBuffer& buffer;
};
}
