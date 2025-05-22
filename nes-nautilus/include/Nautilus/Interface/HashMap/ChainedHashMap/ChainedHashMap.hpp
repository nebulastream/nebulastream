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

#include <cstdint>
#include <functional>
#include <vector>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>

namespace NES::Nautilus::Interface
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
    static constexpr auto DEFAULT_PAGE_SIZE = 8024;

    ChainedHashMap(uint64_t keySize, uint64_t valueSize, uint64_t numberOfBuckets, uint64_t pageSize);
    ~ChainedHashMap() override;
    [[nodiscard]] ChainedHashMapEntry* findChain(HashFunction::HashValue::raw_type hash) const;
    AbstractHashMapEntry* insertEntry(HashFunction::HashValue::raw_type hash, Memory::AbstractBufferProvider* bufferProvider) override;
    [[nodiscard]] uint64_t getNumberOfTuples() const override;
    [[nodiscard]] const ChainedHashMapEntry* getPage(uint64_t pageIndex) const;
    [[nodiscard]] ChainedHashMapEntry* getStartOfChain(uint64_t entryIdx) const;
    [[nodiscard]] uint64_t getNumberOfChains() const;

    /// Clears and deletes all entries in the hash map. It also releases the memory of any allocated buffers or other memory.
    void clear() noexcept;

    /// The passed method is being executed, once the destructor is called. This is necessary as the value type of this hash map
    /// might allocate its own memory. Thus, the destructor of the value type should be called to release the memory.
    void setDestructorCallback(const std::function<void(ChainedHashMapEntry*)>& callback);

private:
    friend class ChainedHashMapRef;

    Memory::TupleBuffer entrySpace;
    std::vector<Memory::TupleBuffer> storageSpace;
    uint64_t numberOfTuples; /// Number of entries in the hash map
    uint64_t pageSize; /// Size of one storage page in bytes
    uint64_t entrySize; /// Size of one entry: sizeof(ChainedHashMapEntry) + keySize + valueSize
    uint64_t entriesPerPage; /// Number of entries per page
    uint64_t numberOfChains; /// Number of buckets in the hash map
    ChainedHashMapEntry** entries; /// Stores the pointers to the first entry in each chain
    HashFunction::HashValue::raw_type mask; /// Mask to calculate the bucket position from the hash value. Always a (power of 2)-1
    std::function<void(ChainedHashMapEntry*)> destructorCallBack; /// Callback function to be executed, once the destructor is called
};
}
