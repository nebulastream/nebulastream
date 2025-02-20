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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASHMAP_CHAINEDHASHMAP_CHAINEDHASHMAP_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASHMAP_CHAINEDHASHMAP_CHAINEDHASHMAP_HPP_
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Runtime/Allocator/MemoryResource.hpp>
#include <cassert>

namespace NES::Nautilus::Interface {
class ChainedHashMapRef;

/**
 * @brief Implementation of a single thread chained HashMap.
 * To operate on the hash-map, {@refitem ChainedHashMapRef.hpp} provides a Nautilus wrapper.
 * The implementation origins from Kersten et.al. https://github.com/TimoKersten/db-engine-paradigms and Leis et.al
 * https://db.in.tum.de/~leis/papers/morsels.pdf.
 *
 * The HashMap is distinguishes two memory areas:
 *
 * Entry Space:
 * The entry space is fixed size and contains pointers into the storage space.
 *
 * Storage Space:
 * The storage space contains individual key-value pairs.
 *
 * @attention
 * 1. This hash map is *not* thread save and allows for no concurrent accesses
 * 2. This hash map dose not clears the content of the entry. So its up to the user to initialize values correctly.
 */
class ChainedHashMap {
  public:
    using hash_t = uint64_t;
    static const size_t DEFAULT_PAGE_SIZE = 8024;
    /**
     * @brief ChainedHashMap Entry
     * Each entry contains a ptr to the next element, the hash of the current value and the keys and values.
     * The physical layout is the following
     * | --- Entry* --- | --- hash_t --- | --- keys ---     | --- values ---    |
     * | --- 64bit ---  | --- 64bit ---  | --- keySize ---  | --- valueSize ---  |
     */
    class Entry {
      public:
        Entry* next;
        hash_t hash;
        // payload data follows this header
        explicit Entry(hash_t hash) : next(nullptr), hash(hash){};
    };

    /**
     * @brief Constructor for a the chained hash map.
     * @param keySize size of the keys in bytes.
     * @param valueSize size of the values in bytes.
     * @param numberOfKeys a number of keys, which are assumed to be stored in the hash map.
     * This is used to size the entry area. A too small number of keys will increase coalitions. A too large number of keys will increase space requirements.
     * @param allocator the memory allocator, which is used to allocate space for entries.
     * @param pageSize the page size with DEFAULT_PAGE_SIZE as default.
     */
    ChainedHashMap(uint64_t keySize,
                   uint64_t valueSize,
                   uint64_t numberOfKeys,
                   std::unique_ptr<std::pmr::memory_resource> allocator,
                   size_t pageSize = DEFAULT_PAGE_SIZE);

    /**
     * @brief Inline implementation to find the start of a chain from the hash map.
     * @param hash the hash for the key.
     * @return Entry* to the start of the chain.
     */
    [[nodiscard]] inline Entry* findChain(hash_t hash) const {
        auto pos = hash & mask;
        return entries[pos];
    }

    /**
     * @brief Inline implementation to insert a new entry in the hash map for a specific hash.
     * This allocates a new entry, and inserts it to the hash map.
     * @param hash the hash for the key.
     * @return Entry* to the new entry.
     */
    inline Entry* insertEntry(hash_t hash) {
        auto* newEntry = allocateNewEntry();
        // call the constructor of Entry at the address of new Entry to initialize the object.
        new (newEntry) Entry(hash);
        insert(newEntry, hash);
        return newEntry;
    }

    /**
     * @brief Returns the current size in number of entries of the hash map.
     * @return number of entries.
     */
    [[nodiscard]] uint64_t getCurrentSize() const;

    /**
     * @brief Returns a page of entries.
     * @param pageIndex page index
     * @return int8_t*
     */
    int8_t* getPage(uint64_t pageIndex);

    /**
     * @brief Inserts a page of entries to the hash table.
     * This assumes that entries have the same shape and size as the the entries of this hash map
     * This avoids any allocations, and inserts the page directly to the map, thus the ownership of the page also moves.
     * @param page
     * @param numberOfEntries
     */
    void insertPage(int8_t* page, uint64_t numberOfEntries);

    /**
     * @brief This serializes the complete ChainedHashMap including its content
     * @return serialized ChainedHashMap
     */
    std::vector<uint8_t> serialize() const;

    /**
     * @brief This deserializes the complete ChainedHashMap including its contents
     * @param data serialized version of ChainedHashMap
     * @param length length of data
     * @param allocator
     * @return ChainedHashMap
     */
    static std::unique_ptr<ChainedHashMap> deserialize(const uint8_t* data, size_t length, std::unique_ptr<std::pmr::memory_resource> allocator);

    /**
     * @brief Destructs the hash map and releases all associated resources.
     * All pointers to entries of the hash map become invalid and accesses are undefined.
     */
    virtual ~ChainedHashMap();

  private:
    /**
     * @brief Inserts a new entry to the hash map. Each new entry will be stored as the first element in the bucket chain.
     * @param entry pointer to the new entry
     * @param hash of the new entry
     */
    inline void insert(Entry* entry, hash_t hash) {
        const size_t pos = hash & mask;
        assert(pos <= mask);
        assert(pos < capacity);
        auto oldValue = entries[pos];
        entry->next = oldValue;
        entries[pos] = entry;
        this->currentSize++;
    }

    /**
     * @brief Allocates a new entry.
     * If the current page has enough space the new entry will be placed in the current page.
     * If the page is full, we will allocate a new page.
     * @return Entry*
     */
    Entry* allocateNewEntry();

    /**
     * @brief Function to determine the pointer to an entry at a specific index.
     * @param entryIndex
     * @return Entry*
     */
    Entry* entryIndexToAddress(uint64_t entryIndex);

  private:
    // ChainedHashMapRef is a friend to access private members and functions
    friend ChainedHashMapRef;
    const std::unique_ptr<std::pmr::memory_resource> allocator;
    const uint64_t pageSize;
    const uint64_t keySize;
    const uint64_t valueSize;
    const uint64_t entrySize;
    const uint64_t entriesPerPage;
    const size_t capacity;
    const hash_t mask;
    uint64_t currentSize = 0;
    Entry** entries;
    std::vector<int8_t*> pages;  // serialise this
};
}// namespace NES::Nautilus::Interface

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASHMAP_CHAINEDHASHMAP_CHAINEDHASHMAP_HPP_
