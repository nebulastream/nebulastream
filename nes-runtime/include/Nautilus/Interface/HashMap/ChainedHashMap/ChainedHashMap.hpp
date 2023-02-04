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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <memory_resource>

namespace NES::Nautilus::Interface {
class ChainedHashMapRef;
class ChainedHashMap {
  public:
    using hash_t = uint64_t;
    static const size_t DEFAULT_PAGE_SIZE = 8024;
    /**
     * @brief HashTable Entry
     * Each entry contains a ptr to the next element, the hash of the current value and the keys and values.
     */
    class Entry {
      public:
        Entry* next;
        hash_t hash;
        // payload data follows this header
        explicit Entry(hash_t hash) : next(nullptr), hash(hash){};
        constexpr int8_t* dataOffset() { return ((int8_t*) this) + sizeof(Entry); }
    };

    ChainedHashMap(uint64_t keySize,
                   uint64_t valueSize,
                   uint64_t nrOfKeys,
                   std::unique_ptr<std::pmr::memory_resource> allocator,
                   size_t pageSize = DEFAULT_PAGE_SIZE);

    [[nodiscard]] inline Entry* findChain(hash_t hash) const {
        auto pos = hash & mask;
        return entries[pos];
    }

    inline Entry* insertEntry(hash_t hash) {
        auto newEntry = allocateNewEntry();
        newEntry = std::construct_at(newEntry, hash);
        insert(newEntry, hash);
        return newEntry;
    }
    [[nodiscard]] uint64_t getCurrentSize() const;

    /**
     * @brief Inserts an entry to this hash table if the hash and key dose not exists yet.
     * @param otherEntry
     * @return
     */
    bool insertEntryIfNotExists(Entry* otherEntry) {
        // check if an entry with the same payload exists in this hash map.
        auto entry = findChain(otherEntry->hash);
        for (; entry != nullptr; entry = entry->next) {
            // use memcmp to check if the keys of both entries are equal.
            // if they are equal return false if no new entry was inserted.
            if (memcmp(otherEntry->dataOffset(), entry->dataOffset(), this->keySize) == 0) {
                return false;
            }
        }
        // insert new entry to hash map and copy data (keys + values) from old entry.
        auto newEntry = insertEntry(otherEntry->hash);
        memcpy(newEntry->dataOffset(), otherEntry->dataOffset(), keySize + valueSize);
        return true;
    };

    virtual ~ChainedHashMap();

    int8_t* getPage(uint64_t pageIndex);

  private:
    friend ChainedHashMapRef;
    const std::unique_ptr<std::pmr::memory_resource> allocator;
    const uint64_t pageSize;
    [[maybe_unused]] const uint64_t keySize;
    [[maybe_unused]] const uint64_t valueSize;
    const uint64_t entrySize;
    const uint64_t entriesPerPage;
    const size_t capacity;
    const hash_t mask;
    uint64_t currentSize = 0;
    Entry** entries;
    std::vector<int8_t*> pages;

    inline void insert(Entry* entry, hash_t hash) {
        const size_t pos = hash & mask;
        assert(pos <= mask);
        assert(pos < capacity);
        auto oldValue = entries[pos];
        entry->next = oldValue;
        entries[pos] = entry;
        this->currentSize++;
    }
    Entry* allocateNewEntry();
    Entry* entryIndexToAddress(uint64_t entryIndex);
};
}// namespace NES::Nautilus::Interface

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
