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

class ChainedHashMap {
  public:
    using hash_t = uint64_t;
    static const size_t DEFAULT_PAGE_SIZE = 1024;
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
    };

    ChainedHashMap(uint64_t keySize,
                   uint64_t valueSize,
                   uint64_t nrOfKeys,
                   std::unique_ptr<std::pmr::memory_resource> allocator,
                   size_t pageSize = DEFAULT_PAGE_SIZE);

    inline Entry* findChain(hash_t hash) {
        auto pos = hash & mask;
        return entries[pos];
    }

    inline Entry* insertEntry(hash_t hash) {
        auto newEntry = allocateNewEntry();
        newEntry = std::construct_at(newEntry, hash);
        insert(newEntry, hash);
        return newEntry;
    }

     ~ChainedHashMap();

  private:
    const std::unique_ptr<std::pmr::memory_resource> allocator;
    const uint64_t pageSize;
    [[maybe_unused]] const uint64_t keySize;
    [[maybe_unused]] const uint64_t valueSize;
    const uint64_t entrySize;
    const uint64_t entriesPerPage;
    [[maybe_unused]] const uint64_t maskPointer = (~(uint64_t) 0) >> (16);
    const size_t capacity;
    const hash_t mask;
    uint64_t currentSize = 0;
    Entry** entries;
    std::vector<Entry*> pages;

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
