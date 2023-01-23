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

namespace NES::Nautilus::Interface {

class ChainedHashTable {
  public:
    using hash_t = uint64_t;
    /**
     * @brief HashTable Entry
     * Each entry contains a ptr to the next element, the hash of the current value and the keys and values.
     */
    class Entry {
      public:
        Entry* next;
        hash_t hash;
        // payload data follows this header
        Entry(Entry* next, hash_t hash);
    };

    ChainedHashTable(uint64_t keySize, uint64_t valueSize, uint64_t nrOfKeys);

    Entry* findChain(hash_t hash) {
        auto pos = hash & mask;
        return entries[pos];
    }

    Entry* insertEntry(hash_t hash) {
        auto newEntry = allocateNewEntry();
        newEntry->hash = hash;
        insert(newEntry, hash);
        return newEntry;
    }

    void insert(Entry* entry, hash_t hash) {
        const size_t pos = hash & mask;
        assert(pos <= mask);
        assert(pos < capacity);
        auto oldValue = entries[pos];
        entry->next = oldValue;
        entries[pos] = entry;
        this->currentSize++;
    }

    Entry* allocateNewEntry() {
        // check if a new page should be allocated
        if (currentSize % entriesPerPage == 0) {
            // TODO alloc
            // set entries to zero
            pages.emplace_back(buffer.value());
        }
        return entryIndexToAddress(currentSize);
    }

    Entry* entryIndexToAddress(uint64_t entry) {
        auto pageIndex = entry / entriesPerPage;
        auto* page = pages[pageIndex];
        auto entryOffsetInBuffer = entry - (pageIndex * entriesPerPage);
        return page + entryOffsetInBuffer;
    }

  private:
    const uint64_t keySize;
    const uint64_t valueSize;
    const uint64_t entriesPerPage;
    const hash_t mask;
    const uint64_t maskPointer = (~(uint64_t) 0) >> (16);
    const uint64_t maskTag = (~(uint64_t) 0) << (sizeof(uint64_t) * 8 - 16);
    size_t capacity = 0;
    uint64_t currentSize = 0;
    Entry** entries;
    std::vector<Entry*> pages;
};

class ChainedHashMapRef {
  public:
    class EntryRef {
      public:
        EntryRef(Value<MemRef> ref, int64_t keyOffset, int64_t valueOffset);
        EntryRef getNext();
        Value<MemRef> getKeyPtr();
        Value<MemRef> getValuePtr();

      private:
        Value<MemRef> ref;
        int64_t keyOffset;
        int64_t valueOffset;
    };
    ChainedHashMapRef(Value<MemRef> hashTableRef,
                      int64_t valueOffset,
                      std::vector<Nautilus::IR::Types::StampPtr> keyTypes,
                      std::vector<Nautilus::IR::Types::StampPtr> valueTypes);

    EntryRef findOrCreate(std::vector<Value<>> keys);
    EntryRef findOne(std::vector<Value<>> keys);
    Value<UInt64> calculateHash(std::vector<Value<>> keys);
    EntryRef createEntry(std::vector<Value<>> keys, Value<UInt64> hash);
    EntryRef getEntryFromHashTable(Value<UInt64> hash) const;
    Value<> compareKeys(std::vector<Value<>> keyValue, Value<MemRef> keys) const;
    Value<> isValid(std::vector<Value<>>& keyValue, EntryRef& entry) const;

  private:
    Value<MemRef> hashTableRef;
    const int64_t valueOffset;
    const std::vector<Nautilus::IR::Types::StampPtr> keyTypes;
    const std::vector<Nautilus::IR::Types::StampPtr> valueTypes;
};

}// namespace NES::Nautilus::Interface

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
