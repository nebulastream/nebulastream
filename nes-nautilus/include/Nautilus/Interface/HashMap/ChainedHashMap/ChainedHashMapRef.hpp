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
#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASHMAP_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASHMAP_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <functional>
namespace NES {
class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;
}// namespace NES

namespace NES::Nautilus::Interface {

/**
 * @brief A nautilus wrapper to operate on the chained hash map.
 * This class provides convenient methods to insert and access values in the hash map.
 * Furthermore, it provides EntryRef as a wrapper for an individual Entry* and
 * an EntryIterator to iterate over all entries in the hash map.
 */
class ChainedHashMapRef {
  public:
    /**
     * @brief Nautilus reference to an individual entry in chained hash map.
     * This is basically a wrapper around an Entry*.
     */
    class EntryRef {
      public:
        EntryRef(const Value<MemRef>& ref, uint64_t keyOffset, uint64_t valueOffset);

        /**
         * @brief Gets the hash value of this entry.
         * @returns Value<UInt64>
         */
        Value<UInt64> getHash() const;

        /**
         * @brief Gets the pointer to the next entry in the chain.
         * If this entry is the last entry, the returned EntryRef may point to a nullptr.
         * @return EntryRef
         */
        EntryRef getNext() const;

        /**
         * @brief Gets the pointer to the keys in the entry.
         * @return Value<MemRef>
         */
        Value<MemRef> getKeyPtr() const;

        /**
         * @brief Gets the pointer to the values in the entry.
         * @return Value<MemRef>
         */
        Value<MemRef> getValuePtr() const;

        /**
         * @brief Returns true if the next entry is not null.
         * @return bool
         */
        bool operator!=(std::nullptr_t rhs) const;

        /**
         * @brief Returns true if the next entry is null.
         * @return bool
         */
        bool operator==(std::nullptr_t rhs) const;

      private:
        mutable Value<MemRef> ref;
        mutable uint64_t keyOffset;
        mutable uint64_t valueOffset;
    };

    /**
     * @brief Iterator over all entries in the hash map.
     * The iterator extracts the individual pages and visits each entry.
     */
    class EntryIterator {
      public:
        EntryIterator(ChainedHashMapRef& hashTableRef, const Value<UInt64>& entriesPerPage, const Value<UInt64>& currentIndex);
        EntryIterator(ChainedHashMapRef& hashTableRef, const Value<UInt64>& currentIndex);
        EntryIterator& operator++();
        bool operator==(const EntryIterator& other) const;
        EntryRef operator*();

      private:
        ChainedHashMapRef& hashTableRef;
        Value<UInt64> entriesPerPage;
        Value<UInt64> inPageIndex;
        Value<MemRef> currentPage;
        Value<UInt64> currentPageIndex;
        Value<UInt64> currentIndex;
    };

    /**
     * @brief Iterator over all entries in the hash map with a specific key.
     */
    class KeyEntryIterator {
      public:
        KeyEntryIterator(ChainedHashMapRef& hashTableRef,
                         const Value<UInt64>& hash,
                         const std::vector<Value<>>& keys,
                         const Value<UInt64>& currentIndex);
        KeyEntryIterator& operator++();
        bool operator==(KeyEntryIterator other) const;
        bool operator==(std::nullptr_t) const;
        EntryRef operator*() const;

      private:
        ChainedHashMapRef& hashTableRef;
        Value<UInt64> currentIndex;
        std::vector<Value<>> keys;
        EntryRef currentEntry;
    };

    /**
     * @brief Constructor to create a new nautilus wrapper for the hash map.
     * @param hashTableRef reference to the hash map.
     * @param keyDataTypes data types to the keys.
     * @param keySize size of the compound keys in bytes.
     * @param valueSize size of the compound value in bytes.
     */
    ChainedHashMapRef(const Value<MemRef>& hashTableRef,
                      const std::vector<PhysicalTypePtr>& keyDataTypes,
                      uint64_t keySize,
                      uint64_t valueSize);

    /**
     * @brief This function performs a lookup to the hash map with a potentially compound key and an associated hash.
     * If the key as found the entry is returned. Otherwise the entry is a nullptr.
     * @note the hash has to be derived by the keys using the same hash function as when it was inserted the first time.
     * @param hash the hash of the keys derived with a specific hash function.
     * @param keys a list of keys.
     * @return EntryRef
     */
    EntryRef find(const Value<UInt64>& hash, const std::vector<Value<>>& keys);

    /**
     * @brief This function performs a lookup to the hash map with a potentially compound key and an associated hash.
     * It returns an KeyEntryIterator which allows to iterate through all found entries.
     * @note the hash has to be derived by the keys using the same hash function as when it was inserted the first time.
     * @param hash the hash of the keys derived with a specific hash function.
     * @param keys a list of keys.
     * @return KeyEntryIterator
     */
    KeyEntryIterator findAll(const Value<UInt64>& hash, const std::vector<Value<>>& keys);

    /**
     * @brief This function performs a lookup to the hash map with a potentially compound key and an associated hash.
     * If the key as found the entry is returned.
     * If the key was not found a new entry for the set of keys is inserted and returned.
     * New entries contain the hash and the keys but no value.
     * @note the hash has to be derived by the keys using the same hash function as when it was inserted the first time.
     * @param hash the hash of the keys derived with a specific hash function.
     * @param keys a list of keys.
     * @return EntryRef
     */
    EntryRef findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys);

    /**
     * @brief This function performs a lookup to the hash map with a potentially compound key and an associated hash.
     * If the key as found the entry is returned.
     * If the key was not found a new entry for the set of keys is inserted and returned.
     * Furthermore, onInsert is called for each new entry, which allows the initialization of the entry value.
     * @param hash the hash of the keys derived with a specific hash function.
     * @param keys a list of keys.
     * @param onInsert function, which is called for each newly inserted entry.
     * @return EntryRef
     */
    EntryRef
    findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys, const std::function<void(EntryRef&)>& onInsert);

    /**
     * @brief This function inserts an already existing entry from another hash map to this hash map.
     * To this end, we assume that both hash maps use the same hash function.
     * If an entry with the same key already exists the update function is called and allows to merge both entries.
     * @param otherEntry reference to the entry that should be inserted.
     * @param update function, which is called if an entry with the same key already exists.
     */
    void insertEntryOrUpdate(const EntryRef& otherEntry, const std::function<void(EntryRef&)>& update);

    /**
     * @brief Returns the size, in number of entries, of the underling hash map.
     * @return Value<UInt64>
     */
    Value<UInt64> getCurrentSize();

    /**
     * @brief Start of the new iterator.
     * @return EntryIterator
     */
    EntryIterator begin();

    /**
     * @brief End of the new iterator
     * @return EntryIterator
     */
    EntryIterator end();

    /**
     * @brief This function performs an insertion of a new entry to the hash map.
     * If an entry with the same hash already exists we append it as the head of the chain.
     * @param hash the hash of the keys derived with a specific hash function.
     * @param keys a list of keys.
     * @return EntryRef
     */
    EntryRef insert(const Value<UInt64>& hash, const std::vector<Value<>>& keys);

  private:
    Value<UInt64> getPageSize();
    Value<MemRef> getPage(const Value<UInt64>& pageIndex);
    Value<UInt64> getEntriesPerPage();
    EntryRef findChain(const Value<UInt64>& hash);
    EntryRef insert(const Value<UInt64>& hash);
    Value<Boolean> compareKeys(EntryRef& entry, const std::vector<Value<>>& keys);
    Value<MemRef> hashTableRef;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    uint64_t keySize;
    uint64_t valueSize;
};

}// namespace NES::Nautilus::Interface

#endif // NES_NAUTILUS_INCLUDE_NAUTILUS_INTERFACE_HASHMAP_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
