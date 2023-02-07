//
// Created by pgrulich on 23.01.23.
//
#include <iterator>

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_

namespace NES::Nautilus::Interface {

class ChainedHashMapRef {
  public:
    class EntryRef {
      public:
        EntryRef(const Value<MemRef>& ref, uint64_t keyOffset, uint64_t valueOffset);
        Value<UInt64> getHash() const;
        EntryRef getNext() const;
        Value<MemRef> getKeyPtr() const;
        Value<MemRef> getValuePtr() const;
        bool operator!=(std::nullptr_t rhs);
        bool operator==(std::nullptr_t rhs);
        Value<MemRef> getRef() const;

      private:
        mutable Value<MemRef> ref;
        mutable uint64_t keyOffset;
        mutable uint64_t valueOffset;
    };
    ChainedHashMapRef(const Value<MemRef>& hashTableRef, uint64_t keySize, uint64_t valueSize);

    EntryRef findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys);
    EntryRef
    findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys, const std::function<void(EntryRef&)>& onInsert);
    EntryRef findOne(const Value<UInt64>& hash, const std::vector<Value<>>& keys);
    void insertEntryOrUpdate(const EntryRef& otherEntry, const std::function<void(EntryRef&)>& update);

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

    Value<UInt64> getCurrentSize();
    EntryIterator begin();
    EntryIterator end();

  private:
    Value<UInt64> getPageSize();
    Value<MemRef> getPage(Value<UInt64> pageIndex);
    Value<UInt64> getEntriesPerPage();
    EntryRef findChain(const Value<UInt64>& hash);
    EntryRef insert(const Value<UInt64>& hash);
    Value<Boolean> compareKeys(EntryRef& entry, const std::vector<Value<>>& keys);
    Value<MemRef> hashTableRef;
    uint64_t keySize;
    uint64_t valueSize;
};

}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
