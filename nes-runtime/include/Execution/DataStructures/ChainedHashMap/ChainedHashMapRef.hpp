//
// Created by pgrulich on 23.01.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_

namespace NES::Nautilus::Interface {

class ChainedHashMapRef {
  public:
    class EntryRef {
      public:
        EntryRef(Value<MemRef> ref, int64_t keyOffset, int64_t valueOffset);
        EntryRef getNext();
        Value<MemRef> getKeyPtr();
        Value<MemRef> getValuePtr();
        bool operator!=(std::nullptr_t rhs);

      private:
        Value<MemRef> ref;
        uint64_t keyOffset;
        uint64_t valueOffset;
    };
    ChainedHashMapRef(Value<MemRef> hashTableRef, uint64_t keySize);

    EntryRef findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys);
    EntryRef findOrCreate(const Value<UInt64>& hash,
                          const std::vector<Value<>>& keys,
                          const std::function<void(const EntryRef&)>& onInsert);
    EntryRef findOne(const Value<UInt64>& hash, const std::vector<Value<>>& keys);

  private:
    EntryRef findChain(const Value<UInt64>& hash);
    EntryRef insert(const Value<UInt64>& hash);
    Value<Boolean> compareKeys(EntryRef entry, const std::vector<Value<>>& keys);
    Value<MemRef> hashTableRef;
    uint64_t keyOffset;
    uint64_t valueOffset;
};
}// namespace NES::Nautilus::Interface

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
