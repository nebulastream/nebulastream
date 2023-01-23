//
// Created by pgrulich on 23.01.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_



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

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_DATASTRUCTURES_CHAINEDHASHMAP_CHAINEDHASHMAPREF_HPP_
