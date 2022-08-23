#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
#include <Experimental/Interpreter/DataValue/MemRef.hpp>
#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Util/Experimental/HashMap.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

class HashMap {
  public:
    class Entry {
      public:
        Entry(Value<MemRef> ref, int64_t keyOffset, int64_t valueOffset);
        Entry getNext();
        Value<MemRef> getKeyPtr();
        Value<MemRef> getValuePtr();
        Value<Any> isNull();

      private:
        Value<MemRef> ref;
        int64_t keyOffset;
        int64_t valueOffset;
    };
    HashMap(Value<MemRef> hashTableRef,
            int64_t valueOffset,
            std::vector<IR::Types::StampPtr> keyTypes,
            std::vector<IR::Types::StampPtr> valueTypes);

    Entry findOrCreate(std::vector<Value<>> keys);
    Entry findOne(std::vector<Value<>> keys);
    Value<UInt64> calculateHash(std::vector<Value<>> keys);
    Entry createEntry(std::vector<Value<>> keys, Value<UInt64> hash);
    Entry getEntryFromHashTable(Value<UInt64> hash) const;
    Value<> compareKeys(std::vector<Value<>> keyValue, Value<MemRef> keys) const;

  private:
    Value<MemRef> hashTableRef;
    const int64_t valueOffset;
    const std::vector<IR::Types::StampPtr> keyTypes;
    const std::vector<IR::Types::StampPtr> valueTypes;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
