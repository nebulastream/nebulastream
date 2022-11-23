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
#include <Util/Experimental/HashMap.hpp>

namespace NES::Nautilus {

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
            std::vector<Nautilus::IR::Types::StampPtr> keyTypes,
            std::vector<Nautilus::IR::Types::StampPtr> valueTypes);

    Entry findOrCreate(std::vector<Value<>> keys);
    Entry findOne(std::vector<Value<>> keys);
    Value<UInt64> calculateHash(std::vector<Value<>> keys);
    Entry createEntry(std::vector<Value<>> keys, Value<UInt64> hash);
    Entry getEntryFromHashTable(Value<UInt64> hash) const;
    Value<> compareKeys(std::vector<Value<>> keyValue, Value<MemRef> keys) const;
    Value<> isValid(std::vector<Value<>>& keyValue, Entry& entry) const;

  private:
    Value<MemRef> hashTableRef;
    const int64_t valueOffset;
    const std::vector<Nautilus::IR::Types::StampPtr> keyTypes;
    const std::vector<Nautilus::IR::Types::StampPtr> valueTypes;
};

}// namespace NES::Nautilus

#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_UTIL_HASHMAP_HPP_
