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
#include <Experimental/Interpreter/Util/HashMap.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus {

HashMap::Entry::Entry(Value<MemRef> ref, int64_t keyOffset, int64_t valueOffset)
    : ref(ref), keyOffset(keyOffset), valueOffset(valueOffset) {}

extern "C" void* getNextEntry(void* entryPtr) {
    auto entry = (NES::Experimental::Hashmap::Entry*) entryPtr;
    return entry->next;
}
HashMap::Entry HashMap::Entry::getNext() {
    auto value = FunctionCall<>("getNextEntry", getNextEntry, ref);
    return Entry(value, keyOffset, valueOffset);
}
Value<MemRef> HashMap::Entry::getKeyPtr() { return (ref + keyOffset).as<MemRef>(); }
Value<MemRef> HashMap::Entry::getValuePtr() { return (ref + valueOffset).as<MemRef>(); }
Value<Any> HashMap::Entry::isNull() { return ref == 0; }

extern "C" void* getHashEntry(void* state, uint64_t hash) {
    auto hashMap = (NES::Experimental::Hashmap*) state;
    auto entry = hashMap->find_chain(hash);
    return entry;
}
extern "C" void* createEntryProxy(void* state, uint64_t hash) {
    auto hashMap = (NES::Experimental::Hashmap*) state;
    return hashMap->insertEntry(hash);
}

Value<> HashMap::compareKeys(std::vector<Value<>> keyValues, Value<MemRef> ref) const {
    Value<Boolean> equals = true;
    for (auto& keyValue : keyValues) {
        equals = equals && keyValue == ref.load<Int64>();
        ref = ref + (uint64_t) 8;
    }
    return equals;
}

HashMap::Entry HashMap::getEntryFromHashTable(Value<UInt64> hash) const {
    auto entry = FunctionCall<>("getHashEntry", getHashEntry, hashTableRef, hash);
    return Entry(entry, NES::Experimental::Hashmap::headerSize, NES::Experimental::Hashmap::headerSize + valueOffset);
}

extern "C" uint64_t calculateHashProxy(int64_t value, uint64_t hash) {
    const NES::Experimental::Hash<NES::Experimental::MurMurHash3> hasher =
        NES::Experimental::Hash<NES::Experimental::MurMurHash3>();
    return hasher(value, hash);
}

Value<UInt64> HashMap::calculateHash(std::vector<Value<>> keys) {
    Value<UInt64> hash = NES::Experimental::MurMurHash3::SEED;
    for (auto& keyValue : keys) {
        hash = FunctionCall<>("calculateHashProxy", calculateHashProxy, keyValue.as<Int64>(), hash);
    }
    return hash;
}

HashMap::Entry HashMap::createEntry(std::vector<Value<>> keys, Value<UInt64> hash) {
    auto entryRef = FunctionCall<>("createEntryProxy", createEntryProxy, hashTableRef, hash);
    auto entry = Entry(entryRef, NES::Experimental::Hashmap::headerSize, NES::Experimental::Hashmap::headerSize + valueOffset);
    auto keyPtr = entry.getKeyPtr();
    for (auto i = 0ull; i < keys.size(); i++) {
        keyPtr.store(keys[i]);
        keyPtr = keyPtr + (uint64_t) 8;
    }
    return entry;
}

Value<> HashMap::isValid(std::vector<Value<>>& keyValue, Entry& entry) const {
    Value<Boolean> valid = false;
    if (!entry.isNull()) {
        valid = compareKeys(keyValue, entry.getKeyPtr());
    }
    return valid;
}

HashMap::Entry HashMap::findOrCreate(std::vector<Value<>> keys) {
    // calculate hash
    auto hash = calculateHash(keys);

    // return entry if it exists
    auto entry = getEntryFromHashTable(hash);
    //for (; !isValid(keys, entry); entry = entry.getNext()) {
    //}
    if (entry.isNull()) {
        // creates a new entry and place it to the right spot.
        entry = createEntry(keys, hash);
    }

    return entry;
}

HashMap::Entry HashMap::findOne(std::vector<Value<>> keys) {
    // calculate hash
    auto hash = calculateHash(keys);

    // return entry if it exists
    auto entry = getEntryFromHashTable(hash);
    for (; !entry.isNull(); entry = entry.getNext()) {
        if (compareKeys(keys, entry.getKeyPtr())) {
            return entry;
        }
    }
    return entry;
}

HashMap::HashMap(Value<MemRef> hashTableRef,
                 int64_t valueOffset,
                 std::vector<IR::Types::StampPtr> keyTypes,
                 std::vector<IR::Types::StampPtr> valueTypes)
    : hashTableRef(hashTableRef), valueOffset(valueOffset), keyTypes(keyTypes), valueTypes(valueTypes) {}

}// namespace NES::Nautilus