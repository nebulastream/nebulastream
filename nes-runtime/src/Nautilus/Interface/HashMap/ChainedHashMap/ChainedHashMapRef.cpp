#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>

namespace NES::Nautilus::Interface {

extern "C" void* findChainProxy(void* state, uint64_t hash) {
    auto hashMap = (ChainedHashMap*) state;
    return hashMap->findChain(hash);
}

extern "C" void* insetProxy(void* state, uint64_t hash) {
    auto hashMap = (ChainedHashMap*) state;
    return hashMap->insertEntry(hash);
}

ChainedHashMapRef::EntryRef::EntryRef(const Value<MemRef>& ref, uint64_t keyOffset, uint64_t valueOffset)
    : ref(ref), keyOffset(keyOffset), valueOffset(valueOffset) {}

Value<MemRef> ChainedHashMapRef::EntryRef::getKeyPtr() { return (ref + keyOffset).as<MemRef>(); }

Value<MemRef> ChainedHashMapRef::EntryRef::getValuePtr() { return (ref + valueOffset).as<MemRef>(); }

ChainedHashMapRef::EntryRef ChainedHashMapRef::EntryRef::getNext() {
    // This assumes that the next ptr is stored as the first element in the entry.
    static_assert(offsetof(ChainedHashMap::Entry, next) == 0);
    // perform a load to load the next value
    auto next = ref.load<MemRef>();
    return {next, keyOffset, valueOffset};
}

bool ChainedHashMapRef::EntryRef::operator!=(std::nullptr_t) {
    return ref != 0;
}

ChainedHashMapRef::ChainedHashMapRef(const Value<MemRef>& hashTableRef, uint64_t keySize)
    : hashTableRef(hashTableRef), keyOffset(sizeof(ChainedHashMap::Entry)), valueOffset(keyOffset + keySize) {}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findChain(const Value<UInt64>& hash) {
    auto entry = FunctionCall<>("findChainProxy", findChainProxy, hashTableRef, hash);
    return {entry, keyOffset, valueOffset};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::insert(const Value<UInt64>& hash) {
    auto entry = FunctionCall<>("insertProxy", insetProxy, hashTableRef, hash);
    return {entry, keyOffset, valueOffset};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOne(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    auto entry = findChain(hash);
    for (; entry != nullptr; entry = entry.getNext()) {
        if (compareKeys(entry, keys)) {
            return entry;
        }
    }
    return entry;
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const Value<UInt64>& hash,
                                                            const std::vector<Value<>>& keys,
                                                            const std::function<void(EntryRef&)>& onInsert) {
    auto entry = findChain(hash);
    for (; entry != nullptr; entry = entry.getNext()) {
        if (compareKeys(entry, keys)) {
            return entry;
        }
    }
    // create new entry
    auto newEntry = insert(hash);
    auto keyPtr = newEntry.getKeyPtr();
    for (auto& key : keys) {
        keyPtr.store(key);
        // todo fix types
        keyPtr = keyPtr + (uint64_t) 8;
    }
    // call on insert lambda function to insert default values
    onInsert(newEntry);
    return newEntry;
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    return findOrCreate(hash, keys, [](const EntryRef&) {
        // nop
    });
}

Value<Boolean> ChainedHashMapRef::compareKeys(EntryRef& entry, const std::vector<Value<>>& keys) {
    Value<Boolean> equals = true;
    auto ref = entry.getKeyPtr();
    for (auto& keyValue : keys) {
        // todo fix types
        equals = equals && keyValue == ref.load<Int64>();
        ref = ref + (uint64_t) 8;
    }
    return equals;
}

}// namespace NES::Nautilus::Interface