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
#include <Nautilus/Interface/DataTypes/Utils.hpp>
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

extern "C" bool insertEntryIfNewProxy(void* state, void* e) {
    auto hashMap = (ChainedHashMap*) state;
    auto entry = (ChainedHashMap::Entry*) e;
    return hashMap->insertEntryIfNotExists(entry);
}

ChainedHashMapRef::EntryRef::EntryRef(const Value<MemRef>& ref, uint64_t keyOffset, uint64_t valueOffset)
    : ref(ref), keyOffset(keyOffset), valueOffset(valueOffset) {}

Value<MemRef> ChainedHashMapRef::EntryRef::getKeyPtr() const { return (ref + keyOffset).as<MemRef>(); }

Value<MemRef> ChainedHashMapRef::EntryRef::getValuePtr() const { return (ref + valueOffset).as<MemRef>(); }

ChainedHashMapRef::EntryRef ChainedHashMapRef::EntryRef::getNext() const {
    // This assumes that the next ptr is stored as the first element in the entry.
    static_assert(offsetof(ChainedHashMap::Entry, next) == 0);
    // perform a load to load the next value
    auto next = ref.load<MemRef>();
    return {next, keyOffset, valueOffset};
}

Value<UInt64> ChainedHashMapRef::EntryRef::getHash() const {
    // This assumes that the hash ptr is stored as the first element in the entry.
    return (ref + offsetof(ChainedHashMap::Entry, hash)).as<MemRef>().load<UInt64>();
}

bool ChainedHashMapRef::EntryRef::operator!=(std::nullptr_t) { return ref != 0; }
bool ChainedHashMapRef::EntryRef::operator==(std::nullptr_t) { return ref == 0; }

ChainedHashMapRef::ChainedHashMapRef(const Value<MemRef>& hashTableRef, uint64_t keySize, uint64_t valueSize)
    : hashTableRef(hashTableRef), keySize(keySize), valueSize(valueSize) {}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findChain(const Value<UInt64>& hash) {
    auto entry = FunctionCall<>("findChainProxy", findChainProxy, hashTableRef, hash);
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::insert(const Value<UInt64>& hash) {
    auto entry = FunctionCall<>("insertProxy", insetProxy, hashTableRef, hash);
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOne(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    auto entry = findChain(hash);
    for (; entry != nullptr; entry = entry.getNext()) {
        if (compareKeys(entry, keys)) {
            break;
        }
    }
    return entry;
}

void ChainedHashMapRef::insertEntryOrUpdate(const EntryRef& otherEntry, const std::function<void(EntryRef&)>& update) {
    auto entry = findChain(otherEntry.getHash());
    for (; entry != nullptr; entry = entry.getNext()) {
        if (memEquals(entry.getKeyPtr(), otherEntry.getKeyPtr(), Value<UInt64>(keySize))) {
            update(entry);
            break;
        }
    }
    if (entry == nullptr) {
        auto newEntry = insert(otherEntry.getHash());
        memCopy(newEntry.getKeyPtr(), otherEntry.getKeyPtr(), Value<UInt64>(keySize + valueSize));
    }
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const Value<UInt64>& hash,
                                                            const std::vector<Value<>>& keys,
                                                            const std::function<void(EntryRef&)>& onInsert) {
    auto entry = findChain(hash);
    for (; entry != nullptr; entry = entry.getNext()) {
        if (compareKeys(entry, keys)) {
            break;
        }
    }
    if (entry == nullptr) {
        // create new entry
        entry = insert(hash);
        auto keyPtr = entry.getKeyPtr();
        for (auto& key : keys) {
            keyPtr.store(key);
            // todo fix types
            keyPtr = keyPtr + key->getType().getSize();
        }
        // call on insert lambda function to insert default values
        onInsert(entry);
    }
    return entry;
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
        equals = equals && (keyValue == ref.load<Int64>());
        ref = ref + (uint64_t) 8;
    }
    return equals;
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::begin() {
    auto entriesPerPage = getEntriesPerPage();
    return ChainedHashMapRef::EntryIterator(*this, entriesPerPage, (uint64_t) 0);
}
ChainedHashMapRef::EntryIterator ChainedHashMapRef::end() {
    auto currentSize = getCurrentSize();
    auto entriesPerPage = getEntriesPerPage();
    return ChainedHashMapRef::EntryIterator(*this, currentSize);
}

Value<UInt64> ChainedHashMapRef::getCurrentSize() {
    return getMember(this->hashTableRef, ChainedHashMap, currentSize).load<UInt64>();
}

Value<UInt64> ChainedHashMapRef::getPageSize() { return getMember(this->hashTableRef, ChainedHashMap, pageSize).load<UInt64>(); }

void* getPageProxy(void* hmPtr, uint64_t pageIndex) {
    auto hashMap = (ChainedHashMap*) hmPtr;
    return hashMap->getPage(pageIndex);
}

Value<MemRef> ChainedHashMapRef::getPage(Value<UInt64> pageIndex) {
    return FunctionCall("getPageProxy", getPageProxy, hashTableRef, pageIndex);
}

ChainedHashMapRef::EntryIterator::EntryIterator(ChainedHashMapRef& hashTableRef,
                                                const Value<UInt64>& entriesPerPage,
                                                const Value<UInt64>& currentIndex)
    : hashTableRef(hashTableRef), entriesPerPage(entriesPerPage), inPageIndex((uint64_t) 0),
      currentPage(hashTableRef.getPage((uint64_t) 0)), currentPageIndex((uint64_t) 0), currentIndex(currentIndex) {}

ChainedHashMapRef::EntryIterator::EntryIterator(ChainedHashMapRef& hashTableRef, const Value<UInt64>& currentIndex)
    : hashTableRef(hashTableRef), entriesPerPage((uint64_t) 0), inPageIndex((uint64_t) 0),
      currentPage(/*use hash table ref as a temp mem ref that is in this case never used*/ hashTableRef.hashTableRef),
      currentPageIndex((uint64_t) 0), currentIndex(currentIndex) {}

ChainedHashMapRef::EntryIterator& ChainedHashMapRef::EntryIterator::operator++() {
    inPageIndex = inPageIndex + (uint64_t) 1;
    if (entriesPerPage == inPageIndex) {
        inPageIndex = (uint64_t) 0;
        currentPageIndex = currentPageIndex + (uint64_t) 1;
        currentPage = hashTableRef.getPage(currentPageIndex);
    }
    currentIndex = currentIndex + (uint64_t) 1;
    return *this;
}

bool ChainedHashMapRef::EntryIterator::operator==(const EntryIterator& other) const {
    return this->currentIndex == other.currentIndex;
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::EntryIterator::operator*() {
    auto entry = currentPage + ((sizeof(ChainedHashMap::Entry) + hashTableRef.keySize + hashTableRef.valueSize) * inPageIndex);
    return {entry.as<MemRef>(), sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + hashTableRef.keySize};
}

Value<UInt64> ChainedHashMapRef::getEntriesPerPage() {
    return getMember(this->hashTableRef, ChainedHashMap, entriesPerPage).load<UInt64>();
}

}// namespace NES::Nautilus::Interface