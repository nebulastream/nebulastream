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
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Util/StdInt.hpp>

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

Value<MemRef> ChainedHashMapRef::EntryRef::getKeyPtr() const { return (ref + keyOffset).as<MemRef>(); }

Value<MemRef> ChainedHashMapRef::EntryRef::getValuePtr() const { return (ref + valueOffset).as<MemRef>(); }

ChainedHashMapRef::EntryRef ChainedHashMapRef::EntryRef::getNext() const {
    // This assumes that the next ptr is stored as the first element in the entry.
    // perform a load to load the next value
    auto next = getMember(ref, ChainedHashMap::Entry, next).load<MemRef>();
    return {next, keyOffset, valueOffset};
}

Value<UInt64> ChainedHashMapRef::EntryRef::getHash() const {
    // load the has value from the entry
    return getMember(ref, ChainedHashMap::Entry, hash).load<UInt64>();
}

bool ChainedHashMapRef::EntryRef::operator!=(std::nullptr_t) const { return ref != 0; }
bool ChainedHashMapRef::EntryRef::operator==(std::nullptr_t) const { return ref == 0; }

ChainedHashMapRef::ChainedHashMapRef(const Value<MemRef>& hashTableRef,
                                     const std::vector<PhysicalTypePtr>& keyDataTypes,
                                     uint64_t keySize,
                                     uint64_t valueSize)
    : hashTableRef(hashTableRef), keyDataTypes(keyDataTypes), keySize(keySize), valueSize(valueSize) {}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findChain(const Value<UInt64>& hash) {
    auto entry = FunctionCall<>("findChainProxy", findChainProxy, hashTableRef, hash);
    auto i = ChainedHashMapRef::EntryRef(entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize);
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::insert(const Value<UInt64>& hash) {
    auto entry = FunctionCall<>("insertProxy", insetProxy, hashTableRef, hash);
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::insert(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    // create new entry
    auto entry = insert(hash);
    // store keys
    auto keyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keys.size(); i++) {
        auto& key = keys[i];
        keyPtr.store(key);
        keyPtr = keyPtr + keyDataTypes[i]->size();
    }
    return entry;
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::find(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    // find chain
    auto entry = findChain(hash);
    // iterate chain and search for the correct entry
    for (; entry != nullptr; entry = entry.getNext()) {
        if (compareKeys(entry, keys)) {
            break;
        }
    }
    return entry;
}

ChainedHashMapRef::KeyEntryIterator ChainedHashMapRef::findAll(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    return {*this, hash, keys};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const Value<UInt64>& hash, const std::vector<Value<>>& keys) {
    return findOrCreate(hash, keys, [](const EntryRef&) {
        // nop
    });
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const Value<UInt64>& hash,
                                                            const std::vector<Value<>>& keys,
                                                            const std::function<void(EntryRef&)>& onInsert) {
    // find entry
    auto entry = find(hash, keys);
    // if the entry is null, insert a new entry.
    if (entry == nullptr) {
        // create new entry
        entry = insert(hash, keys);
        // call on insert lambda function to insert default values
        onInsert(entry);
    }
    return entry;
}

void ChainedHashMapRef::insertEntryOrUpdate(const EntryRef& otherEntry, const std::function<void(EntryRef&)>& update) {
    auto entry = findChain(otherEntry.getHash());
    for (; entry != nullptr; entry = entry.getNext()) {
        if (MemRefUtils::memEquals(entry.getKeyPtr(), otherEntry.getKeyPtr(), Value<UInt64>(keySize))) {
            update(entry);
            break;
        }
    }
    if (entry == nullptr) {
        auto newEntry = insert(otherEntry.getHash());
        MemRefUtils::memCopy(newEntry.getKeyPtr(), otherEntry.getKeyPtr(), Value<UInt64>(keySize + valueSize));
    }
}

Value<Boolean> ChainedHashMapRef::compareKeys(EntryRef& entry, const std::vector<Value<>>& keys) {
    Value<Boolean> equals = true;
    auto keyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keys.size(); i++) {
        auto& key = keys[i];
        auto keyFromEntry = MemRefUtils::loadValue(keyPtr, keyDataTypes[i]);
        equals = equals && (key == keyFromEntry);
        keyPtr = keyPtr + keyDataTypes[i]->size();
    }
    return equals;
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::begin() {
    auto entriesPerPage = getEntriesPerPage();
    return {*this, entriesPerPage, 0_u64};
}
ChainedHashMapRef::EntryIterator ChainedHashMapRef::end() {
    auto currentSize = getCurrentSize();
    return {*this, currentSize};
}

Value<UInt64> ChainedHashMapRef::getCurrentSize() {
    return getMember(this->hashTableRef, ChainedHashMap, currentSize).load<UInt64>();
}

Value<UInt64> ChainedHashMapRef::getPageSize() { return getMember(this->hashTableRef, ChainedHashMap, pageSize).load<UInt64>(); }

void* getPageProxy(void* hmPtr, uint64_t pageIndex) {
    auto hashMap = (ChainedHashMap*) hmPtr;
    return hashMap->getPage(pageIndex);
}

Value<MemRef> ChainedHashMapRef::getPage(const Value<UInt64>& pageIndex) {
    return FunctionCall("getPageProxy", getPageProxy, hashTableRef, pageIndex);
}

ChainedHashMapRef::EntryIterator::EntryIterator(ChainedHashMapRef& hashTableRef,
                                                const Value<UInt64>& entriesPerPage,
                                                const Value<UInt64>& currentIndex)
    : hashTableRef(hashTableRef), entriesPerPage(entriesPerPage), inPageIndex(0_u64), currentPage(hashTableRef.getPage(0_u64)),
      currentPageIndex(0_u64), currentIndex(currentIndex) {}

ChainedHashMapRef::EntryIterator::EntryIterator(ChainedHashMapRef& hashTableRef, const Value<UInt64>& currentIndex)
    : hashTableRef(hashTableRef), entriesPerPage(0_u64), inPageIndex(0_u64),
      currentPage(/*use hash table ref as a temp mem ref that is in this case never used*/ hashTableRef.hashTableRef),
      currentPageIndex(0_u64), currentIndex(currentIndex) {}

ChainedHashMapRef::EntryIterator& ChainedHashMapRef::EntryIterator::operator++() {
    inPageIndex = inPageIndex + 1_u64;
    if (entriesPerPage == inPageIndex) {
        inPageIndex = 0_u64;
        currentPageIndex = currentPageIndex + 1_u64;
        currentPage = hashTableRef.getPage(currentPageIndex);
    }
    currentIndex = currentIndex + 1_u64;
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

ChainedHashMapRef::KeyEntryIterator::KeyEntryIterator(ChainedHashMapRef& hashTableRef,
                                                      const Value<UInt64>& hash,
                                                      const std::vector<Value<>>& keys)
    : hashTableRef(hashTableRef), keys(keys), currentEntry(hashTableRef.find(hash, keys)) {}

ChainedHashMapRef::KeyEntryIterator& ChainedHashMapRef::KeyEntryIterator::operator++() {
    Value<Boolean> equals = false;
    // Iterate while there's a valid entry and keys are not equal.
    while (currentEntry != nullptr && !equals) {
        currentEntry = currentEntry.getNext();
        if (currentEntry != nullptr) {
            equals = hashTableRef.compareKeys(currentEntry, keys);
        }
    }
    return *this;
}

bool ChainedHashMapRef::KeyEntryIterator::operator==(KeyEntryIterator other) const {
    return currentEntry == nullptr && other.currentEntry == nullptr;
}

bool ChainedHashMapRef::KeyEntryIterator::operator==(std::nullptr_t) const { return currentEntry == nullptr; }

ChainedHashMapRef::EntryRef ChainedHashMapRef::KeyEntryIterator::operator*() const { return currentEntry; }
}// namespace NES::Nautilus::Interface