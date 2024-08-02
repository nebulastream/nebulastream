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
#include <Nautilus/DataTypes/ExecutableDataType.hpp>
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

ChainedHashMapRef::EntryRef::EntryRef(const MemRef& ref, uint64_t keyOffset, uint64_t valueOffset)
    : ref(ref), keyOffset(keyOffset), valueOffset(valueOffset) {}

MemRef ChainedHashMapRef::EntryRef::getKeyPtr() const { return (ref + nautilus::val<uint64_t>(keyOffset)); }

MemRef ChainedHashMapRef::EntryRef::getValuePtr() const { return (ref + nautilus::val<uint64_t>(valueOffset)); }

ChainedHashMapRef::EntryRef ChainedHashMapRef::EntryRef::getNext() const {
    // This assumes that the next ptr is stored as the first element in the entry.
    // perform a load to load the next value
    auto next = getMemberAsPointer(ref, ChainedHashMap::Entry, next, int8_t);
    return {next, keyOffset, valueOffset};
}

UInt64 ChainedHashMapRef::EntryRef::getHash() const {
    // load the has value from the entry
    return getMember(ref, ChainedHashMap::Entry, hash, uint64_t);
}

nautilus::val<bool> ChainedHashMapRef::EntryRef::operator!=(std::nullptr_t) const { return ref != nullptr; }
nautilus::val<bool> ChainedHashMapRef::EntryRef::operator==(std::nullptr_t) const { return ref == nullptr; }

ChainedHashMapRef::ChainedHashMapRef(const MemRef& hashTableRef,
                                     const std::vector<PhysicalTypePtr>& keyDataTypes,
                                     uint64_t keySize,
                                     uint64_t valueSize)
    : hashTableRef(hashTableRef), keyDataTypes(keyDataTypes), keySize(keySize), valueSize(valueSize) {}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findChain(const UInt64& hash) {
    auto entry = invoke(findChainProxy, hashTableRef, hash);
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::insert(const UInt64& hash) {
    auto entry = invoke(insetProxy, hashTableRef, hash);
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + keySize};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::insert(const UInt64& hash, const std::vector<ExecDataType>& keys) {
    // create new entry
    auto entry = insert(hash);
    // store keys
    auto keyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keys.size(); i++) {
        auto& key = keys[i];
        writeExecDataTypeToMemRef(keyPtr, key);
        keyPtr = keyPtr + nautilus::val<uint64_t>(keyDataTypes[i]->size());
    }
    return entry;
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::find(const UInt64& hash, const std::vector<ExecDataType>& keys) {
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

ChainedHashMapRef::KeyEntryIterator ChainedHashMapRef::findAll(const UInt64& hash, const std::vector<ExecDataType>& keys) {
    return {*this, hash, keys, 0_u64};
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const UInt64& hash, const std::vector<ExecDataType>& keys) {
    return findOrCreate(hash, keys, [](const EntryRef&) {
        // nop
    });
}

ChainedHashMapRef::EntryRef ChainedHashMapRef::findOrCreate(const UInt64& hash,
                                                            const std::vector<ExecDataType>& keys,
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
        if (memEquals(entry.getKeyPtr(), otherEntry.getKeyPtr(), UInt64(keySize))) {
            update(entry);
            break;
        }
    }
    if (entry == nullptr) {
        auto newEntry = insert(otherEntry.getHash());
        memCopy(newEntry.getKeyPtr(), otherEntry.getKeyPtr(), UInt64(keySize + valueSize));
    }
}

Boolean ChainedHashMapRef::compareKeys(EntryRef& entry, const std::vector<ExecDataType>& keys) {
    auto equals = ExecutableDataType<bool>::create(true);
    auto keyPtr = entry.getKeyPtr();
    for (size_t i = 0; i < keys.size(); i++) {
        auto& key = keys[i];
        const auto keyFromEntry = readExecDataTypeFromMemRef(keyPtr, keyDataTypes[i]);
        const auto tmp = (key == keyFromEntry);
        equals = tmp && equals;
        keyPtr = keyPtr + nautilus::val<uint64_t>(keyDataTypes[i]->size());
    }
    if (equals) {
        return {true};
    } else {
        return {false};
    }
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::begin() {
    auto entriesPerPage = getEntriesPerPage();
    return {*this, entriesPerPage, 0_u64};
}
ChainedHashMapRef::EntryIterator ChainedHashMapRef::end() {
    auto currentSize = getCurrentSize();
    return {*this, currentSize};
}

UInt64 ChainedHashMapRef::getCurrentSize() {
    return getMember(this->hashTableRef, ChainedHashMap, currentSize, uint64_t);
}

UInt64 ChainedHashMapRef::getPageSize() { return getMember(this->hashTableRef, ChainedHashMap, pageSize, uint64_t); }

void* getPageProxy(void* hmPtr, uint64_t pageIndex) {
    auto hashMap = (ChainedHashMap*) hmPtr;
    return hashMap->getPage(pageIndex);
}

MemRef ChainedHashMapRef::getPage(const UInt64& pageIndex) {
    return nautilus::invoke(getPageProxy, hashTableRef, pageIndex);
}

ChainedHashMapRef::EntryIterator::EntryIterator(ChainedHashMapRef& hashTableRef,
                                                const UInt64& entriesPerPage,
                                                const UInt64& currentIndex)
    : hashTableRef(hashTableRef), entriesPerPage(entriesPerPage), inPageIndex(0_u64), currentPage(hashTableRef.getPage(0_u64)),
      currentPageIndex(0_u64), currentIndex(currentIndex) {}

ChainedHashMapRef::EntryIterator::EntryIterator(ChainedHashMapRef& hashTableRef, const UInt64& currentIndex)
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
    return {entry, sizeof(ChainedHashMap::Entry), sizeof(ChainedHashMap::Entry) + hashTableRef.keySize};
}

UInt64 ChainedHashMapRef::getEntriesPerPage() {
    return getMember(this->hashTableRef, ChainedHashMap, entriesPerPage, uint64_t);
}

ChainedHashMapRef::KeyEntryIterator::KeyEntryIterator(ChainedHashMapRef& hashTableRef,
                                                      const UInt64& hash,
                                                      const std::vector<ExecDataType>& keys,
                                                      const UInt64& currentIndex)
    : hashTableRef(hashTableRef), currentIndex(currentIndex), keys(keys), currentEntry(hashTableRef.findChain(hash)) {}

ChainedHashMapRef::KeyEntryIterator& ChainedHashMapRef::KeyEntryIterator::operator++() {
    Boolean equals = false;
    for (; currentEntry != nullptr && !equals; currentEntry = currentEntry.getNext()) {
        currentIndex = currentIndex + 1_u64;
        equals = hashTableRef.compareKeys(currentEntry, keys);
    }
    return *this;
}

bool ChainedHashMapRef::KeyEntryIterator::operator==(KeyEntryIterator other) const {
    return currentEntry == nullptr && other.currentEntry == nullptr;
}

bool ChainedHashMapRef::KeyEntryIterator::operator==(std::nullptr_t) const { return currentEntry == nullptr; }

ChainedHashMapRef::EntryRef ChainedHashMapRef::KeyEntryIterator::operator*() const { return currentEntry; }
}// namespace NES::Nautilus::Interface
