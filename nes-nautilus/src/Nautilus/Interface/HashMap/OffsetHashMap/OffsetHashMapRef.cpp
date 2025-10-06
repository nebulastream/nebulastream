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
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetHashMapRef.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetEntryMemoryProvider.hpp>

namespace NES::Nautilus::Interface {

OffsetHashMapRef::OffsetEntryRef::OffsetEntryRef(
    const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    std::vector<MemoryProvider::OffsetFieldOffsets> fieldsKey,
    std::vector<MemoryProvider::OffsetFieldOffsets> fieldsValue)
    : entryRef(entryRef)
    , hashMapRef(hashMapRef)
    , memoryProviderKeys(std::move(fieldsKey))
    , memoryProviderValues(std::move(fieldsValue))
{
}

OffsetHashMapRef::OffsetEntryRef::OffsetEntryRef(
    const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    MemoryProvider::OffsetEntryMemoryProvider memoryProviderKeys,
    MemoryProvider::OffsetEntryMemoryProvider memoryProviderValues)
    : entryRef(entryRef)
    , hashMapRef(hashMapRef)
    , memoryProviderKeys(std::move(memoryProviderKeys))
    , memoryProviderValues(std::move(memoryProviderValues))
{
}

OffsetHashMapRef::OffsetEntryRef::OffsetEntryRef(const OffsetEntryRef& other) = default;
OffsetHashMapRef::OffsetEntryRef& OffsetHashMapRef::OffsetEntryRef::operator=(const OffsetEntryRef& other) = default;
OffsetHashMapRef::OffsetEntryRef::OffsetEntryRef(OffsetEntryRef&& other) noexcept = default;

Record OffsetHashMapRef::OffsetEntryRef::getKey() const {
    return memoryProviderKeys.readRecord(entryRef);
}

Record OffsetHashMapRef::OffsetEntryRef::getValue() const {
    return memoryProviderValues.readRecord(entryRef);
}

nautilus::val<int8_t*> OffsetHashMapRef::OffsetEntryRef::getValueMemArea() const {
    constexpr uint64_t headerSize = DataStructures::OffsetEntry::headerSize();
    uint64_t keySize = 0;
    for (const auto& field : memoryProviderKeys.getAllFields()) {
        keySize += field.type.getSizeInBytes();
    }
    
    auto basePtr = static_cast<nautilus::val<int8_t*>>(static_cast<nautilus::val<void*>>(entryRef));
    return basePtr + static_cast<int64_t>(headerSize + keySize);
}

void OffsetHashMapRef::OffsetEntryRef::copyKeysToEntry(
    const Record& keys,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const {
    memoryProviderKeys.writeRecord(entryRef, hashMapRef, bufferProvider, keys);
}

void OffsetHashMapRef::OffsetEntryRef::copyValuesToEntry(
    const Record& values,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const {
    memoryProviderValues.writeRecord(entryRef, hashMapRef, bufferProvider, values);
}

void OffsetHashMapRef::OffsetEntryRef::copyKeysToEntry(
    const OffsetEntryRef& otherEntryRef,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const {
    memoryProviderKeys.writeEntryRef(entryRef, hashMapRef, bufferProvider, otherEntryRef.entryRef);
}

void OffsetHashMapRef::OffsetEntryRef::copyValuesToEntry(
    const OffsetEntryRef& otherEntryRef,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) const {
    memoryProviderValues.writeEntryRef(entryRef, hashMapRef, bufferProvider, otherEntryRef.entryRef);
}

VarVal OffsetHashMapRef::OffsetEntryRef::getKey(const Record::RecordFieldIdentifier& fieldIdentifier) const {
    return memoryProviderKeys.readVarVal(entryRef, fieldIdentifier);
}

nautilus::val<DataStructures::OffsetEntry*> OffsetHashMapRef::OffsetEntryRef::getNext() const {
    std::function<uint32_t(DataStructures::OffsetEntry*)> getNextOffset = [](DataStructures::OffsetEntry* e) -> uint32_t {
        return e ? e->nextOffset : 0;
    };
    auto nextOffset = nautilus::invoke(getNextOffset, entryRef);
    
    if (nextOffset != nautilus::val<uint32_t>(0)) {
        std::function<DataStructures::OffsetEntry*(DataStructures::OffsetHashMapWrapper*, uint32_t)> getEntry = 
            [](DataStructures::OffsetHashMapWrapper* w, uint32_t offset) -> DataStructures::OffsetEntry* {
                return w ? w->getOffsetEntry(offset) : nullptr;
            };
        return nautilus::invoke(getEntry, hashMapRef, nextOffset);
    }
    return nautilus::val<DataStructures::OffsetEntry*>(nullptr);
}

HashFunction::HashValue OffsetHashMapRef::OffsetEntryRef::getHash() const {
    std::function<uint64_t(DataStructures::OffsetEntry*)> getHashValue = [](DataStructures::OffsetEntry* e) -> uint64_t {
        return e ? e->hash : 0;
    };
    auto hash = nautilus::invoke(getHashValue, entryRef);
    return HashFunction::HashValue(hash);
}

void OffsetHashMapRef::OffsetEntryRef::updateEntryRef(const nautilus::val<DataStructures::OffsetEntry*>& newEntryRef) {
    entryRef = newEntryRef;
}

// EntryIterator Implementation
OffsetHashMapRef::EntryIterator::EntryIterator(
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    const nautilus::val<uint64_t>& bucketIndex,
    const std::vector<MemoryProvider::OffsetFieldOffsets>& fieldKeys,
    const std::vector<MemoryProvider::OffsetFieldOffsets>& fieldValues)
    : hashMapRef(hashMapRef)
    , currentEntry(nautilus::val<DataStructures::OffsetEntry*>(nullptr), hashMapRef, fieldKeys, fieldValues)
    , bucketIndex(bucketIndex)
    , tupleIndex(0)
    , numberOfBuckets(0)
{
}

OffsetHashMapRef::EntryIterator& OffsetHashMapRef::EntryIterator::operator++() {
    // Check if we have a current entry with a next pointer
    if (currentEntry.entryRef != nautilus::val<DataStructures::OffsetEntry*>(nullptr)) {
        auto next = currentEntry.getNext();
        if (next != nautilus::val<DataStructures::OffsetEntry*>(nullptr)) {
            currentEntry.entryRef = next;
            return *this;
        }
    }
    
    // Move to next bucket
    bucketIndex = bucketIndex + nautilus::val<uint64_t>(1);
    
    // Find next non-empty bucket
    nautilus::val<bool> found(false);
    while (bucketIndex < numberOfBuckets && !found) {
        std::function<DataStructures::OffsetEntry*(DataStructures::OffsetHashMapWrapper*, uint64_t)> getFirstEntry = 
            [](DataStructures::OffsetHashMapWrapper* w, uint64_t idx) -> DataStructures::OffsetEntry* {
                if (!w) return nullptr;
                auto& impl = w->getImpl();
                auto& state = impl.extractState();
                if (idx < state.chains.size() && state.chains[idx] != 0) {
                    return w->getOffsetEntry(state.chains[idx]);
                }
                return nullptr;
            };
        currentEntry.entryRef = nautilus::invoke(getFirstEntry, hashMapRef, bucketIndex);
        
        if (currentEntry.entryRef != nautilus::val<DataStructures::OffsetEntry*>(nullptr)) {
            found = nautilus::val<bool>(true);
        } else {
            bucketIndex = bucketIndex + nautilus::val<uint64_t>(1);
        }
    }
    
    if (!found) {
        currentEntry.entryRef = nautilus::val<DataStructures::OffsetEntry*>(nullptr);
    }
    
    return *this;
}

nautilus::val<bool> OffsetHashMapRef::EntryIterator::operator==(const EntryIterator& other) const {
    return (bucketIndex == other.bucketIndex) && (currentEntry.entryRef == other.currentEntry.entryRef);
}

nautilus::val<bool> OffsetHashMapRef::EntryIterator::operator!=(const EntryIterator& other) const {
    return !(*this == other);
}

nautilus::val<DataStructures::OffsetEntry*> OffsetHashMapRef::EntryIterator::operator*() const {
    return currentEntry.entryRef;
}

// OffsetHashMapRef Implementation
OffsetHashMapRef::OffsetHashMapRef(
    const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
    std::vector<MemoryProvider::OffsetFieldOffsets> fieldKeys,
    std::vector<MemoryProvider::OffsetFieldOffsets> fieldValues)
    : HashMapRef(static_cast<nautilus::val<HashMap*>>(static_cast<nautilus::val<void*>>(hashMapRef)))
    , hashMapRef(hashMapRef)
    , fieldKeys(std::move(fieldKeys))
    , fieldValues(std::move(fieldValues))
{
}

OffsetHashMapRef::OffsetHashMapRef(const OffsetHashMapRef& other) = default;
OffsetHashMapRef& OffsetHashMapRef::operator=(const OffsetHashMapRef& other) = default;

OffsetHashMapRef::EntryIterator OffsetHashMapRef::begin() const {
    // Get bucket count from wrapper
    std::function<uint64_t(DataStructures::OffsetHashMapWrapper*)> getBucketCount = 
        [](DataStructures::OffsetHashMapWrapper* w) -> uint64_t {
            if (!w) return 0;
            auto& impl = w->getImpl();
            auto& state = impl.extractState();
            return state.chains.size();
        };
    auto bucketCount = nautilus::invoke(getBucketCount, hashMapRef);
    
    // Find first non-empty bucket
    EntryIterator it(hashMapRef, nautilus::val<uint64_t>(0), fieldKeys, fieldValues);
    it.numberOfBuckets = bucketCount;
    
    // Search for first entry
    std::function<DataStructures::OffsetEntry*(DataStructures::OffsetHashMapWrapper*, uint64_t)> findFirstEntry = 
        [](DataStructures::OffsetHashMapWrapper* w, uint64_t maxBuckets) -> DataStructures::OffsetEntry* {
            if (!w) return nullptr;
            auto& impl = w->getImpl();
            auto& state = impl.extractState();
            for (size_t i = 0; i < state.chains.size() && i < maxBuckets; ++i) {
                if (state.chains[i] != 0) {
                    return w->getOffsetEntry(state.chains[i]);
                }
            }
            return nullptr;
        };
    it.currentEntry.entryRef = nautilus::invoke(findFirstEntry, hashMapRef, bucketCount);
    
    // If we found an entry, set the bucket index appropriately
    if (it.currentEntry.entryRef != nautilus::val<DataStructures::OffsetEntry*>(nullptr)) {
        std::function<uint64_t(DataStructures::OffsetHashMapWrapper*, DataStructures::OffsetEntry*)> findBucketIndex = 
            [](DataStructures::OffsetHashMapWrapper* w, DataStructures::OffsetEntry* entry) -> uint64_t {
                if (!w || !entry) return 0;
                auto& impl = w->getImpl();
                auto& state = impl.extractState();
                auto entryOffset = w->getOffsetFromEntry(entry);
                for (size_t i = 0; i < state.chains.size(); ++i) {
                    if (state.chains[i] == entryOffset) {
                        return i;
                    }
                }
                return 0;
            };
        it.bucketIndex = nautilus::invoke(findBucketIndex, hashMapRef, it.currentEntry.entryRef);
    } else {
        it.bucketIndex = bucketCount; // End position
    }
    
    return it;
}

OffsetHashMapRef::EntryIterator OffsetHashMapRef::end() const {
    // Get bucket count and return end iterator
    std::function<uint64_t(DataStructures::OffsetHashMapWrapper*)> getBucketCount = 
        [](DataStructures::OffsetHashMapWrapper* w) -> uint64_t {
            if (!w) return 0;
            auto& impl = w->getImpl();
            auto& state = impl.extractState();
            return state.chains.size();
        };
    auto bucketCount = nautilus::invoke(getBucketCount, hashMapRef);
    
    EntryIterator it(hashMapRef, bucketCount, fieldKeys, fieldValues);
    it.currentEntry.entryRef = nautilus::val<DataStructures::OffsetEntry*>(nullptr);
    it.numberOfBuckets = bucketCount;
    return it;
}

nautilus::val<AbstractHashMapEntry*> OffsetHashMapRef::findEntry(
    const nautilus::val<AbstractHashMapEntry*>& otherEntry) {
    
    // Get hash from the other entry (cast to OffsetEntry to access hash)
    std::function<uint64_t(DataStructures::OffsetEntry*)> getHashFunc = 
        [](DataStructures::OffsetEntry* e) -> uint64_t {
            return e ? e->hash : 0;
        };
    auto hash = nautilus::invoke(getHashFunc, 
        static_cast<nautilus::val<DataStructures::OffsetEntry*>>(
            static_cast<nautilus::val<void*>>(otherEntry)
        ));
    
    // Find entry in the hash map
    std::function<DataStructures::OffsetEntry*(DataStructures::OffsetHashMapWrapper*, uint64_t)> findBucketFunc = 
        [](DataStructures::OffsetHashMapWrapper* w, uint64_t h) -> DataStructures::OffsetEntry* {
            if (!w) return nullptr;
            return w->findBucket(h);
        };
    auto foundEntry = nautilus::invoke(findBucketFunc, hashMapRef, hash);
    
    // Walk the chain to find exact match
    while (foundEntry != nautilus::val<DataStructures::OffsetEntry*>(nullptr)) {
        std::function<uint64_t(DataStructures::OffsetEntry*)> getEntryHash = 
            [](DataStructures::OffsetEntry* e) -> uint64_t {
                return e ? e->hash : 0;
            };
        auto entryHash = nautilus::invoke(getEntryHash, foundEntry);
        
        if (entryHash == hash) {
            // Found matching hash - in a complete implementation we'd compare keys too
            return static_cast<nautilus::val<AbstractHashMapEntry*>>(
                static_cast<nautilus::val<void*>>(foundEntry)
            );
        }
        
        // Get next in chain
        std::function<uint32_t(DataStructures::OffsetEntry*)> getNextOffset = 
            [](DataStructures::OffsetEntry* e) -> uint32_t {
                return e ? e->nextOffset : 0;
            };
        auto nextOffset = nautilus::invoke(getNextOffset, foundEntry);
        
        if (nextOffset == nautilus::val<uint32_t>(0)) {
            break;
        }
        
        std::function<DataStructures::OffsetEntry*(DataStructures::OffsetHashMapWrapper*, uint32_t)> getOffsetEntry = 
            [](DataStructures::OffsetHashMapWrapper* w, uint32_t offset) -> DataStructures::OffsetEntry* {
                return w ? w->getOffsetEntry(offset) : nullptr;
            };
        foundEntry = nautilus::invoke(getOffsetEntry, hashMapRef, nextOffset);
    }
    
    return nautilus::val<AbstractHashMapEntry*>(nullptr);
}

nautilus::val<AbstractHashMapEntry*> OffsetHashMapRef::findOrCreateEntry(
    const Record& recordKey,
    const HashFunction& hashFunction,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) {
    
    // Calculate hash using the key record
    std::vector<VarVal> keyValues;
    keyValues.reserve(fieldKeys.size());
    for (const auto& field : fieldKeys) {
        keyValues.emplace_back(recordKey.read(field.fieldIdentifier));
    }
    auto hashValue = hashFunction.calculate(keyValues);
    
    // Try to find existing entry
    std::function<DataStructures::OffsetEntry*(DataStructures::OffsetHashMapWrapper*, uint64_t)> findExistingEntry = 
        [](DataStructures::OffsetHashMapWrapper* w, uint64_t h) -> DataStructures::OffsetEntry* {
            if (!w) return nullptr;
            auto entry = w->findBucket(h);
            while (entry) {
                if (entry->hash == h) {
                    // TODO: Should compare actual keys here
                    return entry;
                }
                if (entry->nextOffset != 0) {
                    entry = w->getOffsetEntry(entry->nextOffset);
                } else {
                    break;
                }
            }
            return nullptr;
        };
    auto foundEntry = nautilus::invoke(findExistingEntry, hashMapRef, static_cast<nautilus::val<uint64_t>>(hashValue));
    
    if (foundEntry != nautilus::val<DataStructures::OffsetEntry*>(nullptr)) {
        // Found existing entry
        return static_cast<nautilus::val<AbstractHashMapEntry*>>(
            static_cast<nautilus::val<void*>>(foundEntry)
        );
    }
    
    // Create new entry
    std::function<AbstractHashMapEntry*(DataStructures::OffsetHashMapWrapper*, uint64_t, AbstractBufferProvider*)> insertEntryFunc = 
        [](DataStructures::OffsetHashMapWrapper* w, uint64_t h, AbstractBufferProvider* bp) -> AbstractHashMapEntry* {
            if (!w) return nullptr;
            return w->insertEntry(h, bp);
        };
    auto newEntry = nautilus::invoke(insertEntryFunc, hashMapRef, static_cast<nautilus::val<uint64_t>>(hashValue), bufferProvider);
    
    if (newEntry != nautilus::val<AbstractHashMapEntry*>(nullptr)) {
        // Write keys to the new entry
        OffsetEntryRef entryRef(
            static_cast<nautilus::val<DataStructures::OffsetEntry*>>(
                static_cast<nautilus::val<void*>>(newEntry)
            ),
            hashMapRef,
            fieldKeys,
            fieldValues
        );
        entryRef.copyKeysToEntry(recordKey, bufferProvider);
        
        // Call initialization callback
        onInsert(newEntry);
    }
    
    return newEntry;
}

void OffsetHashMapRef::insertOrUpdateEntry(
    const nautilus::val<AbstractHashMapEntry*>& otherEntry,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onUpdate,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider) {
    
    // Try to find existing entry
    auto existingEntry = findEntry(otherEntry);
    
    if (existingEntry != nautilus::val<AbstractHashMapEntry*>(nullptr)) {
        // Entry exists, update it
        onUpdate(existingEntry);
    } else {
        // Insert new entry (cast to OffsetEntry to access hash)
        std::function<uint64_t(DataStructures::OffsetEntry*)> getHashFunc = 
            [](DataStructures::OffsetEntry* e) -> uint64_t {
                return e ? e->hash : 0;
            };
        auto hash = nautilus::invoke(getHashFunc, 
            static_cast<nautilus::val<DataStructures::OffsetEntry*>>(
                static_cast<nautilus::val<void*>>(otherEntry)
            ));
        
        std::function<AbstractHashMapEntry*(DataStructures::OffsetHashMapWrapper*, uint64_t, AbstractBufferProvider*)> insertFunc = 
            [](DataStructures::OffsetHashMapWrapper* w, uint64_t h, AbstractBufferProvider* bp) -> AbstractHashMapEntry* {
                if (!w) return nullptr;
                return w->insertEntry(h, bp);
            };
        auto newEntry = nautilus::invoke(insertFunc, hashMapRef, hash, bufferProvider);
        
        if (newEntry != nautilus::val<AbstractHashMapEntry*>(nullptr)) {
            onInsert(newEntry);
        }
    }
}

}
