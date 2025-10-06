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

#pragma once

#include <cstdint>
#include <functional>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMapRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/DataStructures/OffsetBasedHashMap.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetEntryMemoryProvider.hpp>

namespace NES::Nautilus::Interface
{

using AbstractBufferProvider = Memory::AbstractBufferProvider;

class OffsetHashMapRef final : public HashMapRef
{
public:
    struct OffsetEntryRef
    {
        void copyKeysToEntry(const Record& keys, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const;
        void copyKeysToEntry(const OffsetEntryRef& otherEntryRef, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const;
        void copyValuesToEntry(const Record& values, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const;
        void copyValuesToEntry(const OffsetEntryRef& otherEntryRef, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const;
        
        [[nodiscard]] VarVal getKey(const Record::RecordFieldIdentifier& fieldIdentifier) const;
        [[nodiscard]] Record getKey() const;
        [[nodiscard]] Record getValue() const;
        void updateEntryRef(const nautilus::val<DataStructures::OffsetEntry*>& entryRef);
        [[nodiscard]] nautilus::val<int8_t*> getValueMemArea() const;
        [[nodiscard]] HashFunction::HashValue getHash() const;
        [[nodiscard]] nautilus::val<DataStructures::OffsetEntry*> getNext() const;
        
        OffsetEntryRef(
            const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
            const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
            std::vector<MemoryProvider::OffsetFieldOffsets> fieldsKey,
            std::vector<MemoryProvider::OffsetFieldOffsets> fieldsValue);

        OffsetEntryRef(
            const nautilus::val<DataStructures::OffsetEntry*>& entryRef,
            const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
            MemoryProvider::OffsetEntryMemoryProvider memoryProviderKeys,
            MemoryProvider::OffsetEntryMemoryProvider memoryProviderValues);

        OffsetEntryRef(const OffsetEntryRef& other);
        OffsetEntryRef& operator=(const OffsetEntryRef& other);
        OffsetEntryRef(OffsetEntryRef&& other) noexcept;
        ~OffsetEntryRef() = default;

        nautilus::val<DataStructures::OffsetEntry*> entryRef;
        nautilus::val<DataStructures::OffsetHashMapWrapper*> hashMapRef;
        MemoryProvider::OffsetEntryMemoryProvider memoryProviderKeys;
        MemoryProvider::OffsetEntryMemoryProvider memoryProviderValues;
    };

    class EntryIterator
    {
    public:
        EntryIterator(
            const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
            const nautilus::val<uint64_t>& bucketIndex,
            const std::vector<MemoryProvider::OffsetFieldOffsets>& fieldKeys,
            const std::vector<MemoryProvider::OffsetFieldOffsets>& fieldValues);
        EntryIterator& operator++();
        nautilus::val<bool> operator==(const EntryIterator& other) const;
        nautilus::val<bool> operator!=(const EntryIterator& other) const;
        nautilus::val<DataStructures::OffsetEntry*> operator*() const;

        // Public for initialization
        nautilus::val<DataStructures::OffsetHashMapWrapper*> hashMapRef;
        OffsetEntryRef currentEntry;
        nautilus::val<uint64_t> bucketIndex;
        nautilus::val<uint64_t> tupleIndex;
        nautilus::val<uint64_t> numberOfBuckets;
    };

    OffsetHashMapRef(
        const nautilus::val<DataStructures::OffsetHashMapWrapper*>& hashMapRef,
        std::vector<MemoryProvider::OffsetFieldOffsets> fieldsKey,
        std::vector<MemoryProvider::OffsetFieldOffsets> fieldsValue);

    OffsetHashMapRef(const OffsetHashMapRef& other);
    OffsetHashMapRef& operator=(const OffsetHashMapRef& other);
    ~OffsetHashMapRef() override = default;

    nautilus::val<AbstractHashMapEntry*> findOrCreateEntry(
        const Record& recordKey,
        const HashFunction& hashFunction,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) override;
        
    void insertOrUpdateEntry(
        const nautilus::val<AbstractHashMapEntry*>& otherEntry,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onUpdate,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider) override;
        
    nautilus::val<AbstractHashMapEntry*> findEntry(const nautilus::val<AbstractHashMapEntry*>& otherEntry) override;
    
    [[nodiscard]] EntryIterator begin() const;
    [[nodiscard]] EntryIterator end() const;

private:
    [[nodiscard]] nautilus::val<DataStructures::OffsetEntry*> findBucket(const HashFunction::HashValue& hash) const;
    nautilus::val<DataStructures::OffsetEntry*>
    insert(const HashFunction::HashValue& hash, const nautilus::val<AbstractBufferProvider*>& bufferProvider);
    [[nodiscard]] nautilus::val<bool> compareKeys(const OffsetEntryRef& entryRef, const Record& keys) const;
    [[nodiscard]] nautilus::val<DataStructures::OffsetEntry*> findKey(const Record& recordKey, const HashFunction::HashValue& hash) const;
    [[nodiscard]] nautilus::val<DataStructures::OffsetEntry*> findEntry(const OffsetEntryRef& otherEntryRef) const;

    nautilus::val<DataStructures::OffsetHashMapWrapper*> hashMapRef;
    std::vector<MemoryProvider::OffsetFieldOffsets> fieldKeys;
    std::vector<MemoryProvider::OffsetFieldOffsets> fieldValues;
};

}
