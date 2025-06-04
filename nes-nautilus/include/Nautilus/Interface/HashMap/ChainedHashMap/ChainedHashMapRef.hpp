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
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMapRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES::Nautilus::Interface
{
/// Forward declaration of EntryIterator so that we can use it in ChainedHashMapRef for making EntryIterator friend.
class EntryIterator;

class ChainedHashMapRef final : public HashMapRef
{
public:
    /// A nautilus wrapper to operate on the chained hash map.
    /// Especially a wrapper around a ChainedHashMapEntry.
    struct ChainedEntryRef
    {
        void copyKeysToEntry(const Record& keys, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const;
        void
        copyKeysToEntry(const ChainedEntryRef& otherEntryRef, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const;
        void copyValuesToEntry(const Record& values, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const;
        void
        copyValuesToEntry(const ChainedEntryRef& otherEntryRef, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) const;
        [[nodiscard]] VarVal getKey(const Record::RecordFieldIdentifier& fieldIdentifier) const;
        [[nodiscard]] Record getKey() const;
        [[nodiscard]] Record getValue() const;
        void updateEntryRef(const nautilus::val<ChainedHashMapEntry*>& entryRef);
        [[nodiscard]] nautilus::val<int8_t*> getValueMemArea() const;
        [[nodiscard]] HashFunction::HashValue getHash() const;
        [[nodiscard]] nautilus::val<ChainedHashMapEntry*> getNext() const;
        ChainedEntryRef(
            const nautilus::val<ChainedHashMapEntry*>& entryRef,
            const nautilus::val<ChainedHashMap*>& hashMapRef,
            std::vector<MemoryProvider::FieldOffsets> fieldsKey,
            std::vector<MemoryProvider::FieldOffsets> fieldsValue);

        ChainedEntryRef(
            const nautilus::val<ChainedHashMapEntry*>& entryRef,
            const nautilus::val<ChainedHashMap*>& hashMapRef,
            MemoryProvider::ChainedEntryMemoryProvider memoryProviderKeys,
            MemoryProvider::ChainedEntryMemoryProvider memoryProviderValues);

        ChainedEntryRef(const ChainedEntryRef& other);
        ChainedEntryRef& operator=(const ChainedEntryRef& other);
        ChainedEntryRef(ChainedEntryRef&& other) noexcept;
        ~ChainedEntryRef() = default;


        nautilus::val<ChainedHashMapEntry*> entryRef;
        nautilus::val<ChainedHashMap*> hashMapRef;
        MemoryProvider::ChainedEntryMemoryProvider memoryProviderKeys;
        MemoryProvider::ChainedEntryMemoryProvider memoryProviderValues;
    };

    /// Iterator for iterating over all entries in the hash map.
    /// The idea is that we are starting at each chain until we reach the end of the chain.
    /// Then, we are moving to the next chain until we reach the end of the hash map.
    class EntryIterator
    {
    public:
        EntryIterator(
            const nautilus::val<HashMap*>& hashMapRef,
            const nautilus::val<uint64_t>& tupleIndex,
            const std::vector<MemoryProvider::FieldOffsets>& fieldKeys,
            const std::vector<MemoryProvider::FieldOffsets>& fieldValues);
        EntryIterator& operator++();
        nautilus::val<bool> operator==(const EntryIterator& other) const;
        nautilus::val<bool> operator!=(const EntryIterator& other) const;
        nautilus::val<ChainedHashMapEntry*> operator*() const;

    private:
        nautilus::val<HashMap*> hashMapRef;
        ChainedEntryRef currentEntry;
        nautilus::val<uint64_t> chainIndex;
        nautilus::val<uint64_t> tupleIndex;
        nautilus::val<uint64_t> numberOfChains;
    };

    ChainedHashMapRef(
        const nautilus::val<HashMap*>& hashMapRef,
        std::vector<MemoryProvider::FieldOffsets> fieldsKey,
        std::vector<MemoryProvider::FieldOffsets> fieldsValue,
        const nautilus::val<uint64_t>& entriesPerPage,
        const nautilus::val<uint64_t>& entrySize);
    ChainedHashMapRef(const ChainedHashMapRef& other);
    ChainedHashMapRef& operator=(const ChainedHashMapRef& other);
    ~ChainedHashMapRef() override = default;

    nautilus::val<AbstractHashMapEntry*> findOrCreateEntry(
        const Record& recordKey,
        const HashFunction& hashFunction,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) override;
    void insertOrUpdateEntry(
        const nautilus::val<AbstractHashMapEntry*>& otherEntry,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onUpdate,
        const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
        const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider) override;
    nautilus::val<AbstractHashMapEntry*> findEntry(const nautilus::val<AbstractHashMapEntry*>& otherEntry) override;
    [[nodiscard]] EntryIterator begin() const;
    [[nodiscard]] EntryIterator end() const;


private:
    /// Finds the chain for the given hash value. If no chain exists, it returns nullptr.
    [[nodiscard]] nautilus::val<ChainedHashMapEntry*> findChain(const HashFunction::HashValue& hash) const;
    nautilus::val<ChainedHashMapEntry*>
    insert(const HashFunction::HashValue& hash, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider);
    [[nodiscard]] nautilus::val<bool> compareKeys(const ChainedEntryRef& entryRef, const Record& keys) const;
    [[nodiscard]] nautilus::val<ChainedHashMapEntry*> findKey(const Record& recordKey, const HashFunction::HashValue& hash) const;
    [[nodiscard]] nautilus::val<ChainedHashMapEntry*> findEntry(const ChainedEntryRef& otherEntryRef) const;

    std::vector<MemoryProvider::FieldOffsets> fieldKeys;
    std::vector<MemoryProvider::FieldOffsets> fieldValues;
    nautilus::val<uint64_t> entriesPerPage;
    nautilus::val<uint64_t> entrySize;
};
}
