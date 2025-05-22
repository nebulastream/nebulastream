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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <utility>
#include <vector>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMapRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/function.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/Float.hpp>
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Nautilus::Interface
{
void ChainedHashMapRef::ChainedEntryRef::copyKeysToEntry(const Nautilus::Record& keys) const
{
    memoryProviderKeys.writeRecord(entryRef, keys);
}

void ChainedHashMapRef::ChainedEntryRef::copyKeysToEntry(const ChainedEntryRef& otherEntryRef) const
{
    memoryProviderKeys.writeEntryRef(entryRef, otherEntryRef.entryRef);
}

void ChainedHashMapRef::ChainedEntryRef::copyValuesToEntry(const Nautilus::Record& values) const
{
    memoryProviderValues.writeRecord(entryRef, values);
}

void ChainedHashMapRef::ChainedEntryRef::copyValuesToEntry(const ChainedEntryRef& otherEntryRef) const
{
    memoryProviderValues.writeEntryRef(entryRef, otherEntryRef.entryRef);
}

VarVal ChainedHashMapRef::ChainedEntryRef::getKey(const Record::RecordFieldIdentifier& fieldIdentifier) const
{
    auto recordKey = memoryProviderKeys.readVarVal(entryRef, fieldIdentifier);
    return recordKey;
}

Record ChainedHashMapRef::ChainedEntryRef::getKey() const
{
    return memoryProviderKeys.readRecord(entryRef);
}

Record ChainedHashMapRef::ChainedEntryRef::getValue() const
{
    return memoryProviderValues.readRecord(entryRef);
}

void ChainedHashMapRef::ChainedEntryRef::updateEntryRef(const nautilus::val<ChainedHashMapEntry*>& entryRef)
{
    ChainedHashMapRef::ChainedEntryRef::entryRef = entryRef;
}

nautilus::val<int8_t*> ChainedHashMapRef::ChainedEntryRef::getValueMemArea() const
{
    /// We call this method solely, if we actually need the value memory area and not a VarVal.
    /// Therefore, we do not store the valueOffset in the ChainedEntryRef or the ChainedEntryMemoryProvider
    /// During tracing the minKeyOffset is calculated and should be stored as a constant in the compiled code
    nautilus::static_val<uint64_t> minKeyOffset = std::numeric_limits<uint64_t>::max();
    for (const auto& field : nautilus::static_iterable(memoryProviderValues.getAllFields()))
    {
        const auto offset = field.fieldOffset;
        if (minKeyOffset > offset)
        {
            minKeyOffset = offset;
        }
    }
    auto castedMemArea = static_cast<nautilus::val<int8_t*>>(entryRef);
    auto valueMemArea = castedMemArea + minKeyOffset;
    return valueMemArea;
}

HashFunction::HashValue ChainedHashMapRef::ChainedEntryRef::getHash() const
{
    /// Assuming that the hash value is stored after the next pointer in the ChainedHashMapEntry
    const auto hashRef = Util::getMemberRef(entryRef, &ChainedHashMapEntry::hash);
    return Util::readValueFromMemRef<uint64_t>(hashRef);
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::ChainedEntryRef::getNext() const
{
    return invoke(+[](const ChainedHashMapEntry* entry) { return entry->next; }, entryRef);
}

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    std::vector<MemoryProvider::FieldOffsets> fieldsKey,
    std::vector<MemoryProvider::FieldOffsets> fieldsValue)
    : entryRef(entryRef), memoryProviderKeys(std::move(fieldsKey)), memoryProviderValues(std::move(fieldsValue))
{
}

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    MemoryProvider::ChainedEntryMemoryProvider memoryProviderKeys,
    MemoryProvider::ChainedEntryMemoryProvider memoryProviderValues)
    : entryRef(entryRef), memoryProviderKeys(std::move(memoryProviderKeys)), memoryProviderValues(std::move(memoryProviderValues))
{
}

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(const ChainedEntryRef& other) = default;
ChainedHashMapRef::ChainedEntryRef& ChainedHashMapRef::ChainedEntryRef::operator=(const ChainedEntryRef& other) = default;

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(ChainedEntryRef&& other) noexcept
    : entryRef(other.entryRef)
    , memoryProviderKeys(std::move(other.memoryProviderKeys))
    , memoryProviderValues(std::move(other.memoryProviderValues))
{
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::findKey(const Nautilus::Record& recordKey, const HashFunction::HashValue& hash) const
{
    auto entry = findChain(hash);
    while (entry)
    {
        const ChainedEntryRef entryRef(entry, fieldKeys, fieldValues);
        if (compareKeys(entryRef, recordKey))
        {
            return entry;
        }
        entry = entryRef.getNext();
    }
    return nullptr;
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::findEntry(const ChainedEntryRef& otherEntryRef) const
{
    return findKey(otherEntryRef.getKey(), otherEntryRef.getHash());
}

nautilus::val<AbstractHashMapEntry*> ChainedHashMapRef::findOrCreateEntry(
    const Nautilus::Record& recordKey,
    const HashFunction& hashFunction,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
    const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
{
    /// Calculating the hash value of the keys and finding the entry.
    /// We can use here a std::vector to store the read VarValues of the keyFunction, as the number of keys does not change between
    /// tracing and run time of the compiled query
    std::vector<VarVal> keyValues;
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fieldKeys))
    {
        const auto& keyValue = recordKey.read(fieldIdentifier);
        keyValues.emplace_back(keyValue);
    }


    ///  If entry contains nullptr, there does not exist a key with the same values.
    const auto hashValue = hashFunction.calculate(keyValues);
    if (const auto entryRef = findKey(recordKey, hashValue))
    {
        return static_cast<nautilus::val<AbstractHashMapEntry*>>(entryRef);
    }

    /// We have not found the entry, so we need to insert a new one and copy the keys into the entry.
    const auto newEntryRef = ChainedEntryRef{insert(hashValue, bufferProvider), fieldKeys, fieldValues};
    newEntryRef.copyKeysToEntry(recordKey);


    /// Calling the onInsert lambda function to insert values or anything else that the user wants.
    auto castedEntryRef = static_cast<nautilus::val<AbstractHashMapEntry*>>(newEntryRef.entryRef);
    if (onInsert)
    {
        onInsert(castedEntryRef);
    }

    return castedEntryRef;
}

void ChainedHashMapRef::insertOrUpdateEntry(
    const nautilus::val<AbstractHashMapEntry*>& otherEntry,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onUpdate,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
    const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
{
    /// Finding the entry. If entry contains nullptr, there does not exist a key with the same values.
    const auto chainEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(otherEntry);
    const ChainedEntryRef otherEntryRef(chainEntry, fieldKeys, fieldValues);
    if (const auto entryRef = findEntry(otherEntryRef))
    {
        auto castedEntry = static_cast<nautilus::val<AbstractHashMapEntry*>>(entryRef);
        if (onUpdate)
        {
            onUpdate(castedEntry);
        }
        return;
    }

    /// We have not found the entry, so we need to insert a new one and copy the keys into the entry.
    const auto newEntry = insert(otherEntryRef.getHash(), bufferProvider);
    const ChainedEntryRef newEntryRef(newEntry, fieldKeys, fieldValues);
    newEntryRef.copyKeysToEntry(otherEntryRef);
    if (onInsert)
    {
        auto castedEntryRef = static_cast<nautilus::val<AbstractHashMapEntry*>>(newEntryRef.entryRef);
        onInsert(castedEntryRef);
    }
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::begin() const
{
    constexpr uint64_t numberOfTuples = 0;
    return {hashMapRef, numberOfTuples, fieldKeys, fieldValues};
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::end() const
{
    const auto numberOfTuples
        = invoke(+[](HashMap* hashMap) { return dynamic_cast<ChainedHashMap*>(hashMap)->getNumberOfTuples(); }, hashMapRef);
    return {hashMapRef, numberOfTuples, fieldKeys, fieldValues};
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::findChain(const HashFunction::HashValue& hash) const
{
    return invoke(
        +[](HashMap* hashMap, const HashFunction::HashValue::raw_type hashValue)
        {
            /// If no tuples are in the hashmap, we return nullptr otherwise we return the chain for the hash value.
            return hashMap->getNumberOfTuples() == 0 ? nullptr : dynamic_cast<ChainedHashMap*>(hashMap)->findChain(hashValue);
        },
        hashMapRef,
        hash);
}

nautilus::val<ChainedHashMapEntry*>
ChainedHashMapRef::insert(const HashFunction::HashValue& hash, const nautilus::val<Memory::AbstractBufferProvider*>& bufferProvider)
{
    const auto newEntry = invoke(
        +[](HashMap* hashMap, const HashFunction::HashValue::raw_type hashValue, Memory::AbstractBufferProvider* bufferProviderVal)
        { return dynamic_cast<ChainedHashMap*>(hashMap)->insertEntry(hashValue, bufferProviderVal); },
        hashMapRef,
        hash,
        bufferProvider);
    return static_cast<nautilus::val<ChainedHashMapEntry*>>(newEntry);
}

nautilus::val<bool> ChainedHashMapRef::compareKeys(const ChainedEntryRef& entryRef, const Record& keys) const
{
    nautilus::val<bool> equals = true;
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fieldKeys))
    {
        const auto& key = keys.read(fieldIdentifier);
        const auto& keyFromEntry = entryRef.getKey(fieldIdentifier);
        equals = equals && (key == keyFromEntry);
    }
    return equals;
}

ChainedHashMapRef::ChainedHashMapRef(
    const nautilus::val<HashMap*>& hashMapRef,
    std::vector<MemoryProvider::FieldOffsets> fieldsKey,
    std::vector<MemoryProvider::FieldOffsets> fieldsValue,
    const nautilus::val<uint64_t>& entriesPerPage,
    const nautilus::val<uint64_t>& entrySize)
    : HashMapRef(hashMapRef)
    , fieldKeys(std::move(fieldsKey))
    , fieldValues(std::move(fieldsValue))
    , entriesPerPage(entriesPerPage)
    , entrySize(entrySize)
{
    /// Checking if we have a float32 or float64 field in the key fields. We currently do not support this.
    /// Furthermore, there is a debate if we should support this in the future, as other systems usually have a fixed size for floats.
    for (const auto& field : nautilus::static_iterable(fieldKeys))
    {
        if (std::dynamic_pointer_cast<Float>(field.type->type))
        {
            throw NotImplemented("Float32 and Float64 are not supported in the key fields for the chained hash map.");
        }
        if (std::dynamic_pointer_cast<VariableSizedDataType>(field.type->type))
        {
            throw NotImplemented("Variable sized data types are not supported in the key fields for the chained hash map.");
        }
    }
}

ChainedHashMapRef::ChainedHashMapRef(const ChainedHashMapRef& other)
    : ChainedHashMapRef(other.hashMapRef, other.fieldKeys, other.fieldValues, other.entriesPerPage, other.entrySize)
{
}

ChainedHashMapRef& ChainedHashMapRef::operator=(const ChainedHashMapRef& other)
{
    hashMapRef = other.hashMapRef;
    fieldKeys = other.fieldKeys;
    fieldValues = other.fieldValues;
    entriesPerPage = other.entriesPerPage;
    entrySize = other.entrySize;
    return *this;
}

uint64_t findChainIndexProxy(const HashMap* hashMap, const uint64_t tupleIndexVal)
{
    const auto* const chainedHashMap = dynamic_cast<const ChainedHashMap*>(hashMap);
    uint64_t seenTuples = 0;
    uint64_t chainIndex = 0;

    /// First, we have to find a chain that is not empty (!= nullptr)
    for (; chainIndex < chainedHashMap->getNumberOfChains(); chainIndex = chainIndex + 1)
    {
        if (chainedHashMap->getStartOfChain(chainIndex) != nullptr)
        {
            break;
        }
    }

    /// Second, we have to find the chain that contains the tuple with the index #tupleIndexVal
    /// Thus, we count how many tuples are there per chain until we reach the tuple with the index #tupleIndexVal
    for (; chainIndex < chainedHashMap->getNumberOfChains(); ++chainIndex)
    {
        const auto* currentChain = chainedHashMap->getStartOfChain(chainIndex);
        while (currentChain != nullptr && seenTuples < tupleIndexVal)
        {
            ++seenTuples;
            currentChain = currentChain->next;
        }
        if (seenTuples >= tupleIndexVal)
        {
            return chainIndex;
        }
    }
    INVARIANT(false, "Could not find the tuple with index {} in ChainedHashMap.", tupleIndexVal);
    std::terminate(); /// Ensure termination even if INVARIANT is disabled.
}

ChainedHashMapRef::EntryIterator::EntryIterator(
    const nautilus::val<HashMap*>& hashMapRef,
    const nautilus::val<uint64_t>& tupleIndex,
    const std::vector<MemoryProvider::FieldOffsets>& fieldKeys,
    const std::vector<MemoryProvider::FieldOffsets>& fieldValues)
    : hashMapRef(hashMapRef), currentEntry({nullptr, fieldKeys, fieldValues}), chainIndex(0), tupleIndex(tupleIndex)
{
    const auto numberOfEntries = nautilus::invoke(
        +[](const HashMap* hashMap) -> uint64_t
        {
            if (hashMap == nullptr)
            {
                return 0;
            }
            const auto* const chainedHashMap = dynamic_cast<const ChainedHashMap*>(hashMap);
            return chainedHashMap->getNumberOfTuples();
        },
        hashMapRef);

    /// We only set the members, if the number of entries is larger than 0. So if there exist some numberOfEntries
    if (numberOfEntries == 0)
    {
        return;
    }
    /// We have to initialize the chainIndex and the currentEntry by iterating over the chains until we have seen #tupleIndex tuples.
    chainIndex = invoke(findChainIndexProxy, hashMapRef, tupleIndex);
    currentEntry = ChainedHashMapRef::ChainedEntryRef(
        invoke(
            +[](const HashMap* hashMap, const uint64_t chainIndexVal)
            { return dynamic_cast<const ChainedHashMap*>(hashMap)->getStartOfChain(chainIndexVal); },
            hashMapRef,
            chainIndex),
        fieldKeys,
        fieldKeys);
    numberOfChains
        = invoke(+[](const HashMap* hashMap) { return dynamic_cast<const ChainedHashMap*>(hashMap)->getNumberOfChains(); }, hashMapRef);
}

ChainedHashMapRef::EntryIterator& ChainedHashMapRef::EntryIterator::operator++()
{
    /// Going to the next entry for the current chain
    currentEntry.updateEntryRef(currentEntry.getNext());
    while (not currentEntry.entryRef)
    {
        chainIndex = chainIndex + nautilus::val<uint64_t>(1);
        currentEntry.updateEntryRef(invoke(
            +[](const HashMap* hashMap, const uint64_t chainIndexVal)
            { return dynamic_cast<const ChainedHashMap*>(hashMap)->getStartOfChain(chainIndexVal); },
            hashMapRef,
            chainIndex));
    }

    /// We have to increment the tupleIndex, as we have seen a new tuple.
    tupleIndex = tupleIndex + nautilus::val<uint64_t>(1);
    return *this;
}

nautilus::val<bool> ChainedHashMapRef::EntryIterator::operator==(const EntryIterator& other) const
{
    return tupleIndex == other.tupleIndex;
}

nautilus::val<bool> ChainedHashMapRef::EntryIterator::operator!=(const EntryIterator& other) const
{
    return not(*this == other);
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::EntryIterator::operator*() const
{
    return currentEntry.entryRef;
}

}
