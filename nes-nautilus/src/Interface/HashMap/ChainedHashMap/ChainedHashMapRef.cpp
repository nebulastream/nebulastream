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
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <optional>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Interface/Hash/BloomFilterRef.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/HashMap/HashMapRef.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/static.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <nautilus/val_std.hpp>
#include <ErrorHandling.hpp>
#include <select.hpp>

namespace NES
{
void ChainedHashMapRef::ChainedEntryRef::copyKeysToEntry(
    const Record& keys, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    memoryProviderKeys.writeRecord(entryRef, hashMapBuffer, bufferProvider, keys);
}

void ChainedHashMapRef::ChainedEntryRef::copyKeysToEntry(
    const ChainedEntryRef& otherEntryRef, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    memoryProviderKeys.writeEntryRef(entryRef, hashMapBuffer, bufferProvider, otherEntryRef.entryRef);
}

void ChainedHashMapRef::ChainedEntryRef::copyValuesToEntry(
    const Record& values, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    memoryProviderValues.writeRecord(entryRef, hashMapBuffer, bufferProvider, values);
}

void ChainedHashMapRef::ChainedEntryRef::copyValuesToEntry(
    const ChainedEntryRef& otherEntryRef, const nautilus::val<AbstractBufferProvider*>& bufferProvider) const
{
    memoryProviderValues.writeEntryRef(entryRef, hashMapBuffer, bufferProvider, otherEntryRef.entryRef);
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

nautilus::val<int8_t*> ChainedHashMapRef::ChainedEntryRef::getValueMemArea() const
{
    /// We call this method solely, if we actually need the value memory area and not a VarVal.
    /// Therefore, we do not store the valueOffset in the ChainedEntryRef or the ChainedEntryMemoryProvider
    /// During tracing the offset is calculated and should be stored as a constant in the compiled code
    uint64_t valueMemAreaOffset = 0;
    if (memoryProviderValues.getAllFields().empty())
    {
        /// We take the max offset of the keys
        valueMemAreaOffset = std::numeric_limits<uint64_t>::min();
        for (const auto& field : memoryProviderKeys.getAllFields())
        {
            const auto offset = field.fieldOffset;
            const auto fieldSize = field.type.getSizeInBytesWithNull();
            valueMemAreaOffset = std::max(valueMemAreaOffset, offset + fieldSize);
        }
    }
    else
    {
        /// We take the min offset of the values
        valueMemAreaOffset = std::numeric_limits<uint64_t>::max();
        for (const auto& field : memoryProviderValues.getAllFields())
        {
            const auto offset = field.fieldOffset;
            valueMemAreaOffset = std::min(valueMemAreaOffset, offset);
        }
    }
    auto castedMemArea = static_cast<nautilus::val<int8_t*>>(entryRef);
    auto valueMemArea = castedMemArea + valueMemAreaOffset;
    return valueMemArea;
}

HashFunction::HashValue ChainedHashMapRef::ChainedEntryRef::getHash() const
{
    /// Assuming that the hash value is stored after the next pointer in the ChainedHashMapEntry
    const auto hashRef = getMemberRef(entryRef, &ChainedHashMapEntry::hash);
    return readValueFromMemRef<uint64_t>(hashRef);
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::ChainedEntryRef::getNext() const
{
    const auto nextRef = getMemberRef(entryRef, &ChainedHashMapEntry::next);
    auto next = readValueFromMemRef<ChainedHashMapEntry**>(nextRef);
    return next;
}

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    BorrowedNautilusBuffer hashMapBuffer,
    std::vector<FieldOffsets> fieldsKey,
    std::vector<FieldOffsets> fieldsValue)
    : entryRef(entryRef)
    , hashMapBuffer(std::move(hashMapBuffer))
    , memoryProviderKeys(std::move(fieldsKey))
    , memoryProviderValues(std::move(fieldsValue))
{
}

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(
    const nautilus::val<ChainedHashMapEntry*>& entryRef,
    BorrowedNautilusBuffer hashMapBuffer,
    ChainedEntryMemoryProvider memoryProviderKeys,
    ChainedEntryMemoryProvider memoryProviderValues)
    : entryRef(entryRef)
    , hashMapBuffer(std::move(hashMapBuffer))
    , memoryProviderKeys(std::move(memoryProviderKeys))
    , memoryProviderValues(std::move(memoryProviderValues))
{
}

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(const ChainedEntryRef& other) = default;
ChainedHashMapRef::ChainedEntryRef& ChainedHashMapRef::ChainedEntryRef::operator=(const ChainedEntryRef& other) = default;

ChainedHashMapRef::ChainedEntryRef::ChainedEntryRef(ChainedEntryRef&& other) noexcept
    : entryRef(other.entryRef)
    , hashMapBuffer(std::move(other.hashMapBuffer))
    , memoryProviderKeys(std::move(other.memoryProviderKeys))
    , memoryProviderValues(std::move(other.memoryProviderValues))
{
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::findKey(const Record& recordKey, const HashFunction::HashValue& hash) const
{
    auto entry = findChain(hash);
    while (entry != nullptr)
    {
        const ChainedEntryRef entryRef{entry, buffer, fieldKeys, fieldValues};
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

nautilus::val<AbstractHashMapEntry*> ChainedHashMapRef::findEntry(const nautilus::val<AbstractHashMapEntry*>& otherEntry)
{
    /// Finding the entry. If chainEntry is nullptr, there does not exist a key with the same values.
    const auto chainEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(otherEntry);
    const ChainedEntryRef otherEntryRef{chainEntry, buffer, fieldKeys, fieldValues};
    const auto entryRef = findEntry(otherEntryRef);
    return entryRef;
}

nautilus::val<AbstractHashMapEntry*> ChainedHashMapRef::findOrCreateEntry(
    const Record& recordKey,
    const HashFunction& hashFunction,
    const std::function<void(nautilus::val<AbstractHashMapEntry*>&)>& onInsert,
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
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
    if (const auto entryRef = findKey(recordKey, hashValue); entryRef != nullptr)
    {
        return static_cast<nautilus::val<AbstractHashMapEntry*>>(entryRef);
    }

    /// We have not found the entry, so we need to insert a new one and copy the keys into the entry.
    auto newEntryRef = ChainedEntryRef{insert(hashValue, bufferProvider), buffer, fieldKeys, fieldValues};
    newEntryRef.copyKeysToEntry(recordKey, bufferProvider);


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
    const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    /// Finding the entry. If entry contains nullptr, there does not exist a key with the same values.
    const auto chainEntry = static_cast<nautilus::val<ChainedHashMapEntry*>>(otherEntry);
    const ChainedEntryRef otherEntryRef{chainEntry, buffer, fieldKeys, fieldValues};
    if (const auto entryRef = findEntry(otherEntryRef); entryRef != nullptr)
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
    const ChainedEntryRef newEntryRef{newEntry, buffer, fieldKeys, fieldValues};
    newEntryRef.copyKeysToEntry(otherEntryRef, bufferProvider);
    if (onInsert)
    {
        auto castedEntryRef = static_cast<nautilus::val<AbstractHashMapEntry*>>(newEntryRef.entryRef);
        onInsert(castedEntryRef);
    }
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::begin() const
{
    const nautilus::val<uint64_t> tupleIndex = 0;
    const nautilus::val<uint64_t> indexOnPage = 0;
    const nautilus::val<uint64_t> pageIndex = 0;
    nautilus::val<EntryIterator::PageCounts> args;
    const auto currentEntry = nautilus::invoke(
        +[](const TupleBuffer* buffer, const uint64_t pageIndexVal, const uint64_t indexOnPageVal, EntryIterator::PageCounts* args)
        {
            const auto chm = ChainedHashMap::load(*buffer);
            /// get number of pages in chained hash map
            args->numPages = chm.getNumberOfPages();
            if (args->numPages == 0)
            {
                return static_cast<const std::byte*>(nullptr);
            }
            /// get first page
            const auto& page = chm.getPage(pageIndexVal);
            /// get number of tuples in page
            args->numTuplesInPage = chm.getPage(pageIndexVal).getNumberOfTuples();
            /// get entry
            return page.getAvailableMemoryArea().subspan(indexOnPageVal * sizeof(ChainedHashMapEntry)).data();
        },
        buffer.asArg(),
        pageIndex,
        indexOnPage,
        &args);

    /// Guard that checks whether the hashmap is non-empty.
    if (args.get(&EntryIterator::PageCounts::numPages) != 0)
    {
        return {
            buffer,
            currentEntry,
            entrySize,
            tupleIndex,
            indexOnPage,
            args.get(&EntryIterator::PageCounts::numTuplesInPage),
            pageIndex,
            args.get(&EntryIterator::PageCounts::numPages)};
    }
    /// Empty hash map, return the end() iterator.
    return end();
}

ChainedHashMapRef::EntryIterator ChainedHashMapRef::end() const
{
    /// The iterator pointing to the end() should NEVER be advanced. Therefore, we do not need to set a lot of its members
    const auto numberOfTuples = invoke(
        +[](const TupleBuffer* buffer)
        {
            const auto chm = ChainedHashMap::load(*buffer);
            return chm.getTotalNumberOfRecords();
        },
        buffer.asArg());
    return {buffer, nullptr, entrySize, numberOfTuples, -1, -1, -1, -1};
}

nautilus::val<ChainedHashMapEntry*> ChainedHashMapRef::findChain(const HashFunction::HashValue& hash) const
{
    /// Before dereferencing the cache-cold chain, consult the in-map BloomFilter. Without a filter we go
    /// straight to the chain walk, which costs nothing extra: the chains array is nullptr-filled by init().
    if (bloomFilter and not bloomFilter->mightContain(hash))
    {
        return nullptr;
    }
    return nautilus::invoke(
        +[](const TupleBuffer* buffer, const HashFunction::HashValue::raw_type hashValue) -> ChainedHashMapEntry*
        {
            ChainedHashMap chm = ChainedHashMap::load(*buffer);
            if (chm.getTotalNumberOfRecords() == 0)
            {
                return nullptr;
            }
            const auto entryPos = hashValue & chm.getMask();
            return chm.getChain(entryPos);
        },
        buffer.asArg(),
        hash);
}

nautilus::val<ChainedHashMapEntry*>
ChainedHashMapRef::insert(const HashFunction::HashValue& hash, const nautilus::val<AbstractBufferProvider*>& bufferProvider)
{
    const auto newEntry = invoke(
        +[](TupleBuffer* buffer, const HashFunction::HashValue::raw_type hashValue, AbstractBufferProvider* bufferProviderVal)
        {
            auto chm = ChainedHashMap::load(*buffer);
            return chm.insertEntry(hashValue, bufferProviderVal);
        },
        buffer.asArg(),
        hash,
        bufferProvider);

    if (bloomFilter)
    {
        bloomFilter->add(hash);
    }

    return static_cast<nautilus::val<ChainedHashMapEntry*>>(newEntry);
}

nautilus::val<bool> ChainedHashMapRef::compareKeys(const ChainedEntryRef& entryRef, const Record& keys) const
{
    nautilus::val<bool> result{true};
    for (const auto& [fieldIdentifier, type, fieldOffset] : nautilus::static_iterable(fieldKeys))
    {
        /// We need to take the null values into account as they are a separate group.
        /// Thus, a simple if (keys.read(fieldIdentifier) != entryRef.getKey(fieldIdentifier)) is not enough
        const auto& keyValue = keys.read(fieldIdentifier);
        const auto entryValue = entryRef.getKey(fieldIdentifier);
        const auto nullsMatch = keyValue.isNull() == entryValue.isNull();
        result = result and nullsMatch;

        if (type.isType(DataType::Type::VARSIZED))
        {
            result = nautilus::select(
                keyValue.getRawValueAs<VariableSizedData>() != entryValue.getRawValueAs<VariableSizedData>(),
                nautilus::val<bool>{false},
                result);
        }
        else
        {
            result = nautilus::select(
                (keyValue.castToType(type.type) != entryValue.castToType(type.type)).getRawValueAs<nautilus::val<bool>>(),
                nautilus::val<bool>{false},
                result);
        }
    }
    return result;
}

ChainedHashMapRef::ChainedHashMapRef(
    BorrowedNautilusBuffer buffer,
    std::vector<FieldOffsets> fieldsKey,
    std::vector<FieldOffsets> fieldsValue,
    const nautilus::val<uint64_t>& entriesPerPage,
    const nautilus::val<uint64_t>& entrySize,
    const std::optional<Nautilus::Interface::BloomFilterParams> bloomFilterParams)
    /// Copied, not moved: the bloom-filter setup below still reads `buffer` after the base is initialised.
    : HashMapRef(buffer)
    , fieldKeys(std::move(fieldsKey))
    , fieldValues(std::move(fieldsValue))
    , entriesPerPage(entriesPerPage)
    , entrySize(entrySize)
{
    /// The bit area lives inline in the map's buffer and is zeroed by init(), so its address is stable and
    /// valid from here on. Resolving it once keeps the traced lookup path free of a per-call invoke.
    if (bloomFilterParams)
    {
        bloomFilter.emplace(
            invoke(
                +[](TupleBuffer* buffer IF_INVARIANT(, const uint64_t bitCount) IF_INVARIANT(, const uint64_t hashCount))
                {
                    auto chm = ChainedHashMap::load(*buffer);
                    INVARIANT(chm.getBloomFilterParams().has_value(), "The hash map was initialised without a BloomFilter bit area");
                    /// The traced probe folds bitCount into its modulo and unrolls hashCount, so a view sized
                    /// differently from the map it is bound to would index outside the allocated bit area.
                    INVARIANT(
                        chm.getBloomFilterParams()->getBitCount() == bitCount and chm.getBloomFilterParams()->getHashCount() == hashCount,
                        "BloomFilter sizing of the view ({} bits, {} hashes) does not match the map it is bound to ({} bits, {} hashes)",
                        bitCount,
                        hashCount,
                        chm.getBloomFilterParams()->getBitCount(),
                        chm.getBloomFilterParams()->getHashCount());
                    return chm.getBloomFilterMemArea();
                },
                buffer.asArg() IF_INVARIANT(, nautilus::val<uint64_t>{bloomFilterParams->getBitCount()})
                    IF_INVARIANT(, nautilus::val<uint64_t>{bloomFilterParams->getHashCount()})),
            *bloomFilterParams);
    }
}

/// Copies the already-bound bloomFilter rather than delegating to the ctor above, which would re-run its
/// invokes and emit redundant traced calls per copy.
ChainedHashMapRef::ChainedHashMapRef(const ChainedHashMapRef& other)
    : HashMapRef(other.buffer)
    , fieldKeys(other.fieldKeys)
    , fieldValues(other.fieldValues)
    , entriesPerPage(other.entriesPerPage)
    , entrySize(other.entrySize)
    , bloomFilter(other.bloomFilter)
{
}

ChainedHashMapRef& ChainedHashMapRef::operator=(const ChainedHashMapRef& other)
{
    buffer = other.buffer;
    fieldKeys = other.fieldKeys;
    fieldValues = other.fieldValues;
    entriesPerPage = other.entriesPerPage;
    entrySize = other.entrySize;
    bloomFilter = other.bloomFilter;
    return *this;
}

ChainedHashMapRef::EntryIterator::EntryIterator(
    BorrowedNautilusBuffer buffer,
    const nautilus::val<ChainedHashMapEntry*>& currentEntry,
    const nautilus::val<uint64_t>& entrySize,
    const nautilus::val<uint64_t>& tupleIndex,
    const nautilus::val<uint64_t>& indexOnPage,
    const nautilus::val<uint64_t>& numberOfTuplesInCurrentPage,
    const nautilus::val<uint64_t>& pageIndex,
    const nautilus::val<uint64_t>& numberOfPages)
    : buffer(std::move(buffer))
    , currentEntry(currentEntry)
    , entrySize(entrySize)
    , tupleIndex(tupleIndex)
    , indexOnPage(indexOnPage)
    , numberOfTuplesInCurrentPage(numberOfTuplesInCurrentPage)
    , pageIndex(pageIndex)
    , numberOfPages(numberOfPages)
{
}

ChainedHashMapRef::EntryIterator& ChainedHashMapRef::EntryIterator::operator++()
{
    /// We have to increment the tupleIndex, as we have seen a new tuple.
    ++tupleIndex;
    ++indexOnPage;
    if (indexOnPage >= numberOfTuplesInCurrentPage)
    {
        indexOnPage = 0;
        if (pageIndex + 1 >= numberOfPages)
        {
            return *this;
        }
        ++pageIndex;
        nautilus::val<PageCounts> args;
        currentEntry = nautilus::invoke(
            +[](TupleBuffer* buffer, const uint64_t pageIndexVal, const uint64_t indexOnPageVal, PageCounts* args)
            {
                const auto chm = ChainedHashMap::load(*buffer);
                /// get number of pages in chained hash map
                args->numPages = chm.getNumberOfPages();
                /// get first page
                const auto& page = chm.getPage(pageIndexVal);
                /// get number of tuples in page
                args->numTuplesInPage = chm.getPage(pageIndexVal).getNumberOfTuples();
                /// get entry
                return page.getAvailableMemoryArea().subspan(indexOnPageVal * sizeof(ChainedHashMapEntry)).data();
            },
            buffer.asArg(),
            pageIndex,
            indexOnPage,
            &args);
        numberOfTuplesInCurrentPage = args.get(&PageCounts::numTuplesInPage);
        return *this;
    }
    currentEntry = static_cast<nautilus::val<int8_t*>>(currentEntry) + entrySize;

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
    return currentEntry;
}

}
