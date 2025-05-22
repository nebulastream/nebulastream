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

#include <bit>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES::Nautilus::Interface
{
/// Taken from https://github.com/TimoKersten/db-engine-paradigms/blob/ae3286b279ad26ab294224d630d650bc2f2f3519/include/common/runtime/Hashmap.hpp#L193
/// Calculates the capacity of the hash map for the expected number of keys
/// This method assures that the capacity is a power of 2 that is greater or equal to the number of keys
constexpr auto assumedLoadFactor = 0.75;

uint64_t calcCapacity(const uint64_t numberOfKeys, const double loadFactor)
{
    PRECONDITION(numberOfKeys > 0, "Number of keys {} has to be greater than 0", numberOfKeys);
    PRECONDITION(loadFactor > 0, "Load factor {} has to be greater than 0", loadFactor);

    const uint64_t numberOfZeroBits = std::countl_zero(numberOfKeys);
    INVARIANT(
        numberOfZeroBits < 64,
        "Number of keys {} is too large for the hash map. The number of keys has to be smaller than 2^64 with numberOfZeroBits {}",
        numberOfKeys,
        numberOfZeroBits);

    constexpr uint64_t oneAsUint64 = 1;
    const uint64_t exp = 64 - numberOfZeroBits;
    const auto capacity = (oneAsUint64 << exp);
    if (static_cast<uint64_t>(capacity * loadFactor) < numberOfKeys)
    {
        return capacity << 1UL;
    }
    return capacity;
}

ChainedHashMap::ChainedHashMap(const uint64_t keySize, const uint64_t valueSize, const uint64_t numberOfBuckets, const uint64_t pageSize)
    : numberOfTuples(0)
    , pageSize(pageSize)
    , entrySize(sizeof(ChainedHashMapEntry) + keySize + valueSize)
    , entriesPerPage(pageSize / entrySize)
    , numberOfChains(calcCapacity(numberOfBuckets, assumedLoadFactor))
    , entries(nullptr)
    , mask(numberOfChains - 1)
    /// Setting the default destructor callback to a dummy function, thus we can call it without having to check if it is set
    , destructorCallBack(+[](ChainedHashMapEntry*) { })
{
    PRECONDITION(entrySize > 0, "Entry size has to be greater than 0. Entry size is set to small for entry size {}", entrySize);
    PRECONDITION(
        entriesPerPage > 0,
        "At least one entry has to fit on a page. Pagesize is set to small for pageSize {} and entry size {}",
        pageSize,
        entrySize);
    PRECONDITION(
        numberOfChains > 0,
        "Number of chains has to be greater than 0. Number of chains is set to small for number of chains {}",
        numberOfChains);
    PRECONDITION(
        (numberOfChains & (numberOfChains - 1)) == 0,
        "Number of chains has to be a power of 2. Number of chains is set to small for number of chains {}",
        numberOfChains);
}

ChainedHashMap::~ChainedHashMap()
{
    clear();
}

void ChainedHashMap::setDestructorCallback(const std::function<void(ChainedHashMapEntry*)>& callback)
{
    destructorCallBack = callback;
}

ChainedHashMapEntry* ChainedHashMap::findChain(const HashFunction::HashValue::raw_type hash) const
{
    const auto entryStartPos = hash & mask;
    return entries[entryStartPos];
}

uint64_t ChainedHashMap::getNumberOfTuples() const
{
    return numberOfTuples;
}

AbstractHashMapEntry*
ChainedHashMap::insertEntry(const HashFunction::HashValue::raw_type hash, Memory::AbstractBufferProvider* bufferProvider)
{
    /// 0. Checking, if we have to set fill the entry space. This should be only done once, i.e., when the entries are still null
    if (entries == nullptr)
    {
        /// We add one more entry to the capacity, as we need to have a valid entry for the last entry in the entries array
        /// We will be using this entry for checking, if we are at the end of our hash map in our EntryIterator
        const auto totalSpace = (numberOfChains + 1) * sizeof(ChainedHashMapEntry*);
        const auto entryBuffer = bufferProvider->getUnpooledBuffer(totalSpace);
        if (not entryBuffer)
        {
            throw CannotAllocateBuffer("Could not allocate memory for ChainedHashMap of size {}", std::to_string(totalSpace));
        }
        entrySpace = entryBuffer.value();
        entries = reinterpret_cast<ChainedHashMapEntry**>(entrySpace.getBuffer());
        std::memset(static_cast<void*>(entries), 0, entryBuffer->getBufferSize());

        /// Pointing the end of the entries to itself
        entries[numberOfChains] = reinterpret_cast<ChainedHashMapEntry*>(&entries[numberOfChains]);
    }

    /// 1. Check if we need to allocate a new page
    if (numberOfTuples % entriesPerPage == 0)
    {
        auto newPage = bufferProvider->getUnpooledBuffer(pageSize);
        if (not newPage)
        {
            throw CannotAllocateBuffer("Could not allocate memory for new page in ChainedHashMap of size {}", std::to_string(pageSize));
        }
        std::memset(newPage.value().getBuffer(), 0, pageSize);
        storageSpace.emplace_back(newPage.value());
    }

    /// 2. Finding the new entry
    const auto pageIndex = numberOfTuples / entriesPerPage;
    INVARIANT(
        storageSpace.size() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        storageSpace.size());
    auto* page = storageSpace[pageIndex].getBuffer();
    const auto entryOffsetInBuffer = numberOfTuples - (pageIndex * entriesPerPage);
    auto* const newEntry = reinterpret_cast<ChainedHashMapEntry*>(page + (entryOffsetInBuffer * entrySize));

    /// 3. Inserting the new entry
    const auto entryPos = hash & mask;
    INVARIANT(entryPos <= mask, "Invalid entry position, as pos {} is greater than mask {}", entryPos, mask);
    INVARIANT(entryPos < numberOfChains, "Invalid entry position as pos {} is greater than capacity {}", entryPos, numberOfChains);
    new (newEntry) ChainedHashMapEntry(hash);

    /// 4. Updating the chain and the current size
    auto* const oldValue = entries[entryPos];
    newEntry->next = oldValue;
    entries[entryPos] = newEntry;
    this->numberOfTuples++;
    return newEntry;
}

const ChainedHashMapEntry* ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    PRECONDITION(pageIndex < storageSpace.size(), "Page index {} is greater than the number of pages {}", pageIndex, storageSpace.size());
    return storageSpace[pageIndex].getBuffer<ChainedHashMapEntry>();
}

ChainedHashMapEntry* ChainedHashMap::getStartOfChain(const uint64_t entryIdx) const
{
    PRECONDITION(entryIdx <= numberOfChains, "Entry index {} is greater than the capacity {}", entryIdx, numberOfChains);
    return entries[entryIdx];
}

uint64_t ChainedHashMap::getNumberOfChains() const
{
    return numberOfChains;
}

void ChainedHashMap::clear() noexcept
{
    /// Deleting all entries in the hash map
    if (entries != nullptr)
    {
        /// Calling for every value in the hash map the destructor callback
        /// We start here by iterating over all entries while starting in the entry space
        for (uint64_t entryIdx = 0; entryIdx < numberOfChains; ++entryIdx)
        {
            auto* entry = entries[entryIdx];
            while (entry != nullptr)
            {
                auto* next = entry->next;
                destructorCallBack(entry);
                entry = next;
            }
        }
    }
    entries = nullptr;
    numberOfTuples = 0;

    /// Releasing all memory
    storageSpace.clear();
    entrySpace.release();
}

}
