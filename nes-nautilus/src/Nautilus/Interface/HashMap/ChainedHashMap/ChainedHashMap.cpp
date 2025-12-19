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
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <ErrorHandling.hpp>

namespace NES
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

ChainedHashMap::ChainedHashMap(AbstractBufferProvider* bufferProvider, uint64_t entrySize, const uint64_t numberOfBuckets, uint64_t pageSize)
{
    const uint64_t entriesPerPage = pageSize / entrySize;
    const uint64_t numberOfChains = calcCapacity(numberOfBuckets, assumedLoadFactor);
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

    // get the tuple buffer for the main buffer
    const auto totalSpace = 9 * sizeof(uint64_t) + (numberOfChains + 1) * sizeof(ChainedHashMapEntry*);
    const auto buffer = bufferProvider->getUnpooledBuffer(totalSpace);
    if (not buffer)
    {
        throw CannotAllocateBuffer("Could not allocate memory for ChainedHashMap of size {}", std::to_string(totalSpace));
    }
    mainBuffer = std::move(buffer.value());
    // fill in the metadata and the entries mapping
    auto basePointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
    basePointer[Metadata::NUM_CHAINS_POS] = numberOfChains;
    basePointer[Metadata::PAGE_SIZE_POS] = pageSize;
    basePointer[Metadata::ENTRY_SIZE_POS] = entrySize;
    basePointer[Metadata::ENTRIES_PER_PAGE_POS] = entriesPerPage;
    basePointer[Metadata::NUM_TUPLES_POS] = 0;
    basePointer[Metadata::MASK_POS] = numberOfChains - 1;
    auto entriesPointer = basePointer.subspan(Metadata::CHAINS_BEGIN_POS, numberOfChains - 1);
    std::ranges::fill(entriesPointer, 0);
    // num pages
    basePointer[Metadata::NUM_PAGES_POS] = 0;
    auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    // child buffer indices
    indexPointer[Metadata::STORAGE_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Metadata::VARSIZED_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Metadata::STORAGE_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Metadata::VARSIZED_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
}

ChainedHashMap::ChainedHashMap(AbstractBufferProvider* bufferProvider, const uint64_t keySize, const uint64_t valueSize, const uint64_t numberOfBuckets, const uint64_t pageSize)
{
    const uint64_t entrySize = sizeof(ChainedHashMapEntry) + keySize + valueSize;
    const uint64_t entriesPerPage = pageSize / entrySize;
    const uint64_t numberOfChains = calcCapacity(numberOfBuckets, assumedLoadFactor);
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
    // get the tuple buffer for the main buffer
    const auto totalSpace = calculateMainBufferSize(numberOfChains);
    const auto buffer = bufferProvider->getUnpooledBuffer(totalSpace);
    if (not buffer)
    {
        throw CannotAllocateBuffer("Could not allocate memory for ChainedHashMap of size {}", std::to_string(totalSpace));
    }
    mainBuffer = std::move(buffer.value());

    // fill in the metadata and the entries mapping
    auto basePointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
    basePointer[Metadata::NUM_CHAINS_POS] = numberOfChains;
    basePointer[Metadata::PAGE_SIZE_POS] = pageSize;
    basePointer[Metadata::ENTRY_SIZE_POS] = entrySize;
    basePointer[Metadata::ENTRIES_PER_PAGE_POS] = entriesPerPage;
    basePointer[Metadata::NUM_TUPLES_POS] = 0;
    basePointer[Metadata::MASK_POS] = numberOfChains - 1;
    auto entriesPointer = basePointer.subspan(Metadata::CHAINS_BEGIN_POS, numberOfChains - 1);
    std::ranges::fill(entriesPointer, 0);
    // num pages
    basePointer[Metadata::NUM_PAGES_POS] = 0;
    // child buffer indices pointer
    auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    // initialize child buffer indices
    indexPointer[Metadata::STORAGE_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Metadata::VARSIZED_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Metadata::STORAGE_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Metadata::VARSIZED_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
}

std::unique_ptr<ChainedHashMap> ChainedHashMap::createNewMapWithSameConfiguration(AbstractBufferProvider* bufferProvider, const ChainedHashMap& other)
{
    return std::make_unique<ChainedHashMap>(bufferProvider, other.entrySize(), other.numberOfChains(), other.pageSize());
}

// ChainedHashMapEntry* ChainedHashMap::findChain(const HashFunction::HashValue::raw_type hash) const
// {
//     const auto entryStartPos = hash & getMask();
//     return entries[entryStartPos];
// }

std::span<std::byte> ChainedHashMap::allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, const size_t neededSize)
{
    uint64_t varSizedStorageLastPageSize = 0;
    uint64_t varSizedStorageLastPageNumTuples = 0;
    if (varSizedSpaceLastPageIndex() != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        auto varSizedStorageLastPageBuffer = mainBuffer.loadChildBuffer(varSizedSpaceLastPageIndex());
        varSizedStorageLastPageSize = varSizedStorageLastPageBuffer.getBufferSize();
        varSizedStorageLastPageNumTuples = varSizedStorageLastPageBuffer.getNumberOfTuples();
    }
    if (varSizedStorageLastPageSize == 0 or varSizedStorageLastPageNumTuples + neededSize >= varSizedStorageLastPageSize)
    {
        /// We allocate more space than currently necessary for the variable sized data to reduce the allocation overhead
        auto varSizedBuffer = bufferProvider->getUnpooledBuffer(neededSize * Metadata::NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS);
        if (not varSizedBuffer)
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for ChainedHashMap of size {}",
                std::to_string(neededSize * Metadata::NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS));
        }
        // store new var sized buffer page
        auto newVarSizedBufferIndex = mainBuffer.storeChildBuffer(varSizedBuffer.value());
        if (varSizedSpaceLastPageIndex() != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
        {
            // append new page for var sized storage
            auto varSizedStorageLastPageBuffer = mainBuffer.loadChildBuffer(varSizedSpaceLastPageIndex());
            auto bufferPointer = varSizedStorageLastPageBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
            bufferPointer[0] = newVarSizedBufferIndex;
        } else
        {
            // first page for var sized storage
            auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
            // store at the storage space pointer of the main buffer
            indexPointer[Metadata::VARSIZED_SPACE_INDEX_POS] = newVarSizedBufferIndex;
        }
        // set last page index
        setVarSizedSpaceLastPageIndex(newVarSizedBufferIndex);
    }

    auto newlyAddedPage = mainBuffer.loadChildBuffer(varSizedSpaceLastPageIndex());
    newlyAddedPage.setNumberOfTuples(newlyAddedPage.getNumberOfTuples() + neededSize);
    return newlyAddedPage.getAvailableMemoryArea().subspan(newlyAddedPage.getNumberOfTuples() - neededSize);
}

uint64_t ChainedHashMap::numberOfTuples() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::NUM_TUPLES_POS];
}

VariableSizedAccess::Index ChainedHashMap::appendPage(AbstractBufferProvider* bufferProvider)
{
    // create and initialize new page
    auto newPage = bufferProvider->getUnpooledBuffer(pageSize());
    if (not newPage)
    {
        throw CannotAllocateBuffer("Could not allocate memory for new page in ChainedHashMap of size {}", std::to_string(pageSize()));
    }
    std::ranges::fill(newPage.value().getAvailableMemoryArea(), std::byte{0});
    auto newPagePointer = newPage->getAvailableMemoryArea<VariableSizedAccess::Index>();
    newPagePointer[0] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // store new page as child buffer to the main buffer and as the last page index
    auto newPageIndex = mainBuffer.storeChildBuffer(newPage.value());
    if (getNumberOfPages() > 0)
    {
        // a page exists
        auto lastPage = mainBuffer.loadChildBuffer(storageSpaceLastPageIndex());
        auto indexPointer = lastPage.getAvailableMemoryArea<VariableSizedAccess::Index>();
        // store at the next child buffer index position of the last page
        indexPointer[Metadata::STORAGE_SPACE_INDEX_POS] = newPageIndex;
    } else
    {
        // no pages yet, insert first page
        auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
        // store at the storage space pointer of the main buffer
        indexPointer[Metadata::STORAGE_SPACE_INDEX_POS] = newPageIndex;
    }
    setStorageSpaceLastPageIndex(newPageIndex);
    // increase num page count
    auto numPagesPointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
    numPagesPointer[Metadata::NUM_PAGES_POS]++;
    return newPageIndex;
}

AbstractHashMapEntry* ChainedHashMap::insertEntry(const HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider)
{
    /// 0. Checking, if we have to set fill the entry space. This should be only done once, i.e., when the entries are still null
    // if (entries == nullptr) [[unlikely]]
    // {
    //     /// We add one more entry to the capacity, as we need to have a valid entry for the last entry in the entries array
    //     /// We will be using this entry for checking, if we are at the end of our hash map in our EntryIterator
    //     const auto totalSpace = (numberOfChains + 1) * sizeof(ChainedHashMapEntry*);
    //     const auto entryBuffer = bufferProvider->getUnpooledBuffer(totalSpace);
    //     if (not entryBuffer)
    //     {
    //         throw CannotAllocateBuffer("Could not allocate memory for ChainedHashMap of size {}", std::to_string(totalSpace));
    //     }
    //     entrySpace = entryBuffer.value();
    //     entries = reinterpret_cast<ChainedHashMapEntry**>(entrySpace.getAvailableMemoryArea().data());
    //     std::memset(static_cast<void*>(entries), 0, entryBuffer->getBufferSize());
    //
    //     /// Pointing the end of the entries to itself
    //     entries[numberOfChains] = reinterpret_cast<ChainedHashMapEntry*>(&entries[numberOfChains]);
    // }

    /// 1. Check if we need to allocate a new page
    if (numberOfTuples() % entriesPerPage() == 0)
    {
        // create new page and add it as the last page in the linked list
        auto newPage = appendPage(bufferProvider);
    }

    /// 2. Finding the new entry
    const auto pageIndex = numberOfTuples() / entriesPerPage();
    INVARIANT(
        getNumberOfPages() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        getNumberOfPages());
    // get the page by iterating the linked list
    // todo: room for improvement here since iterating for a specific page is sub-optimal (ideas: use index, prune iterations etc.)
    auto currPage = mainBuffer.loadChildBuffer(storageSpaceChildBufferIndex());
    for (uint64_t i=1; i<pageIndex; i++)
    {
        auto nextChildBufferIndex = currPage.getAvailableMemoryArea<VariableSizedAccess::Index>()[0];
        currPage = mainBuffer.loadChildBuffer(nextChildBufferIndex);
    }
    currPage.setNumberOfTuples(currPage.getNumberOfTuples() + 1);
    const auto entryOffsetInBuffer = (numberOfTuples() - (pageIndex * entriesPerPage())) * entrySize();

    /// 3. Inserting the new entry
    const auto entryPos = hash & mask();
    INVARIANT(entryPos <= mask(), "Invalid entry position, as pos {} is greater than mask {}", entryPos, mask());
    INVARIANT(entryPos < numberOfChains(), "Invalid entry position as pos {} is greater than capacity {}", entryPos, numberOfChains());
    auto* const newEntry = new (currPage.getAvailableMemoryArea().subspan(entryOffsetInBuffer).data()) ChainedHashMapEntry(hash);

    /// 4. Updating the chain and the current size
    auto mainBufferPointer = mainBuffer.getAvailableMemoryArea<ChainedHashMapEntry*>();
    if (mainBufferPointer[Metadata::CHAINS_BEGIN_POS + entryPos] == nullptr)
    {
        mainBufferPointer[Metadata::CHAINS_BEGIN_POS + entryPos] = newEntry;
    } else
    {
        auto *oldValue = mainBufferPointer[Metadata::CHAINS_BEGIN_POS + entryPos];
        newEntry->next = oldValue;
        mainBufferPointer[Metadata::CHAINS_BEGIN_POS + entryPos] = newEntry;
    }

    /// 4. Updating the chain and the current size
    // auto* const oldValue = entries[entryPos];
    // newEntry->next = oldValue;
    // entries[entryPos] = newEntry;
    // this->numberOfTuples++;
    auto numTuplesPointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
    numTuplesPointer[Metadata::NUM_PAGES_POS]++;
    return newEntry;
}

const TupleBuffer ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    PRECONDITION(pageIndex < getNumberOfPages(), "Page index {} is greater than the number of pages {}", pageIndex, getNumberOfPages());
    auto currPage = mainBuffer.loadChildBuffer(storageSpaceChildBufferIndex());
    for (uint64_t i=1; i<pageIndex; i++)
    {
        auto nextChildBufferIndex = currPage.getAvailableMemoryArea<VariableSizedAccess::Index>()[0];
        currPage = mainBuffer.loadChildBuffer(nextChildBufferIndex);
    }
    return currPage;
}

uint64_t ChainedHashMap::calculateMainBufferSize(uint64_t numberOfChains)
{
    return (Metadata::MAIN_BUFFER_UINT64_FIELDS_NUM * sizeof(uint64_t)) + // metadata
        ((numberOfChains + 1) * sizeof(ChainedHashMapEntry*)) + // entries
        (Metadata::MAIN_BUFFER_UINT32_FIELDS_NUM * sizeof(VariableSizedAccess::Index)); // storageSpaceChildBufferIndex + varSizedSpaceChildBufferIndex
}

uint64_t ChainedHashMap::getNumberOfPages() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::NUM_PAGES_POS];
}

uint64_t ChainedHashMap::numberOfChains() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::NUM_CHAINS_POS];
}

uint64_t ChainedHashMap::entrySize() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::ENTRY_SIZE_POS];
}

uint64_t ChainedHashMap::entriesPerPage() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::ENTRIES_PER_PAGE_POS];
}

uint64_t ChainedHashMap::pageSize() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::PAGE_SIZE_POS];
}

uint64_t ChainedHashMap::mask() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Metadata::MASK_POS];
}

VariableSizedAccess::Index ChainedHashMap::storageSpaceChildBufferIndex() const
{
    return mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[Metadata::STORAGE_SPACE_INDEX_POS];
}

VariableSizedAccess::Index ChainedHashMap::varSizedSpaceChildBufferIndex() const
{
    return mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[Metadata::VARSIZED_SPACE_INDEX_POS];
}

VariableSizedAccess::Index ChainedHashMap::storageSpaceLastPageIndex() const
{
    return mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[Metadata::STORAGE_SPACE_LAST_PAGE_INDEX_POS];
}

VariableSizedAccess::Index ChainedHashMap::varSizedSpaceLastPageIndex() const
{
    return mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[Metadata::VARSIZED_SPACE_INDEX_POS];
}


void ChainedHashMap::setStorageSpaceLastPageIndex(VariableSizedAccess::Index childBufferIndex)
{
    // child buffer indices pointer
    auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    // set as lat page
    indexPointer[Metadata::STORAGE_SPACE_LAST_PAGE_INDEX_POS] = childBufferIndex;
}

void ChainedHashMap::setVarSizedSpaceLastPageIndex(VariableSizedAccess::Index childBufferIndex)
{
    // child buffer indices pointer
    auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    // set as lat page
    indexPointer[Metadata::VARSIZED_SPACE_LAST_PAGE_INDEX_POS] = childBufferIndex;
}

ChainedHashMapEntry* ChainedHashMap::getChain(uint64_t pos) const
{
    return mainBuffer.getAvailableMemoryArea<ChainedHashMapEntry*>()[Metadata::CHAINS_BEGIN_POS + pos];
}

}
