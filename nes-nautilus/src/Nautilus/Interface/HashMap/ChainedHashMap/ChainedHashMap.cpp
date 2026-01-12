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
    PRECONDITION(pageSize == 4096, "Page size must match with the default tuple buffer size of 4KB. Current page size: {}", pageSize);

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
    basePointer[Offsets::NUM_CHAINS_POS] = numberOfChains;
    basePointer[Offsets::PAGE_SIZE_POS] = pageSize;
    basePointer[Offsets::ENTRY_SIZE_POS] = entrySize;
    basePointer[Offsets::ENTRIES_PER_PAGE_POS] = entriesPerPage;
    basePointer[Offsets::NUM_TUPLES_POS] = 0;
    basePointer[Offsets::MASK_POS] = numberOfChains - 1;
    // num pages
    basePointer[Offsets::NUM_PAGES_POS] = 0;
    basePointer[Offsets::NUM_VARSIZED_PAGES_POS] = 0;
    auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    // child buffer indices
    indexPointer[Offsets::STORAGE_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Offsets::VARSIZED_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // indexPointer[Offsets::STORAGE_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // indexPointer[Offsets::VARSIZED_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // entries
    auto entriesPointer = basePointer.subspan(Offsets::CHAINS_BEGIN_POS, numberOfChains - 1);
    std::ranges::fill(entriesPointer, 0);
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
    PRECONDITION(pageSize == 4096, "Page size must match with the default tuple buffer size of 4KB. Current page size: {}", pageSize);
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
    basePointer[Offsets::NUM_CHAINS_POS] = numberOfChains;
    basePointer[Offsets::PAGE_SIZE_POS] = pageSize;
    basePointer[Offsets::ENTRY_SIZE_POS] = entrySize;
    basePointer[Offsets::ENTRIES_PER_PAGE_POS] = entriesPerPage;
    basePointer[Offsets::NUM_TUPLES_POS] = 0;
    basePointer[Offsets::MASK_POS] = numberOfChains - 1;
    // num pages
    basePointer[Offsets::NUM_PAGES_POS] = 0;
    basePointer[Offsets::NUM_VARSIZED_PAGES_POS] = 0;
    // child buffer indices pointer
    auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
    // child buffer indices
    indexPointer[Offsets::STORAGE_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    indexPointer[Offsets::VARSIZED_SPACE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // indexPointer[Offsets::STORAGE_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // indexPointer[Offsets::VARSIZED_SPACE_LAST_PAGE_INDEX_POS] = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    // entries
    auto entriesPointer = basePointer.subspan(Offsets::CHAINS_BEGIN_POS, numberOfChains - 1);
    std::ranges::fill(entriesPointer, 0);
}

std::unique_ptr<ChainedHashMap> ChainedHashMap::createNewMapWithSameConfiguration(AbstractBufferProvider* bufferProvider, const ChainedHashMap& other)
{
    return std::make_unique<ChainedHashMap>(bufferProvider, other.entrySize(), other.numberOfChains(), other.pageSize());
}

std::span<std::byte> ChainedHashMap::allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, const size_t neededSize)
{
    uint64_t varSizedStorageLastPageSize = 0;
    uint64_t varSizedStorageLastPageNumTuples = 0;
    uint64_t varSizedPagesNum = getNumberOfVarSizedPages();
    if (varSizedPagesNum > 0)
    {
        auto lastVarSizedPage = getVarSizedPage(varSizedPagesNum-1);
        varSizedStorageLastPageSize = lastVarSizedPage.getBufferSize();
        varSizedStorageLastPageNumTuples = lastVarSizedPage.getNumberOfTuples();
    }
    if (varSizedStorageLastPageSize == 0 or varSizedStorageLastPageNumTuples + neededSize >= varSizedStorageLastPageSize)
    {
        /// We allocate more space than currently necessary for the variable sized data to reduce the allocation overhead
        auto newPage = bufferProvider->getUnpooledBuffer(neededSize * Offsets::NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS);
        if (not newPage)
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for ChainedHashMap of size {}",
                std::to_string(neededSize * Offsets::NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS));
        }
        // store new var sized buffer page
        VariableSizedAccess::Index newPageIndex = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
        if (varSizedPagesNum > 0)
        {
            // append new page for var sized storage
            auto lastPage = getVarSizedPage(varSizedPagesNum-1);
            newPageIndex = lastPage.storeChildBuffer(newPage.value());
            INVARIANT(newPageIndex == NEXT_PAGE_CHILD_BUFFER_INDEX, "Could not link new varsized page to the last page in ChainedHashMap. Child buffer index not 0 as expected.");
        } else
        {
            // no pages yet, insert first page
            newPageIndex = mainBuffer.storeChildBuffer(newPage.value());
            auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
            // store at the storage space pointer of the main buffer
            indexPointer[Offsets::VARSIZED_SPACE_INDEX_POS] = newPageIndex;
        }
        // increase page count
        auto numPagesPointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
        numPagesPointer[Offsets::NUM_VARSIZED_PAGES_POS]++;
        varSizedPagesNum = getNumberOfVarSizedPages();
    }

    auto lastPage = getVarSizedPage(varSizedPagesNum-1);
    lastPage.setNumberOfTuples(lastPage.getNumberOfTuples() + neededSize);
    return lastPage.getAvailableMemoryArea().subspan(lastPage.getNumberOfTuples() - neededSize);
}

void ChainedHashMap::appendPage(AbstractBufferProvider* bufferProvider)
{
    // create and initialize new page
    auto newPage = bufferProvider->getBufferBlocking();
    INVARIANT(newPage.getBufferSize() == pageSize(), "Allocated page size {} does not match the expected page size {}",
        newPage.getBufferSize(), pageSize());
    std::ranges::fill(newPage.getAvailableMemoryArea(), std::byte{0});
    // store new page as child buffer to the main buffer and as the last page index
    VariableSizedAccess::Index newPageIndex = TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE;
    auto numPages = getNumberOfPages();
    if (getNumberOfPages() > 0)
    {
        // a page exists
        auto lastPage = getPage(getNumberOfPages()-1);
        newPageIndex = lastPage.storeChildBuffer(newPage);
        INVARIANT(newPageIndex == NEXT_PAGE_CHILD_BUFFER_INDEX, "Could not link new page to the last page in ChainedHashMap. Child buffer index not 0 as expected.");
    } else
    {
        // no pages yet, insert first page
        newPageIndex = mainBuffer.storeChildBuffer(newPage);
        auto indexPointer = mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>();
        // store at the storage space pointer of the main buffer
        indexPointer[Offsets::STORAGE_SPACE_INDEX_POS] = newPageIndex;
    }
    // increase num page count
    auto numPagesPointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
    numPagesPointer[Offsets::NUM_PAGES_POS]++;
}

AbstractHashMapEntry* ChainedHashMap::insertEntry(const HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider)
{
    /// 1. Check if we need to allocate a new page
    if (numberOfTuples() % entriesPerPage() == 0)
    {
        // create new page and add it as the last page in the linked list
        appendPage(bufferProvider);
    }

    /// 2. Finding the new entry
    const auto pageIndex = numberOfTuples() / entriesPerPage();
    INVARIANT(
        getNumberOfPages() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        getNumberOfPages());
    // get the page by iterating the linked list
    auto currPage = getPage(pageIndex);
    currPage.setNumberOfTuples(currPage.getNumberOfTuples() + 1);
    const auto entryOffsetInBuffer = (numberOfTuples() - (pageIndex * entriesPerPage())) * entrySize();

    /// 3. Inserting the new entry
    const auto entryPos = hash & mask();
    INVARIANT(entryPos <= mask(), "Invalid entry position, as pos {} is greater than mask {}", entryPos, mask());
    INVARIANT(entryPos < numberOfChains(), "Invalid entry position as pos {} is greater than capacity {}", entryPos, numberOfChains());
    auto mainBufferPointer = mainBuffer.getAvailableMemoryArea<ChainedHashMapEntry*>();
    auto* const newEntry = new (currPage.getAvailableMemoryArea().subspan(entryOffsetInBuffer).data()) ChainedHashMapEntry(hash);
    /// 4. Updating the chain and the current size
    if (mainBufferPointer[Offsets::CHAINS_BEGIN_POS + entryPos] == nullptr)
    {
        mainBufferPointer[Offsets::CHAINS_BEGIN_POS + entryPos] = newEntry;
    } else
    {
        auto *oldValue = mainBufferPointer[Offsets::CHAINS_BEGIN_POS + entryPos];
        newEntry->next = oldValue;
        mainBufferPointer[Offsets::CHAINS_BEGIN_POS + entryPos] = newEntry;
    }
    auto numTuplesPointer = mainBuffer.getAvailableMemoryArea<uint64_t>();
    numTuplesPointer[Offsets::NUM_TUPLES_POS]++;

    return newEntry;
}

const TupleBuffer ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    PRECONDITION(pageIndex < getNumberOfPages(), "Page index {} is greater than the number of pages {}", pageIndex, getNumberOfPages());
    TupleBuffer currPage = mainBuffer.loadChildBuffer(storageSpaceChildBufferIndex());
    for (uint64_t i=0; i<pageIndex; i++)
    {
        INVARIANT(currPage.getNumberOfChildBuffers() > 0, "Could not find page {} in ChainedHashMap as the linked list is broken (has no child buffer) at page {}",
            pageIndex, i);
        currPage = currPage.loadChildBuffer(NEXT_PAGE_CHILD_BUFFER_INDEX);
    }
    return currPage;
}

const TupleBuffer ChainedHashMap::getVarSizedPage(const uint64_t pageIndex) const
{
    PRECONDITION(pageIndex < getNumberOfVarSizedPages(), "Page index {} is greater than the number of varsized pages {}", pageIndex, getNumberOfVarSizedPages());
    TupleBuffer currPage = mainBuffer.loadChildBuffer(varSizedSpaceChildBufferIndex());
    for (uint64_t i=0; i<pageIndex; i++)
    {
        INVARIANT(currPage.getNumberOfChildBuffers() > 0, "Could not find page {} in ChainedHashMap as the linked list is broken (has no child buffer) at page {}",
            pageIndex, i);
        currPage = currPage.loadChildBuffer(NEXT_PAGE_CHILD_BUFFER_INDEX);
    }
    return currPage;
}

uint64_t ChainedHashMap::calculateMainBufferSize(uint64_t numberOfChains)
{
    return (Offsets::MAIN_BUFFER_UINT64_FIELDS_NUM * sizeof(uint64_t)) + // metadata
        ((numberOfChains + 1) * sizeof(ChainedHashMapEntry*)) + // entries
        (Offsets::MAIN_BUFFER_UINT32_FIELDS_NUM * sizeof(VariableSizedAccess::Index)); // storageSpaceChildBufferIndex + varSizedSpaceChildBufferIndex
}

uint64_t ChainedHashMap::numberOfTuples() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::NUM_TUPLES_POS];
}

uint64_t ChainedHashMap::getNumberOfPages() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::NUM_PAGES_POS];
}

uint64_t ChainedHashMap::getNumberOfVarSizedPages() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::NUM_VARSIZED_PAGES_POS];
}

uint64_t ChainedHashMap::numberOfChains() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::NUM_CHAINS_POS];
}

uint64_t ChainedHashMap::entrySize() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::ENTRY_SIZE_POS];
}

uint64_t ChainedHashMap::entriesPerPage() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::ENTRIES_PER_PAGE_POS];
}

uint64_t ChainedHashMap::pageSize() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::PAGE_SIZE_POS];
}

uint64_t ChainedHashMap::mask() const
{
    return mainBuffer.getAvailableMemoryArea<uint64_t>()[Offsets::MASK_POS];
}

VariableSizedAccess::Index ChainedHashMap::storageSpaceChildBufferIndex() const
{
    return mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[Offsets::STORAGE_SPACE_INDEX_POS];
}

VariableSizedAccess::Index ChainedHashMap::varSizedSpaceChildBufferIndex() const
{
    return mainBuffer.getAvailableMemoryArea<VariableSizedAccess::Index>()[Offsets::VARSIZED_SPACE_INDEX_POS];
}

ChainedHashMapEntry* ChainedHashMap::getChain(uint64_t pos) const
{
    return mainBuffer.getAvailableMemoryArea<ChainedHashMapEntry*>()[Offsets::CHAINS_BEGIN_POS + pos];
}

}
