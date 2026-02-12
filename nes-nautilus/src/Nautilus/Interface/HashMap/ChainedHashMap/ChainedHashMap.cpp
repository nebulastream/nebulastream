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
constexpr auto assumedLoadFactor = 0.75;

/// Taken from https://github.com/TimoKersten/db-engine-paradigms/blob/ae3286b279ad26ab294224d630d650bc2f2f3519/include/common/runtime/Hashmap.hpp#L193
/// Calculates the capacity of the hash map for the expected number of keys
/// This method assures that the capacity is a power of 2 that is greater or equal to the number of keys
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

ChainedHashMap ChainedHashMap::init(TupleBuffer& tupleBuffer, uint64_t entrySize, const uint64_t numberOfBuckets, uint64_t pageSize)
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

    /// Create object
    ChainedHashMap chm(tupleBuffer);

    /// Initialize header
    new (tupleBuffer.getAvailableMemoryArea<Header>().data())
        Header(numberOfBuckets, numberOfChains, pageSize, entrySize, entriesPerPage, numberOfChains - 1);

    /// Initialize chains array
    auto* chainsArray = chm.chains();
    std::fill_n(chainsArray, numberOfChains + 1, 0ULL);
    chainsArray[numberOfChains] = reinterpret_cast<uint64_t>(&chainsArray[numberOfChains]);

    return chm;
}

ChainedHashMap ChainedHashMap::init(
    TupleBuffer& tupleBuffer, const uint64_t keySize, const uint64_t valueSize, const uint64_t numberOfBuckets, const uint64_t pageSize)
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

    /// Create object
    ChainedHashMap chm(tupleBuffer);

    /// Initialize its header
    new (tupleBuffer.getAvailableMemoryArea<Header>().data())
        Header(numberOfBuckets, numberOfChains, pageSize, entrySize, entriesPerPage, numberOfChains - 1);

    /// Initialize chains array
    auto* chainsArray = chm.chains();
    std::fill_n(chainsArray, numberOfChains + 1, 0ULL);
    chainsArray[numberOfChains] = reinterpret_cast<uint64_t>(&chainsArray[numberOfChains]);

    return chm;
}

ChainedHashMap ChainedHashMap::load(TupleBuffer& tupleBuffer)
{
    ChainedHashMap chm(tupleBuffer);
    const auto& hdr = chm.header();
    PRECONDITION(hdr.entrySize > 0, "Entry size has to be greater than 0. Entry size is set to small for entry size {}", hdr.entrySize);
    PRECONDITION(
        hdr.entriesPerPage > 0,
        "At least one entry has to fit on a page. Pagesize is set to small for pageSize {} and entry size {}",
        hdr.pageSize,
        hdr.entrySize);
    PRECONDITION(
        hdr.numChains > 0,
        "Number of chains has to be greater than 0. Number of chains is set to small for number of chains {}",
        hdr.numChains);
    PRECONDITION(
        (hdr.numChains & (hdr.numChains - 1)) == 0,
        "Number of chains has to be a power of 2. Number of chains is set to small for number of chains {}",
        hdr.numChains);
    return chm;
}

void ChainedHashMap::allocateNewVarSizedPage(AbstractBufferProvider* bufferProvider, const size_t neededSize) const
{
    auto newPage = bufferProvider->getUnpooledBuffer(neededSize * NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS);
    if (!newPage)
    {
        throw CannotAllocateBuffer(
            "Could not allocate memory for ChainedHashMap of size {}",
            std::to_string(neededSize * NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS));
    }

    auto varSizedBufferIdx = getVarSizedBufferIdx();
    if (varSizedBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        /// Storage buffer already exists - just add the new page
        auto varSizedBuffer = buffer.loadChildBuffer(varSizedBufferIdx);
        auto tmp = varSizedBuffer.storeChildBuffer(
            newPage.value()); ///NOLINT: storeChildBuffer is [[nodiscard]] but no use for child buffer index at this point.
    }
    else
    {
        /// Create new storage buffer and add the page
        auto newVarSizedBuffer = bufferProvider->getUnpooledBuffer(4);
        if (!newVarSizedBuffer)
        {
            throw CannotAllocateBuffer("Could not allocate memory for storage space of ChainedHashMap of size {}", std::to_string(0));
        }

        auto varSizedBuffer = std::move(newVarSizedBuffer.value());
        auto tmp = varSizedBuffer.storeChildBuffer(newPage.value());
        header().varSizedSpaceIndex = buffer.storeChildBuffer(varSizedBuffer);
    }
}

std::span<std::byte> ChainedHashMap::allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, const size_t neededSize) const
{
    const uint64_t varSizedPagesNum = getNumberOfVarSizedPages();

    /// Check if we need to allocate a new page
    bool needNewPage = (varSizedPagesNum == 0);
    if (!needNewPage)
    {
        auto lastPage = getVarSizedPage(varSizedPagesNum - 1);
        const uint64_t lastPageSize = lastPage.getBufferSize();
        const uint64_t lastPageNumTuples = lastPage.getNumberOfTuples();
        needNewPage = (lastPageNumTuples + neededSize >= lastPageSize);
    }

    if (needNewPage)
    {
        allocateNewVarSizedPage(bufferProvider, neededSize);
    }

    /// Get the last page (either existing or newly created)
    auto lastPage = getVarSizedPage(getNumberOfVarSizedPages() - 1);
    const uint64_t allocationOffset = lastPage.getNumberOfTuples();
    lastPage.setNumberOfTuples(allocationOffset + neededSize);
    return lastPage.getAvailableMemoryArea().subspan(allocationOffset);
}

void ChainedHashMap::appendPage(AbstractBufferProvider* bufferProvider) const
{
    /// create and initialize new page
    TupleBuffer newPage;
    if (bufferProvider->getBufferSize() == pageSize())
    {
        newPage = bufferProvider->getBufferBlocking();
    }
    else
    {
        if (auto newPageUnpooled = bufferProvider->getUnpooledBuffer(pageSize()))
        {
            newPage = newPageUnpooled.value();
        }
        else
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for unpooled storage space page of ChainedHashMap of size {}", std::to_string(pageSize()));
        }
    }
    INVARIANT(
        newPage.getBufferSize() == pageSize(),
        "Allocated page size {} does not match the expected page size {}",
        newPage.getBufferSize(),
        pageSize());
    std::ranges::fill(newPage.getAvailableMemoryArea(), std::byte{0});

    /// get or create storage buffer
    auto storageBufferIdx = getStorageBufferIdx();
    if (storageBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        /// storage buffer already exists
        auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
        auto newPageIdx = storageBuffer.storeChildBuffer(
            newPage); ///NOLINT: storeChildBuffer is [[nodiscard]] but no use for child buffer index at this point.
    }
    else
    {
        /// create new storage buffer (of minimal size)
        auto newStorageBuffer = bufferProvider->getUnpooledBuffer(4);
        if (not newStorageBuffer)
        {
            throw CannotAllocateBuffer("Could not allocate memory for storage space of ChainedHashMap of size {}", std::to_string(4));
        }
        auto storageBuffer = std::move(newStorageBuffer.value());
        auto newPageIdx = storageBuffer.storeChildBuffer(
            newPage); ///NOLINT: storeChildBuffer is [[nodiscard]] but no use for child buffer index at this point.
        header().storageSpaceIndex = buffer.storeChildBuffer(storageBuffer);
    }
}

AbstractHashMapEntry* ChainedHashMap::insertEntry(const HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider)
{
    /// 1. Check if we need to allocate a new page
    if (numberOfTuples() % entriesPerPage() == 0)
    {
        /// create new page and append it
        appendPage(bufferProvider);
    }

    /// 2. Finding the new entry
    const auto pageIndex = numberOfTuples() / entriesPerPage();
    INVARIANT(
        getNumberOfPages() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        getNumberOfPages());
    auto currPage = getPage(pageIndex);
    currPage.setNumberOfTuples(currPage.getNumberOfTuples() + 1);
    const auto entryOffsetInBuffer = (numberOfTuples() - (pageIndex * entriesPerPage())) * entrySize();

    /// 3. Inserting the new entry
    const auto entryPos = hash & mask();
    INVARIANT(entryPos <= mask(), "Invalid entry position, as pos {} is greater than mask {}", entryPos, mask());
    INVARIANT(entryPos < numberOfChains(), "Invalid entry position as pos {} is greater than capacity {}", entryPos, numberOfChains());

    auto* chainsArray = chains();
    auto* const newEntry = new (currPage.getAvailableMemoryArea().subspan(entryOffsetInBuffer).data()) ChainedHashMapEntry(hash);

    /// 4. Updating the chain and the current size
    auto* chainPtr = reinterpret_cast<ChainedHashMapEntry**>(&chainsArray[entryPos]);
    if (*chainPtr == nullptr)
    {
        *chainPtr = newEntry;
    }
    else
    {
        auto* oldValue = *chainPtr;
        newEntry->next = oldValue;
        *chainPtr = newEntry;
    }
    header().numTuples++;

    return newEntry;
}

[[nodiscard]] VariableSizedAccess::Index ChainedHashMap::getStorageBufferIdx() const
{
    return header().storageSpaceIndex;
}

[[nodiscard]] VariableSizedAccess::Index ChainedHashMap::getVarSizedBufferIdx() const
{
    return header().varSizedSpaceIndex;
}

const TupleBuffer ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    auto storageBufferIdx = getStorageBufferIdx();
    PRECONDITION(storageBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE, "Storage space not initialized during getPage()");
    auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
    uint64_t numberOfPages = storageBuffer.getNumberOfChildBuffers();
    PRECONDITION(pageIndex < numberOfPages, "Page index {} is greater than the number of pages {}", pageIndex, numberOfPages);
    VariableSizedAccess::Index pageBufferIdx{pageIndex};
    TupleBuffer page = storageBuffer.loadChildBuffer(pageBufferIdx);
    return page;
}

const TupleBuffer ChainedHashMap::getVarSizedPage(const uint64_t pageIndex) const
{
    auto varSizedSpaceIdx = getVarSizedBufferIdx();
    PRECONDITION(
        varSizedSpaceIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE, "VarSized space not initialized during getVarSizedPage()");
    auto varSizedBuffer = buffer.loadChildBuffer(varSizedSpaceIdx);
    uint64_t numberOfPages = varSizedBuffer.getNumberOfChildBuffers();
    PRECONDITION(pageIndex < numberOfPages, "Page index {} is greater than the number of pages {}", pageIndex, numberOfPages);
    VariableSizedAccess::Index pageBufferIdx{pageIndex};
    TupleBuffer page = varSizedBuffer.loadChildBuffer(pageBufferIdx);
    return page;
}

uint64_t ChainedHashMap::calculateBufferSizeFromBuckets(uint64_t numberOfBuckets)
{
    const uint64_t numberOfChains = calcCapacity(numberOfBuckets, assumedLoadFactor);
    return sizeof(Header) + ((numberOfChains + 1) * sizeof(uint64_t));
}

uint64_t ChainedHashMap::calculateBufferSizeFromChains(uint64_t numberOfChains)
{
    return sizeof(Header) + ((numberOfChains + 1) * sizeof(uint64_t));
}

[[nodiscard]] uint64_t ChainedHashMap::getNumberOfPages() const
{
    auto storageBufferIdx = getStorageBufferIdx();
    if (storageBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
        return storageBuffer.getNumberOfChildBuffers();
    }
    return 0;
}

[[nodiscard]] uint64_t ChainedHashMap::getNumberOfVarSizedPages() const
{
    auto varSizedBufferIdx = getVarSizedBufferIdx();
    if (varSizedBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        auto varSizedBuffer = buffer.loadChildBuffer(varSizedBufferIdx);
        return varSizedBuffer.getNumberOfChildBuffers();
    }
    return 0;
}

ChainedHashMapEntry* ChainedHashMap::getChain(uint64_t pos) const
{
    const auto* chainsArray = chains();
    return reinterpret_cast<ChainedHashMapEntry*>(chainsArray[pos]);
}

}
