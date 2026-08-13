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
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>

#include <algorithm>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <tuple>
#include <utility>

#include <Interface/Hash/HashFunction.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Buffer.hpp>
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

uint64_t ChainedHashMapConfig::bufferSize() const
{
    return ChainedHashMap::calculateBufferSizeFromBuckets(numberOfBuckets, bloomFilterMemAreaSize());
}

void ChainedHashMap::init(Buffer& tupleBuffer, const ChainedHashMapConfig& config)
{
    const auto [entrySize, numberOfBuckets, pageSize, bloomFilter] = config;
    PRECONDITION(entrySize > 0, "Entry size has to be greater than 0. Entry size is set to small for entry size {}", entrySize);
    const uint64_t entriesPerPage = pageSize / entrySize;
    const uint64_t numberOfChains = calcCapacity(numberOfBuckets, assumedLoadFactor);
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
    PRECONDITION(
        tupleBuffer.getBufferSize() >= calculateBufferSizeFromChains(numberOfChains, config.bloomFilterMemAreaSize()),
        "Buffer of size {} is not big enough to hold the header ({} bytes) plus {} chain pointers ({} bytes each) plus {} bytes of "
        "BloomFilter bits",
        tupleBuffer.getBufferSize(),
        sizeof(Header),
        numberOfChains + 1,
        sizeof(ChainedHashMapEntry*),
        config.bloomFilterMemAreaSize());

    /// Create object
    ChainedHashMap chm{tupleBuffer};

    /// Initialize header
    new (tupleBuffer.getAvailableMemoryArea<Header>().data())
        Header{numberOfBuckets, numberOfChains, pageSize, entrySize, entriesPerPage, numberOfChains - 1, bloomFilter};

    /// Initialize chains array
    auto chainsArray = chm.chains();
    std::ranges::fill(chainsArray, nullptr);
    chainsArray[numberOfChains]
        = reinterpret_cast<ChainedHashMapEntry*>(&chainsArray[numberOfChains]); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

    /// Zero the inline BloomFilter bit area so no spurious bits are seen before the first add().
    std::ranges::fill(chm.bloomBits(), uint64_t{0});
}

ChainedHashMap ChainedHashMap::load(const Buffer& tupleBuffer)
{
    ChainedHashMap chm{tupleBuffer};
    const auto& hdr = chm.header();
    PRECONDITION(hdr.status == VALID_CHM, "Invalid ChainedHashMap based on the status value in the header: {}", hdr.status);
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

void ChainedHashMap::allocateNewVarSizedPage(AbstractBufferProvider* bufferProvider, const size_t neededSize)
{
    Buffer newPage;
    if (neededSize <= bufferProvider->getBufferSize())
    {
        newPage = bufferProvider->getBufferBlocking();
    }
    else
    {
        /// The varsized entry does not fit into a pooled buffer, so we allocate an unpooled buffer sized to fit it.
        auto newPageUnpooled = bufferProvider->getUnpooledBuffer(neededSize);
        if (!newPageUnpooled)
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for unpooled VARSIZED data page of ChainedHashMap of size {}", std::to_string(neededSize));
        }
        newPage = newPageUnpooled.value();
    }

    auto varSizedBufferIdx = getVarSizedBufferIdx();
    if (varSizedBufferIdx != Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        /// Storage buffer already exists - just add the new page
        auto varSizedBuffer = buffer.loadChildBuffer(varSizedBufferIdx);
        std::ignore = varSizedBuffer.storeChildBuffer(
            newPage); ///NOLINT: storeChildBuffer is [[nodiscard]] but no use for child buffer index at this point.
    }
    else
    {
        /// Create new storage buffer and add the page
        auto newVarSizedBuffer = bufferProvider->getUnpooledBuffer(VARSIZED_STORAGE_SPACE_BUFFER_SIZE);
        if (!newVarSizedBuffer)
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for storage space of ChainedHashMap of size {}",
                std::to_string(VARSIZED_STORAGE_SPACE_BUFFER_SIZE));
        }

        auto varSizedBuffer = std::move(newVarSizedBuffer.value());
        std::ignore = varSizedBuffer.storeChildBuffer(newPage);
        header().varSizedSpaceIndex = buffer.storeChildBuffer(varSizedBuffer);
    }
}

std::span<std::byte> ChainedHashMap::allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, const size_t neededSize)
{
    const uint64_t varSizedPagesNum = getNumberOfVarSizedPages();

    /// Check if we need to allocate a new page
    bool needNewPage = (varSizedPagesNum == 0);
    if (!needNewPage)
    {
        auto lastPage = getVarSizedPage(varSizedPagesNum - 1);
        const uint64_t lastPageSize = lastPage.getBufferSize();
        const uint64_t lastPageNumTuples = lastPage.getNumberOfTuples();
        needNewPage = (lastPageNumTuples + neededSize > lastPageSize);
    }

    if (needNewPage)
    {
        allocateNewVarSizedPage(bufferProvider, neededSize);
    }

    /// Get the last page (either existing or newly created)
    const uint64_t pageIndex = getNumberOfVarSizedPages() - 1;
    auto lastPage = getVarSizedPage(pageIndex);
    const uint64_t allocationOffset = lastPage.getNumberOfTuples();
    lastPage.setNumberOfTuples(allocationOffset + neededSize);
    return lastPage.getAvailableMemoryArea().subspan(allocationOffset);
}

void ChainedHashMap::appendPage(AbstractBufferProvider* bufferProvider)
{
    /// create and initialize new page
    Buffer newPage;
    if (bufferProvider->getBufferSize() == getPageSize())
    {
        newPage = bufferProvider->getBufferBlocking();
    }
    else
    {
        if (auto newPageUnpooled = bufferProvider->getUnpooledBuffer(getPageSize()))
        {
            newPage = newPageUnpooled.value();
        }
        else
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for unpooled storage space page of ChainedHashMap of size {}", std::to_string(getPageSize()));
        }
    }

    /// get or create storage buffer
    auto storageBufferIdx = getStorageBufferIdx();
    if (storageBufferIdx != Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        /// storage buffer already exists
        auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
        std::ignore = storageBuffer.storeChildBuffer(
            newPage); ///NOLINT: storeChildBuffer is [[nodiscard]] but no use for child buffer index at this point.
    }
    else
    {
        /// create new storage buffer (of minimal size)
        auto newStorageBuffer = bufferProvider->getUnpooledBuffer(FIXED_STORAGE_SPACE_BUFFER_SIZE);
        if (not newStorageBuffer)
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for storage space of ChainedHashMap of size {}",
                std::to_string(FIXED_STORAGE_SPACE_BUFFER_SIZE));
        }
        auto storageBuffer = std::move(newStorageBuffer.value());
        std::ignore = storageBuffer.storeChildBuffer(
            newPage); ///NOLINT: storeChildBuffer is [[nodiscard]] but no use for child buffer index at this point.
        header().storageSpaceIndex = buffer.storeChildBuffer(storageBuffer);
    }
}

AbstractHashMapEntry* ChainedHashMap::insertEntry(const HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider)
{
    /// 1. Check if we need to allocate a new page
    if (getTotalNumberOfRecords() % getEntriesPerPage() == 0)
    {
        /// create new page and append it
        appendPage(bufferProvider);
    }

    /// 2. Finding the new entry
    const auto pageIndex = getTotalNumberOfRecords() / getEntriesPerPage();
    INVARIANT(
        getNumberOfPages() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        getNumberOfPages());
    auto currPage = getPage(pageIndex);
    currPage.setNumberOfTuples(currPage.getNumberOfTuples() + 1);
    const auto entryOffsetInBuffer = (getTotalNumberOfRecords() - (pageIndex * getEntriesPerPage())) * getEntrySize();

    /// 3. Inserting the new entry
    const auto entryPos = hash & getMask();
    INVARIANT(entryPos <= getMask(), "Invalid entry position, as pos {} is greater than mask {}", entryPos, getMask());
    INVARIANT(
        entryPos < getNumberOfChains(), "Invalid entry position as pos {} is greater than capacity {}", entryPos, getNumberOfChains());

    auto chainsArray = chains();
    auto* const newEntry = new (currPage.getAvailableMemoryArea().subspan(entryOffsetInBuffer).data()) ChainedHashMapEntry(hash);

    /// 4. Updating the chain and the current size
    auto& chainPtr = chainsArray[entryPos];
    if (chainPtr == nullptr)
    {
        chainPtr = newEntry;
    }
    else
    {
        newEntry->next = chainPtr;
        chainPtr = newEntry;
    }
    header().numRecords++;

    return newEntry;
}

[[nodiscard]] ChildBufferIndex ChainedHashMap::getStorageBufferIdx() const
{
    return header().storageSpaceIndex;
}

[[nodiscard]] ChildBufferIndex ChainedHashMap::getVarSizedBufferIdx() const
{
    return header().varSizedSpaceIndex;
}

uint64_t* ChainedHashMap::getBloomFilterMemArea()
{
    return bloomBits().data();
}

Buffer ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    auto storageBufferIdx = getStorageBufferIdx();
    PRECONDITION(storageBufferIdx != Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE, "Storage space not initialized during getPage()");
    auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
    uint64_t numberOfPages = storageBuffer.getNumberOfChildBuffers();
    PRECONDITION(pageIndex < numberOfPages, "Page index {} is greater than the number of pages {}", pageIndex, numberOfPages);
    const ChildBufferIndex pageBufferIdx{static_cast<uint32_t>(pageIndex)};
    Buffer page = storageBuffer.loadChildBuffer(pageBufferIdx);
    return page;
}

Buffer ChainedHashMap::getVarSizedPage(const uint64_t pageIndex) const
{
    auto varSizedSpaceIdx = getVarSizedBufferIdx();
    PRECONDITION(varSizedSpaceIdx != Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE, "VarSized space not initialized during getVarSizedPage()");
    auto varSizedBuffer = buffer.loadChildBuffer(varSizedSpaceIdx);
    uint64_t numberOfPages = varSizedBuffer.getNumberOfChildBuffers();
    PRECONDITION(pageIndex < numberOfPages, "Page index {} is greater than the number of pages {}", pageIndex, numberOfPages);
    const ChildBufferIndex pageBufferIdx{static_cast<uint32_t>(pageIndex)};
    Buffer page = varSizedBuffer.loadChildBuffer(pageBufferIdx);
    return page;
}

uint64_t ChainedHashMap::calculateBufferSizeFromBuckets(uint64_t numberOfBuckets, uint64_t bloomFilterMemAreaSize)
{
    const uint64_t numberOfChains = calcCapacity(numberOfBuckets, assumedLoadFactor);
    return calculateBufferSizeFromChains(numberOfChains, bloomFilterMemAreaSize);
}

uint64_t ChainedHashMap::calculateBufferSizeFromChains(uint64_t numberOfChains, uint64_t bloomFilterMemAreaSize)
{
    return sizeof(Header) + ((numberOfChains + 1) * sizeof(ChainedHashMapEntry*)) + bloomFilterMemAreaSize;
}

[[nodiscard]] uint64_t ChainedHashMap::getNumberOfPages() const
{
    auto storageBufferIdx = getStorageBufferIdx();
    if (storageBufferIdx != Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
        return storageBuffer.getNumberOfChildBuffers();
    }
    return 0;
}

[[nodiscard]] uint64_t ChainedHashMap::getNumberOfVarSizedPages() const
{
    auto varSizedBufferIdx = getVarSizedBufferIdx();
    if (varSizedBufferIdx != Buffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
    {
        auto varSizedBuffer = buffer.loadChildBuffer(varSizedBufferIdx);
        return varSizedBuffer.getNumberOfChildBuffers();
    }
    return 0;
}

ChainedHashMapEntry* ChainedHashMap::getChain(uint64_t pos)
{
    auto chainsArray = chains();
    return chainsArray[pos];
}

}
