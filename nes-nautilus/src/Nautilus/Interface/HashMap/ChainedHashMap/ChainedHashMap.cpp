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
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <span>
#include <string>

#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <absl/strings/internal/str_format/extension.h>
#include <absl/strings/str_format.h>
#include <google/protobuf/stubs/port.h>

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

ChainedHashMap::ChainedHashMap(uint64_t entrySize, const uint64_t numberOfBuckets, uint64_t pageSize)
    : numberOfTuples(0)
    , pageSize(pageSize)
    , entrySize(entrySize)
    , entriesPerPage(pageSize / entrySize)
    , numberOfChains(calcCapacity(numberOfBuckets, assumedLoadFactor))
    , entries(nullptr)
    , mask(numberOfChains - 1)
    , destructorCallBack(nullptr)
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

ChainedHashMap::ChainedHashMap(const uint64_t keySize, const uint64_t valueSize, const uint64_t numberOfBuckets, const uint64_t pageSize)
    : numberOfTuples(0)
    , pageSize(pageSize)
    , entrySize(sizeof(ChainedHashMapEntry) + keySize + valueSize)
    , entriesPerPage(pageSize / entrySize)
    , numberOfChains(calcCapacity(numberOfBuckets, assumedLoadFactor))
    , entries(nullptr)
    , mask(numberOfChains - 1)
    , destructorCallBack({})
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

std::unique_ptr<ChainedHashMap> ChainedHashMap::createNewMapWithSameConfiguration(const ChainedHashMap& other)
{
    return std::make_unique<ChainedHashMap>(other.entrySize, other.numberOfChains, other.pageSize);
}

ChainedHashMapEntry* ChainedHashMap::findChain(const HashFunction::HashValue::raw_type hash) const
{
    const auto entryStartPos = hash & mask;
    return entries[entryStartPos];
}

std::span<std::byte> ChainedHashMap::allocateSpaceForVarSized(AbstractBufferProvider* bufferProvider, const size_t neededSize)
{
    if (varSizedSpace.empty() or varSizedSpace.back().getNumberOfTuples() + neededSize >= varSizedSpace.back().getBufferSize())
    {
        /// We allocate more space than currently necessary for the variable sized data to reduce the allocation overhead
        auto varSizedBuffer = bufferProvider->getUnpooledBuffer(neededSize * NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS);
        if (not varSizedBuffer)
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for ChainedHashMap of size {}",
                std::to_string(neededSize * NUMBER_OF_PRE_ALLOCATED_VAR_SIZED_ITEMS));
        }
        varSizedSpace.emplace_back(varSizedBuffer.value());
    }

    varSizedSpace.back().setNumberOfTuples(varSizedSpace.back().getNumberOfTuples() + neededSize);
    return varSizedSpace.back().getAvailableMemoryArea().subspan(varSizedSpace.back().getNumberOfTuples() - neededSize);
}

uint64_t ChainedHashMap::getNumberOfTuples() const
{
    return numberOfTuples;
}

AbstractHashMapEntry* ChainedHashMap::insertEntry(const HashFunction::HashValue::raw_type hash, AbstractBufferProvider* bufferProvider)
{
    /// 0. Checking, if we have to set fill the entry space. This should be only done once, i.e., when the entries are still null
    if (entries == nullptr) [[unlikely]]
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
        entries = reinterpret_cast<ChainedHashMapEntry**>(entrySpace.getAvailableMemoryArea().data());
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
        std::ranges::fill(newPage.value().getAvailableMemoryArea(), std::byte{0});
        storageSpace.emplace_back(newPage.value());
    }

    /// 2. Finding the new entry
    const auto pageIndex = numberOfTuples / entriesPerPage;
    INVARIANT(
        storageSpace.size() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        storageSpace.size());
    auto& bufferStorage = storageSpace[pageIndex];
    bufferStorage.setNumberOfTuples(bufferStorage.getNumberOfTuples() + 1);
    const auto entryOffsetInBuffer = (numberOfTuples - (pageIndex * entriesPerPage)) * entrySize;

    /// 3. Inserting the new entry
    const auto entryPos = hash & mask;
    INVARIANT(entryPos <= mask, "Invalid entry position, as pos {} is greater than mask {}", entryPos, mask);
    INVARIANT(entryPos < numberOfChains, "Invalid entry position as pos {} is greater than capacity {}", entryPos, numberOfChains);
    auto* const newEntry = new (bufferStorage.getAvailableMemoryArea().subspan(entryOffsetInBuffer).data()) ChainedHashMapEntry(hash);

    /// 4. Updating the chain and the current size
    auto* const oldValue = entries[entryPos];
    newEntry->next = oldValue;
    entries[entryPos] = newEntry;
    this->numberOfTuples++;
    return newEntry;
}

const TupleBuffer& ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    PRECONDITION(pageIndex < storageSpace.size(), "Page index {} is greater than the number of pages {}", pageIndex, storageSpace.size());
    return storageSpace[pageIndex];
}

uint64_t ChainedHashMap::getNumberOfPages() const
{
    return storageSpace.size();
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
    if (entries != nullptr and destructorCallBack != nullptr)
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
}

void ChainedHashMap::serialize(std::filesystem::path path) const
{
    NES_INFO("Serializing chained hash map {}", path);
    std::ofstream out(path, std::ios::binary);
    if (!out.is_open())
    {
        NES_ERROR("Cannot open output file {}", path);
        throw CheckpointError("Cannot open output file {}", path);
    }

    ChainedHashMapHeader header{numberOfTuples, pageSize, entrySize, entriesPerPage, numberOfChains};
    out.write(reinterpret_cast<const char*>(&header), sizeof(ChainedHashMapHeader));
    static constexpr uint64_t INVALID = UINT64_MAX;
    /// Serialize entrySpace
    if (entries != nullptr)
    {
        std::unordered_map<const ChainedHashMapEntry*, std::pair<uint64_t, uint64_t>> entryMapping; /// mapping from pointer to (storageIndex, offset)
        for (uint64_t pageIdx = 0; pageIdx < storageSpace.size(); ++pageIdx)
        {
            const auto& page = storageSpace[pageIdx];
            auto pageData = page.getAvailableMemoryArea().data();

            for (uint64_t entryIdx = 0; entryIdx < entriesPerPage; ++entryIdx)
            {
                auto* entry = reinterpret_cast<const ChainedHashMapEntry*>(
                    pageData + (entryIdx * entrySize));
                if (entry->hash != 0)
                {
                    entryMapping[entry] = {pageIdx, entryIdx*entrySize};
                }
                if (entry->next != nullptr)
                {
                    NES_INFO("Found chained entry");
                }
            }
        }

        for (uint64_t i = 0; i < numberOfChains + 1; ++i)
        {
            const ChainedHashMapEntry* entry = entries[i];
            if (auto it = entryMapping.find(entry); it != entryMapping.end())
            {
                auto [pageIdx, offset ] = it->second;
                out.write(reinterpret_cast<const char*>(&pageIdx), sizeof(pageIdx));
                out.write(reinterpret_cast<const char*>(&offset), sizeof(offset));
            } else
            {

                out.write(reinterpret_cast<const char*>(&INVALID), sizeof(INVALID));
                out.write(reinterpret_cast<const char*>(&INVALID), sizeof(INVALID));
            }
        }
    } else
    {
        for (uint64_t i = 0; i < numberOfChains + 1; ++i)
        {
            out.write(reinterpret_cast<const char*>(&INVALID), sizeof(INVALID));
            out.write(reinterpret_cast<const char*>(&INVALID), sizeof(INVALID));
        }
    }

    /// Serialize storageSpace
    const auto numPages = storageSpace.size();
    out.write(reinterpret_cast<const char*>(&numPages), sizeof(numPages));
    for (const auto& storageBuffer : storageSpace)
    {
        const uint64_t storageBufferSize = storageBuffer.getBufferSize();
        out.write(reinterpret_cast<const char*>(&storageBufferSize), sizeof(uint64_t));
        out.write(storageBuffer.getAvailableMemoryArea<char>().data(), storageBuffer.getBufferSize());
    }
    out.close();
}

void ChainedHashMap::deserialize(std::filesystem::path path, AbstractBufferProvider* bufferProvider)
{
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open())
    {
        NES_ERROR("Cannot open input file {}", path);
        throw CheckpointError("Cannot open input file {}", path);
    }

    ChainedHashMapHeader header;
    in.read(reinterpret_cast<char*>(&header), sizeof(ChainedHashMapHeader));
    this->numberOfChains = header.numberOfChains;
    this->entriesPerPage = header.entriesPerPage;
    this->entrySize = header.entrySize;
    this->numberOfTuples = header.numberOfTuples;
    this->pageSize = header.pageSize;

    /// Setup Entryspace
    const auto totalSpace = (numberOfChains + 1) * sizeof(ChainedHashMapEntry*);
    //const auto entryBuffer = bufferProvider->getUnpooledBuffer(totalSpace);
    //if (not entryBuffer)
    //{
    //    throw CannotAccessBuffer("Could not allocate memory for ChainedHashMap of size {}", std::to_string(totalSpace));
    //}
    //entrySpace = entryBuffer.value();
    entries = reinterpret_cast<ChainedHashMapEntry**>(entrySpace.getAvailableMemoryArea().data());
    std::memset(static_cast<void*>(entries), 0, totalSpace);
    entries[numberOfChains] = reinterpret_cast<ChainedHashMapEntry*>(&entries[numberOfChains]);

    /// Read Mappings
    std::vector<std::pair<uint64_t, uint64_t>> entryMappings(numberOfChains + 1);
    for (uint64_t i = 0; i < numberOfChains + 1; ++i)
    {
        uint64_t pageIdx, offset;
        in.read(reinterpret_cast<char*>(&pageIdx), sizeof(pageIdx));
        in.read(reinterpret_cast<char*>(&offset), sizeof(offset));
        entryMappings[i] = {pageIdx, offset};
    }

    uint64_t numPages;
    in.read(reinterpret_cast<char*>(&numPages), sizeof(numPages));
    uint64_t storageBufferSize;
    for (uint64_t i = 0; i < numPages; ++i)
    {

        in.read(reinterpret_cast<char*>(&storageBufferSize), sizeof(storageBufferSize));
        if ( i < this->storageSpace.size())
        {
            in.read(reinterpret_cast<char*>(this->storageSpace[i].getAvailableMemoryArea().data()), storageBufferSize);
        } else
        {
            auto pageBuffer = bufferProvider->getUnpooledBuffer(pageSize);
            if (not pageBuffer)
            {
                throw CannotAllocateBuffer("Could not allocate page during deserialization");
            }
            in.read(reinterpret_cast<char*>(pageBuffer->getAvailableMemoryArea().data()), storageBufferSize);
            this->storageSpace.emplace_back(pageBuffer.value());
        }
    }

    /// Rebuild Chains
    for (uint64_t i = 0; i < numberOfChains + 1; ++i)
    {
        auto [pageIdx, offset] = entryMappings[i];
        if (pageIdx != UINT64_MAX)
        {
            INVARIANT(pageIdx < storageSpace.size(), "Cannot rebuild a chain that has a pageIdx outside of storageSpace.size()!");
            auto* entryPtr = reinterpret_cast<ChainedHashMapEntry*>(
                storageSpace[pageIdx].getAvailableMemoryArea().data() + offset
            );
            entries[i] = entryPtr;
        } else
        {
            entries[i] = nullptr;
        }
    }
    in.close();
}

}
