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
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <istream>
#include <iterator>
#include <limits>
#include <memory>
#include <new>
#include <ostream>
#include <span>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/Hash/HashFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/TupleBufferSerialization.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
namespace
{
constexpr std::array<char, 8> SerializationMagic{'N', 'E', 'S', 'C', 'H', 'M', '0', '1'};
constexpr uint64_t NoVarSizedPage = std::numeric_limits<uint64_t>::max();

struct SerializedLayout
{
    uint64_t numberOfChains;
    uint64_t entrySize;
    uint64_t bloomBytes;

    bool operator==(const SerializedLayout&) const = default;
};

struct VarSizedLocation
{
    uint64_t pageIndex;
    uint64_t offset;
};

struct VarSizedPageRange
{
    const std::byte* begin;
    const std::byte* end;
    uint64_t pageIndex;
};

template <typename T>
void writeValue(std::ostream& out, const T& value)
{
    out.write(reinterpret_cast<const char*>(&value), sizeof(T)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (!out)
    {
        throw CannotSerialize("Failed to write a chained hash map");
    }
}

template <typename T>
T readValue(std::istream& in)
{
    T value{};
    in.read(reinterpret_cast<char*>(&value), sizeof(T)); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (!in)
    {
        throw CannotDeserialize("Truncated chained hash map");
    }
    return value;
}

SerializedLayout layoutOf(const ChainedHashMapConfig& config)
{
    return {
        .numberOfChains = ChainedHashMap::calculateNumberOfChains(config.numberOfBuckets),
        .entrySize = config.entrySize,
        .bloomBytes = config.bloomFilterMemAreaSize()};
}

/// The entry stores the pointer to a var-sized field behind the field's null byte.
std::vector<uint64_t> varSizedPointerOffsets(const ChainedHashMapConfig& config)
{
    std::vector<uint64_t> offsets;
    for (const auto& fields : {std::cref(config.fieldKeys), std::cref(config.fieldValues)})
    {
        for (const auto& field : fields.get())
        {
            if (field.type.isType(DataType::Type::VARSIZED))
            {
                offsets.push_back(field.fieldOffset + (field.type.nullable ? 1 : 0));
            }
        }
    }
    return offsets;
}

/// Entries are visited in insertion order, as insertEntry() fills the pages one after the other.
template <typename Visitor>
void forEachEntry(const ChainedHashMap& map, const uint64_t entrySize, Visitor&& visitor)
{
    for (uint64_t pageIndex = 0; pageIndex < map.getNumberOfPages(); ++pageIndex)
    {
        auto page = map.getPage(pageIndex);
        auto pageMemory = page.getAvailableMemoryArea();
        for (uint64_t entryIndex = 0; entryIndex < page.getNumberOfTuples(); ++entryIndex)
        {
            visitor(pageMemory.subspan(entryIndex * entrySize, entrySize).data());
        }
    }
}

VarSizedLocation locateVarSized(const std::vector<VarSizedPageRange>& pagesByAddress, const std::byte* pointer)
{
    const auto candidate = std::ranges::upper_bound(pagesByAddress, pointer, std::less{}, &VarSizedPageRange::begin);
    if (candidate == pagesByAddress.begin())
    {
        return {.pageIndex = NoVarSizedPage, .offset = 0};
    }
    const auto& page = *std::prev(candidate);
    if (pointer >= page.end)
    {
        return {.pageIndex = NoVarSizedPage, .offset = 0};
    }
    return {.pageIndex = page.pageIndex, .offset = static_cast<uint64_t>(pointer - page.begin)};
}

uint64_t offsetOfHashInEntry()
{
    const ChainedHashMapEntry probe{0};
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    return static_cast<uint64_t>(reinterpret_cast<const std::byte*>(&probe.hash) - reinterpret_cast<const std::byte*>(&probe));
}
}

/// Taken from https://github.com/TimoKersten/db-engine-paradigms/blob/ae3286b279ad26ab294224d630d650bc2f2f3519/include/common/runtime/Hashmap.hpp#L193
/// Calculates the capacity of the hash map for the expected number of keys
/// This method assures that the capacity is a power of 2 that is greater or equal to the number of keys
uint64_t ChainedHashMap::calculateNumberOfChains(const uint64_t numberOfBuckets)
{
    constexpr auto loadFactor = ChainedHashMapConfig::assumedLoadFactor;
    static_assert(loadFactor > 0, "Load factor has to be greater than 0");
    PRECONDITION(numberOfBuckets > 0, "Number of keys {} has to be greater than 0", numberOfBuckets);

    const uint64_t numberOfZeroBits = std::countl_zero(numberOfBuckets);
    INVARIANT(
        numberOfZeroBits < 64,
        "Number of keys {} is too large for the hash map. The number of keys has to be smaller than 2^64 with numberOfZeroBits {}",
        numberOfBuckets,
        numberOfZeroBits);

    constexpr uint64_t oneAsUint64 = 1;
    const uint64_t exp = 64 - numberOfZeroBits;
    const auto capacity = (oneAsUint64 << exp);
    if (static_cast<uint64_t>(capacity * loadFactor) < numberOfBuckets)
    {
        return capacity << 1UL;
    }
    return capacity;
}

uint64_t ChainedHashMap::calculateMask(const uint64_t numberOfBuckets)
{
    return calculateNumberOfChains(numberOfBuckets) - 1;
}

uint64_t ChainedHashMap::calculateBufferSize(const uint64_t numberOfBuckets, const uint64_t bloomBytes)
{
    return sizeof(Header) + ((calculateNumberOfChains(numberOfBuckets) + 1) * sizeof(ChainedHashMapEntry*)) + bloomBytes;
}

void ChainedHashMap::init(TupleBuffer& tupleBuffer, const ChainedHashMapConfig& config)
{
    init(
        tupleBuffer IF_PRECONDITION(, config.entrySize),
        config.numberOfBuckets IF_PRECONDITION(, config.pageSize),
        config.bloomFilterMemAreaSize());
}

void ChainedHashMap::init(
    TupleBuffer& tupleBuffer IF_PRECONDITION(, const uint64_t entrySize),
    const uint64_t numberOfBuckets IF_PRECONDITION(, const uint64_t pageSize),
    const uint64_t bloomBytes)
{
    PRECONDITION(entrySize > 0, "Entry size has to be greater than 0. Entry size is set to small for entry size {}", entrySize);
    const uint64_t numberOfChains = calculateNumberOfChains(numberOfBuckets);
    PRECONDITION(
        pageSize / entrySize > 0,
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
        tupleBuffer.getBufferSize() >= calculateBufferSize(numberOfBuckets, bloomBytes),
        "Buffer of size {} is not big enough to hold the header ({} bytes) plus {} chain pointers ({} bytes each) plus {} bytes of "
        "BloomFilter bits",
        tupleBuffer.getBufferSize(),
        sizeof(Header),
        numberOfChains + 1,
        sizeof(ChainedHashMapEntry*),
        bloomBytes);

    /// Create object
    ChainedHashMap chm{tupleBuffer};

    /// Initialize header
    new (tupleBuffer.getAvailableMemoryArea<Header>().data()) Header{};

    /// Initialize chains array
    auto* chainsArray = chm.chainsBegin();
    std::fill_n(chainsArray, numberOfChains, nullptr);
    chainsArray[numberOfChains]
        = reinterpret_cast<ChainedHashMapEntry*>(&chainsArray[numberOfChains]); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

    /// Zero the inline BloomFilter bit area so no spurious bits are seen before the first add().
    /// A disabled filter yields an empty span, making this a no-op rather than a write past the buffer.
    std::ranges::fill(chm.bloomBits(numberOfChains, bloomBytes), uint64_t{0});
}

ChainedHashMap ChainedHashMap::load(const TupleBuffer& tupleBuffer)
{
    ChainedHashMap chm{tupleBuffer};
    /// The sizing is no longer stored in the buffer, so this is the only thing load() can still check: that
    /// the buffer went through init() at all. Whether the caller's config matches the one init() used is
    /// unverifiable here and has to be guaranteed by construction: every caller sizes its map from the one
    /// ChainedHashMapConfig the query compiler built for it.
    PRECONDITION(
        chm.header().status == VALID_CHM, "Invalid ChainedHashMap based on the status value in the header: {}", chm.header().status);
    return chm;
}

void ChainedHashMap::allocateNewVarSizedPage(AbstractBufferProvider* bufferProvider, const size_t neededSize)
{
    TupleBuffer newPage;
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
    if (varSizedBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
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

void ChainedHashMap::appendPage(AbstractBufferProvider* bufferProvider, const uint64_t pageSize)
{
    /// create and initialize new page
    TupleBuffer newPage;
    if (bufferProvider->getBufferSize() == pageSize)
    {
        newPage = bufferProvider->getBufferBlocking();
    }
    else
    {
        if (auto newPageUnpooled = bufferProvider->getUnpooledBuffer(pageSize))
        {
            newPage = newPageUnpooled.value();
        }
        else
        {
            throw CannotAllocateBuffer(
                "Could not allocate memory for unpooled storage space page of ChainedHashMap of size {}", std::to_string(pageSize));
        }
    }

    /// get or create storage buffer
    auto storageBufferIdx = getStorageBufferIdx();
    if (storageBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE)
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

AbstractHashMapEntry* ChainedHashMap::insertEntry(
    const HashFunction::HashValue::raw_type hash,
    AbstractBufferProvider* bufferProvider,
    const uint64_t entrySize,
    const uint64_t entriesPerPage,
    const uint64_t pageSize,
    const uint64_t mask)
{
    PRECONDITION(
        entrySize > 0 and entriesPerPage > 0,
        "Malformed ChainedHashMap sizing with entry size {} and entries per page {}",
        entrySize,
        entriesPerPage);

    /// 1. Check if we need to allocate a new page
    if (getTotalNumberOfRecords() % entriesPerPage == 0)
    {
        /// create new page and append it
        appendPage(bufferProvider, pageSize);
    }

    /// 2. Finding the new entry
    const auto pageIndex = getTotalNumberOfRecords() / entriesPerPage;
    INVARIANT(
        getNumberOfPages() > pageIndex,
        "Invalid page index {} as it is greater than the number of pages {}",
        pageIndex,
        getNumberOfPages());
    auto currPage = getPage(pageIndex);
    currPage.setNumberOfTuples(currPage.getNumberOfTuples() + 1);
    const auto entryOffsetInBuffer = (getTotalNumberOfRecords() - (pageIndex * entriesPerPage)) * entrySize;

    /// 3. Inserting the new entry
    const auto entryPos = hash & mask;
    INVARIANT(entryPos <= mask, "Invalid entry position, as pos {} is greater than mask {}", entryPos, mask);

    auto* chainsArray = chainsBegin();
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

uint64_t* ChainedHashMap::getBloomFilterMemArea(const uint64_t numberOfChains, const uint64_t bloomBytes)
{
    PRECONDITION(bloomBytes > 0, "A map without an in-map BloomFilter has no bit area to hand out");
    return bloomBits(numberOfChains, bloomBytes).data();
}

TupleBuffer ChainedHashMap::getPage(const uint64_t pageIndex) const
{
    auto storageBufferIdx = getStorageBufferIdx();
    PRECONDITION(storageBufferIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE, "Storage space not initialized during getPage()");
    auto storageBuffer = buffer.loadChildBuffer(storageBufferIdx);
    uint64_t numberOfPages = storageBuffer.getNumberOfChildBuffers();
    PRECONDITION(pageIndex < numberOfPages, "Page index {} is greater than the number of pages {}", pageIndex, numberOfPages);
    const ChildBufferIndex pageBufferIdx{static_cast<uint32_t>(pageIndex)};
    TupleBuffer page = storageBuffer.loadChildBuffer(pageBufferIdx);
    return page;
}

TupleBuffer ChainedHashMap::getVarSizedPage(const uint64_t pageIndex) const
{
    auto varSizedSpaceIdx = getVarSizedBufferIdx();
    PRECONDITION(
        varSizedSpaceIdx != TupleBuffer::INVALID_CHILD_BUFFER_INDEX_VALUE, "VarSized space not initialized during getVarSizedPage()");
    auto varSizedBuffer = buffer.loadChildBuffer(varSizedSpaceIdx);
    uint64_t numberOfPages = varSizedBuffer.getNumberOfChildBuffers();
    PRECONDITION(pageIndex < numberOfPages, "Page index {} is greater than the number of pages {}", pageIndex, numberOfPages);
    const ChildBufferIndex pageBufferIdx{static_cast<uint32_t>(pageIndex)};
    TupleBuffer page = varSizedBuffer.loadChildBuffer(pageBufferIdx);
    return page;
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

ChainedHashMapEntry* ChainedHashMap::getChain(const uint64_t pos)
{
    return chainsBegin()[pos]; /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

void ChainedHashMap::serialize(std::ostream& out, const ChainedHashMapConfig& config) const
{
    writeValue(out, SerializationMagic);
    writeValue(out, layoutOf(config));
    serializeTupleBufferTree(out, buffer);

    const auto pointerOffsets = varSizedPointerOffsets(config);
    if (pointerOffsets.empty())
    {
        return;
    }
    std::vector<TupleBuffer> varSizedPages;
    std::vector<VarSizedPageRange> pagesByAddress;
    for (uint64_t pageIndex = 0; pageIndex < getNumberOfVarSizedPages(); ++pageIndex)
    {
        const auto& page = varSizedPages.emplace_back(getVarSizedPage(pageIndex));
        const auto pageMemory = page.getAvailableMemoryArea();
        pagesByAddress.push_back({.begin = pageMemory.data(), .end = pageMemory.data() + pageMemory.size(), .pageIndex = pageIndex});
    }
    std::ranges::sort(pagesByAddress, std::less{}, &VarSizedPageRange::begin);

    forEachEntry(
        *this,
        config.entrySize,
        [&](const std::byte* entry)
        {
            for (const auto pointerOffset : pointerOffsets)
            {
                const std::byte* pointer = nullptr;
                std::memcpy(static_cast<void*>(&pointer), entry + pointerOffset, sizeof(pointer));
                writeValue(out, locateVarSized(pagesByAddress, pointer));
            }
        });
}

TupleBuffer ChainedHashMap::deserialize(std::istream& in, const ChainedHashMapConfig& config, AbstractBufferProvider& bufferProvider)
{
    if (readValue<std::array<char, SerializationMagic.size()>>(in) != SerializationMagic)
    {
        throw CannotDeserialize("Unknown chained hash map format");
    }
    const auto expectedLayout = layoutOf(config);
    if (const auto layout = readValue<SerializedLayout>(in); layout != expectedLayout)
    {
        throw CannotDeserialize(
            "Chained hash map was written with {} chains, entry size {} and {} bloom filter bytes, but the config expects {}, {} and {}",
            layout.numberOfChains,
            layout.entrySize,
            layout.bloomBytes,
            expectedLayout.numberOfChains,
            expectedLayout.entrySize,
            expectedLayout.bloomBytes);
    }

    auto tupleBuffer = deserializeTupleBufferTree(in, bufferProvider);
    if (tupleBuffer.getBufferSize() < calculateBufferSize(config.numberOfBuckets, expectedLayout.bloomBytes))
    {
        throw CannotDeserialize("Chained hash map buffer of {} bytes is too small for its config", tupleBuffer.getBufferSize());
    }
    ChainedHashMap map{tupleBuffer};
    if (map.header().status != VALID_CHM)
    {
        throw CannotDeserialize("Invalid chained hash map status {}", map.header().status);
    }

    auto* chains = map.chainsBegin();
    std::fill_n(chains, expectedLayout.numberOfChains, nullptr);
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-pointer-arithmetic)
    chains[expectedLayout.numberOfChains] = reinterpret_cast<ChainedHashMapEntry*>(&chains[expectedLayout.numberOfChains]);

    const auto hashOffset = offsetOfHashInEntry();
    const auto mask = expectedLayout.numberOfChains - 1;
    uint64_t numberOfEntries = 0;
    forEachEntry(
        map,
        config.entrySize,
        [&](std::byte* entryMemory)
        {
            HashFunction::HashValue::raw_type hash{};
            std::memcpy(&hash, entryMemory + hashOffset, sizeof(hash));
            auto* entry = new (entryMemory) ChainedHashMapEntry(hash);
            auto& chain = chains[hash & mask]; /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
            entry->next = chain;
            chain = entry;
            ++numberOfEntries;
        });
    if (numberOfEntries != map.header().numRecords)
    {
        throw CannotDeserialize(
            "Chained hash map pages hold {} entries, but its header expects {}", numberOfEntries, map.header().numRecords);
    }

    const auto pointerOffsets = varSizedPointerOffsets(config);
    if (pointerOffsets.empty())
    {
        return tupleBuffer;
    }
    std::vector<TupleBuffer> varSizedPages;
    for (uint64_t pageIndex = 0; pageIndex < map.getNumberOfVarSizedPages(); ++pageIndex)
    {
        varSizedPages.emplace_back(map.getVarSizedPage(pageIndex));
    }
    forEachEntry(
        map,
        config.entrySize,
        [&](std::byte* entry)
        {
            for (const auto pointerOffset : pointerOffsets)
            {
                const auto location = readValue<VarSizedLocation>(in);
                std::byte* pointer = nullptr;
                if (location.pageIndex != NoVarSizedPage)
                {
                    if (location.pageIndex >= varSizedPages.size() || location.offset >= varSizedPages[location.pageIndex].getBufferSize())
                    {
                        throw CannotDeserialize("Var-sized field points outside of the chained hash map's var-sized pages");
                    }
                    pointer = varSizedPages[location.pageIndex].getAvailableMemoryArea().subspan(location.offset).data();
                }
                std::memcpy(entry + pointerOffset, static_cast<const void*>(&pointer), sizeof(pointer));
            }
        });
    return tupleBuffer;
}

}
