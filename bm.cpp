//
// Created by Lukas Schwerdtfeger on 07.07.26.
//


#include <algorithm>
#include <bit>
#include <cassert>
#include <format>
#include <iostream>
#include <mdspan>
#include <memory>
#include <optional>
#include <ostream>
#include <print>
#include <random>
#include <ranges>
#include <span>
#include <stack>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <fstream>
#include <numbers>
#include <valarray>

#include <sys/mman.h>

int overcommit_mode()
{
    std::ifstream f("/proc/sys/vm/overcommit_memory");
    int mode = -1;
    f >> mode;
    return mode;
}

struct Zone;

struct ControlBlock
{
    Zone* zone = nullptr;
    size_t refCount = 0;

    ControlBlock() = default;
    ControlBlock(const ControlBlock& other) = delete;
    ControlBlock(ControlBlock&& other) noexcept = delete;
    ControlBlock& operator=(const ControlBlock& other) = delete;
    ControlBlock& operator=(ControlBlock&& other) noexcept = delete;

    void reduce_ref();

    void increase_ref() { refCount++; }
};

struct Handle
{
    ControlBlock* block;
    std::span<std::byte> page;

    ~Handle()
    {
        if (block)
        {
            block->reduce_ref();
        }
    }

    explicit Handle(ControlBlock* block, std::span<std::byte> page) : block(block), page(page) { block->increase_ref(); }

    Handle(const Handle& other) : block(other.block), page(other.page) { block->increase_ref(); }

    Handle(Handle&& other) noexcept : block(other.block), page(std::move(other.page)) { other.block = nullptr; }

    Handle& operator=(const Handle& other)
    {
        if (this == &other)
            return *this;
        block = other.block;
        page = other.page;
        block->increase_ref();
        return *this;
    }

    Handle& operator=(Handle&& other) noexcept
    {
        if (this == &other)
            return *this;
        other.block = nullptr;
        block = other.block;
        page = std::move(other.page);
        return *this;
    }
};

struct BitMap
{
    std::unique_ptr<uint64_t[]> memory;
    size_t numberOfBits;
    size_t popCount_ = 0;

    explicit BitMap(size_t numberOfBits) : memory(std::make_unique<uint64_t[]>((numberOfBits + 63) / 64)), numberOfBits(numberOfBits) { }

    bool check(size_t index) const
    {
        assert(index < numberOfBits);
        return (memory[index / 64] & uint64_t{1} << (index % 64)) != 0;
    }

    size_t popCount() const { return popCount_; }

    void set(size_t index)
    {
        assert(index < numberOfBits);
        if (!check(index))
        {
            popCount_++;
        }

        memory[index / 64] |= uint64_t{1} << (index % 64);
    }

    void unset(size_t index)
    {
        assert(index < numberOfBits);
        if (check(index))
        {
            popCount_--;
        }

        memory[index / 64] &= ~(uint64_t{1} << (index % 64));
    }

    /// First maximal run of consecutive ones, as half-open [start, end).
    /// Returns nullopt if no bit is set.
    std::optional<std::pair<size_t, size_t>> findFirstRun(BitMap& other) const
    {
        const size_t numWords = (numberOfBits + 63) / 64;

        auto word = [&](size_t wordIndex) { return memory[wordIndex] & ~(other.memory[wordIndex]); };

        // 1. Skip all-zero words to find the first set bit.
        size_t w = 0;
        while (w < numWords && word(w) == 0)
            ++w;
        if (w == numWords)
            return std::nullopt;

        const size_t offset = std::countr_zero(word(w));
        const size_t start = w * 64 + offset;

        // 2. Count consecutive ones starting at `start` within this word.
        const size_t ones = std::countr_one(word(w) >> offset);
        if (ones < 64 - offset) // run ends inside this word
            return std::pair{start, start + ones};

        // 3. Run hits the word boundary: consume all-ones words, then the tail.
        size_t end = (w + 1) * 64;
        ++w;
        while (w < numWords && word(w) == ~uint64_t{0})
        {
            end += 64;
            ++w;
        }
        if (w < numWords)
            end += std::countr_one(word(0));

        return std::pair{start, std::min(end, numberOfBits)};
    }

    std::optional<std::pair<size_t, size_t>> findLastRun(BitMap& other) const
    {
        const size_t numWords = (numberOfBits + 63) / 64;
        auto word = [&](size_t wordIndex) { return memory[wordIndex] & ~(other.memory[wordIndex]); };

        // Iterate from the last block to the first
        for (int64_t i = static_cast<int64_t>(numWords) - 1; i >= 0; --i)
        {
            if (word(i) == 0)
                continue;

            // Found a block with at least one '1'
            // Find the index of the most significant bit (MSB)
            // std::countl_zero is available in <bit> (C++20)
            int msb_pos = 63 - std::countl_zero(word(i));

            // Find the length of the run starting at that MSB
            // Shift right to bring the MSB to the lowest position
            uint64_t shifted = word(i) >> (msb_pos - 0); // Logic adjustment for run length

            // Count consecutive ones starting from the MSB
            // We invert the bits and look for leading zeros to find the run length
            int run_length = std::countl_one(word(i) << (63 - msb_pos));

            size_t end = (i * 64) + msb_pos + 1;
            size_t start = end - run_length;

            return std::make_optional<std::pair<size_t, size_t>>(start, end);
        }

        return std::nullopt;
    }

    std::optional<size_t> findFirstAllocatedButNotInUse(BitMap& inUse) const
    {
        const size_t numWords = (numberOfBits + 63) / 64;
        auto word = [&](size_t wordIndex) { return memory[wordIndex] & ~(inUse.memory[wordIndex]); };

        size_t w = 0;
        while (w < numWords && word(w) == 0)
            ++w;
        if (w == numWords)
            return std::nullopt;

        const size_t offset = std::countr_zero(word(w));
        return w * 64 + offset;
    }

    std::optional<size_t> firstZero() const
    {
        const size_t numWords = (numberOfBits + 63) / 64;
        auto word = [&](size_t wordIndex) { return ~memory[wordIndex]; };

        size_t w = 0;
        while (w < numWords && word(w) == 0)
            ++w;

        const size_t offset = std::countr_zero(word(w));
        return w * 64 + offset;
    }
};

struct Zone
{
    std::span<std::byte> pages;
    std::unique_ptr<ControlBlock[]> controlBlocks;
    BitMap allocations;
    BitMap inUse;
    size_t pageSize;

    size_t toIndex(std::span<std::byte> page)
    {
        assert(page.size() == pageSize);
        return (page.data() - pages.data()) / pageSize;
    }

    Zone(size_t totalMemoryBudget, size_t pageSize)
        : allocations(totalMemoryBudget / pageSize), inUse(totalMemoryBudget / pageSize), pageSize(pageSize)
    {
        auto allocationSize = (totalMemoryBudget / pageSize) * pageSize;
        auto allocation = ::mmap(nullptr, allocationSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        pages = std::span{static_cast<std::byte*>(allocation), allocationSize};
        madvise(pages.data(), pages.size(), MADV_DONTNEED);
        auto numberOfPages = totalMemoryBudget / pageSize;
        controlBlocks = std::make_unique<ControlBlock[]>(numberOfPages);
    }

    ~Zone() { ::munmap(pages.data(), pages.size()); }

    Zone(const Zone& other) = delete;
    Zone(Zone&& other) noexcept = delete;
    Zone& operator=(const Zone& other) = delete;
    Zone& operator=(Zone&& other) noexcept = delete;

    void invariant() { }

    void recycle(std::span<std::byte> page)
    {
        auto index = toIndex(page);
        assert(inUse.check(index) && allocations.check(index) && "page was not in memory");
        inUse.unset(index);
    }

    size_t reclaimable()
    {
        invariant();
        return (allocations.popCount() - inUse.popCount()) * pageSize;
    }

    size_t release(size_t size)
    {
        invariant();
        assert(size <= reclaimable() && "Attempt to release more than reclaimable");
        std::size_t numberOfPagesToRelease = (size + pageSize - 1) / pageSize;
        std::size_t numberOfPagesReleased = 0;
        while (numberOfPagesReleased < numberOfPagesToRelease)
        {
            auto run = allocations.findLastRun(inUse);
            assert(run.has_value() && "release more than reclaimable");
            auto region_start = pages.data() + run->first * pageSize;
            auto region_size = (run->second - run->first) * pageSize;
            std::println("Releasing {} bytes", region_size);
            madvise(region_start, region_size, MADV_DONTNEED);
            numberOfPagesReleased += run->second - run->first;
            for (size_t i = run->first; i < run->second; i++)
            {
                allocations.unset(i);
            }
        }
        return numberOfPagesReleased * pageSize;
    }

    std::optional<Handle> allocate(size_t& availableMemoryBudget)
    {
        invariant();
        if (allocations.popCount() > inUse.popCount())
        {
            auto index = allocations.findFirstAllocatedButNotInUse(inUse);
            assert(index.has_value() && "more allocations than inUse, but no free index");
            auto* block = &controlBlocks[*index];
            block->zone = this;
            inUse.set(*index);
            return Handle(block, pages.subspan(*index * pageSize, pageSize));
        }
        else if (pageSize <= availableMemoryBudget)
        {
            //std::println("Reallocation Allocation: {}", pageSize);
            availableMemoryBudget -= pageSize;
            auto newAllocation = allocations.firstZero();
            assert(newAllocation.has_value() && "no more non allocated blocks, but below budget");

            allocations.set(*newAllocation);
            inUse.set(*newAllocation);

            auto* block = &controlBlocks[*newAllocation];
            block->zone = this;

            return Handle(block, pages.subspan(*newAllocation * pageSize, pageSize));
        }
        else
        {
            return std::nullopt;
        }
    }
};

void ControlBlock::reduce_ref()
{
    refCount--;
    if (refCount == 0)
    {
        auto index = (std::bit_cast<uintptr_t>(this) - std::bit_cast<uintptr_t>(zone->controlBlocks.get())) / sizeof(ControlBlock);
        zone->recycle(zone->pages.subspan(index * zone->pageSize, zone->pageSize));
    }
}

struct BufferManager
{
    std::vector<std::unique_ptr<Zone>> zones;
    size_t budget;

    explicit BufferManager(size_t memoryBudget)
    {
        size_t pageSize = 4096;
        for (int i = 0; i < 10; ++i)
        {
            zones.emplace_back(std::make_unique<Zone>(memoryBudget, pageSize));
            pageSize *= 2;
        }
        budget = memoryBudget;
    }

    size_t findZone(size_t size)
    {
        if (size == 0)
            return 0; // Handle zero case as index 0

        // 1. Ensure we treat 'size' as the ceiling of a power of 2
        // bit_width returns the number of bits needed to represent the value.
        // For size=4096, bit_width is 13.
        // We adjust this to map 4096 (2^12) to index 0.
        size_t width = std::bit_width(size - 1);

        // We want 4096 (2^12) to be 0, 8192 (2^13) to be 1, etc.
        // 12 is the base power (log2(4096))
        size_t zone = (width > 12) ? (width - 12) : 0;

        if (zone >= 10) [[unlikely]]
        {
            throw std::out_of_range(std::format("Allocation of {} exceeds maximum zone", size));
        }

        return zone;
    }

    bool reclaim()
    {
        auto bestZoneIt = std::ranges::max_element(zones, {}, [](const auto& zonePtr) { return zonePtr->reclaimable(); });
        auto& bestZone = **bestZoneIt;
        auto reclaimedMemory = bestZone.reclaimable();
        std::println("Best zone has {} bytes to reclaim", reclaimedMemory);
        if (reclaimedMemory == 0)
        {
            return false;
        }
        auto reclaimAmount = std::max(reclaimedMemory / 2, bestZone.pageSize);
        budget += bestZone.release(reclaimAmount);
        return true;
    }

    std::optional<Handle> allocate(size_t size)
    {
        auto& zone = *zones.at(findZone(size));
        auto allocation = zone.allocate(budget);
        if (allocation)
        {
            std::println("{}", budget);
            return allocation.value();
        }

        if (!reclaim())
        {
            return std::nullopt;
        }

        return allocate(size);
    }
};

std::vector<uint32_t> generate_allocation_requests(uint32_t numberOfRequests, size_t seed)
{
    std::mt19937 rnd(seed);
    std::geometric_distribution<int> distribution;
    constexpr int maxShift = 9; // largest size: 4096 << 9 = 2 MiB

    std::vector<uint32_t> allocationRequests;
    allocationRequests.reserve(numberOfRequests);
    for (uint32_t i = 0; i < numberOfRequests; ++i)
    {
        auto offset = 4 * std::sin((i * std::numbers::pi) / numberOfRequests);
        int shift = std::min(distribution(rnd) + static_cast<int>(offset), maxShift);
        allocationRequests.emplace_back(uint32_t{4096} << shift);
    }
    return allocationRequests;
}

struct Release
{
    uint32_t index;
};

struct Allocate
{
    uint32_t size;
    uint32_t index;
};

std::vector<std::variant<Release, Allocate>> generate_workload(uint32_t numberOfRequests, size_t seed)
{
    auto allocations = generate_allocation_requests(numberOfRequests, seed);
    std::mt19937 rnd(seed);
    std::bernoulli_distribution allocateOrRelease(0.5);

    std::vector<std::variant<Release, Allocate>> result;
    result.reserve(numberOfRequests * 2);
    std::vector<uint32_t> allocation_indices;
    allocation_indices.reserve(numberOfRequests);

    uint32_t currentAllocationIndex = 0;
    while (currentAllocationIndex < numberOfRequests || !allocation_indices.empty())
    {
        if (allocateOrRelease(rnd))
        {
            if (currentAllocationIndex < allocations.size())
            {
                result.emplace_back(Allocate{.size = allocations[currentAllocationIndex], .index = currentAllocationIndex});
                allocation_indices.emplace_back(currentAllocationIndex++);
            }
        }
        else
        {
            if (allocation_indices.empty())
            {
                continue;
            }
            std::uniform_int_distribution<size_t> dist(0, allocation_indices.size() - 1);
            size_t i = dist(rnd);
            auto value = allocation_indices[i];
            allocation_indices[i] = allocation_indices.back();
            allocation_indices.pop_back();
            result.emplace_back(Release{.index = value});
        }
    }

    return result;
}

template <typename... Ts>
struct Overload : Ts...
{
    using Ts::operator()...;
};

void run_benchmark(const std::vector<std::variant<Release, Allocate>>& requests, std::vector<std::optional<Handle>>& allocations)
{
    for (size_t iteration = 0; iteration < 1; iteration++)
    {
        BufferManager bm(1024ULL * 1024ULL * 4);
        std::println("Allocation is done");
        size_t totalAllocationSize = 0;
        for (auto& task : requests)
        {
            std::visit(
                Overload{
                    [&](Release r)
                    {
                        auto handle = std::move(allocations[r.index]);
                        allocations[r.index].reset();
                        if (handle)
                        {
                            assert(
                                *reinterpret_cast<size_t*>(handle->page.data()) == r.index && "Page does not contain the allocation index");
                            totalAllocationSize -= handle->page.size();
                        }
                    },
                    [&](Allocate a)
                    {
                        if (auto handle = bm.allocate(a.size))
                        {
                            allocations[a.index] = std::move(handle.value());
                            *reinterpret_cast<size_t*>(allocations[a.index].value().page.data()) = a.index;
                            totalAllocationSize += allocations[a.index].value().page.size();
                        } else
                        {
                            std::println(std::cerr, "oooming");
                        }
                    },
                },
                task);
        }
    }
}

int main()
{
    auto workload = generate_workload(256, 111);
    std::vector<std::optional<Handle>> allocations(256, std::nullopt);
    run_benchmark(workload, allocations);
}