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

#include <Runtime/Allocator/ComposeFixedMemoryResource.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <sys/mman.h>

namespace NES
{

ComposeFixedMemoryResource::ComposeFixedMemoryResource(const std::size_t arenaBytes, const std::size_t blockBytes)
    : arenaBytes(arenaBytes), blockBytes(blockBytes), numBlocks(arenaBytes / blockBytes)
{
    void* p = mmap(nullptr, arenaBytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    arena = (p == MAP_FAILED) ? nullptr : static_cast<std::uint8_t*>(p);
    if (arena != nullptr)
    {
        freeExtents.emplace(0, numBlocks); /// one big free run to start
    }
}

ComposeFixedMemoryResource::~ComposeFixedMemoryResource()
{
    if (std::getenv("NES_VARALLOC_STATS") != nullptr)
    {
        std::fprintf(
            stderr,
            "VARALLOC_STATS mode=compose block_bytes=%zu peak_reserved_bytes=%zu allocs=%zu peak_free_extents=%zu external_failures=%zu\n",
            blockBytes,
            peakUsedBlocks * blockBytes,
            allocCount,
            peakFreeExtents,
            externalFailures);
    }
    if (arena != nullptr)
    {
        munmap(arena, arenaBytes);
    }
}

void* ComposeFixedMemoryResource::do_allocate(const std::size_t bytes, const std::size_t /*alignment*/)
{
    const std::size_t nBlocks = (std::max<std::size_t>(bytes, 1) + blockBytes - 1) / blockBytes;
    std::lock_guard<std::mutex> lk(mtx);
    /// address-ordered first-fit: the lowest-addressed free run that is large enough
    for (auto it = freeExtents.begin(); it != freeExtents.end(); ++it)
    {
        if (it->second >= nBlocks)
        {
            const std::size_t startBlock = it->first;
            const std::size_t runLen = it->second;
            freeExtents.erase(it);
            if (runLen > nBlocks)
            {
                freeExtents.emplace(startBlock + nBlocks, runLen - nBlocks);
            }
            usedBlocks += nBlocks;
            peakUsedBlocks = std::max(peakUsedBlocks, usedBlocks);
            ++allocCount;
            return arena + (startBlock * blockBytes);
        }
    }
    ++externalFailures; /// no single contiguous run -> A2's external fragmentation surfaces as a failure
    return nullptr;
}

void ComposeFixedMemoryResource::do_deallocate(void* p, const std::size_t bytes, const std::size_t /*alignment*/)
{
    const std::size_t nBlocks = (std::max<std::size_t>(bytes, 1) + blockBytes - 1) / blockBytes;
    const std::size_t startBlock = (static_cast<std::uint8_t*>(p) - arena) / blockBytes;
    std::lock_guard<std::mutex> lk(mtx);

    std::size_t newStart = startBlock;
    std::size_t newLen = nBlocks;

    /// coalesce with the following run if it starts exactly where this one ends
    if (auto next = freeExtents.find(newStart + newLen); next != freeExtents.end())
    {
        newLen += next->second;
        freeExtents.erase(next);
    }
    /// coalesce with the preceding run if it ends exactly where this one starts
    if (auto after = freeExtents.lower_bound(newStart); after != freeExtents.begin())
    {
        auto prev = std::prev(after);
        if (prev->first + prev->second == newStart)
        {
            newStart = prev->first;
            newLen += prev->second;
            freeExtents.erase(prev);
        }
    }
    freeExtents.emplace(newStart, newLen);

    usedBlocks -= nBlocks;
    peakFreeExtents = std::max(peakFreeExtents, freeExtents.size());
}

}
