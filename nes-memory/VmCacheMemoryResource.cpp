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

#include <Runtime/Allocator/VmCacheMemoryResource.hpp>

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <sys/mman.h>

namespace NES
{

namespace
{
constexpr std::size_t PAGE = 4096;
std::size_t roundUp(const std::size_t n, const std::size_t m)
{
    return ((n + m - 1) / m) * m;
}
}

VmCacheMemoryResource::VmCacheMemoryResource(const std::size_t arenaBytes) : arenaBytes(arenaBytes)
{
    void* p = mmap(nullptr, arenaBytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    arena = (p == MAP_FAILED) ? nullptr : static_cast<std::uint8_t*>(p);
}

VmCacheMemoryResource::~VmCacheMemoryResource()
{
    if (std::getenv("NES_VARALLOC_STATS") != nullptr)
    {
        std::fprintf(
            stderr,
            "VARALLOC_STATS mode=vmcache peak_resident_bytes=%zu allocs=%zu reuse=%zu\n",
            peakBytes,
            allocCount,
            reuseCount);
    }
    if (arena != nullptr)
    {
        munmap(arena, arenaBytes);
    }
}

void* VmCacheMemoryResource::do_allocate(const std::size_t bytes, const std::size_t /*alignment*/)
{
    /// Slices are page-rounded and the arena base is page-aligned, so any alignment <= PAGE is satisfied.
    const std::size_t need = roundUp(std::max<std::size_t>(bytes, 1), PAGE);
    std::lock_guard<std::mutex> lk(mtx);
    std::uint8_t* out = nullptr;
    if (auto it = freeBySize.find(need); it != freeBySize.end() && !it->second.empty())
    {
        out = it->second.back();
        it->second.pop_back();
        ++reuseCount;
    }
    else
    {
        if (arena == nullptr || bump + need > arenaBytes)
        {
            return nullptr; /// arena exhausted -> caller turns this into a BufferAllocationFailure
        }
        out = arena + bump;
        bump += need;
    }
    liveBytes += need;
    peakBytes = std::max(peakBytes, liveBytes);
    ++allocCount;
    return out;
}

void VmCacheMemoryResource::do_deallocate(void* p, const std::size_t bytes, const std::size_t /*alignment*/)
{
    const std::size_t need = roundUp(std::max<std::size_t>(bytes, 1), PAGE);
    madvise(p, need, MADV_DONTNEED); /// return the physical pages to the OS; virtual address is reused
    std::lock_guard<std::mutex> lk(mtx);
    liveBytes -= need;
    freeBySize[need].push_back(static_cast<std::uint8_t*>(p));
}

}
