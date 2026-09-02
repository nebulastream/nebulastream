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

#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory_resource>
#include <mutex>
#include <vector>

namespace NES
{

/// Design alternative A3 (vmcache / Umbra-lite): reserve one large anonymous mmap arena up front
/// (MAP_NORESERVE, so nothing is resident until touched), hand out page-rounded slices, and
/// madvise(DONTNEED) every slice on free so *resident* memory tracks the live set rather than the
/// high-water mark. Freed slices are kept in a per-size free list so addresses are reused (bounding
/// virtual growth); the physical pages behind them are reclaimed on free and re-faulted on reuse.
///
/// ponytail: one global mutex guards the arena. That is fine because this resource only serves the
/// *variable-sized* (unpooled) path, which allocates at chunk granularity far off the pooled hot
/// path; shard by size bucket if it ever shows up in a profile.
class VmCacheMemoryResource : public std::pmr::memory_resource
{
public:
    explicit VmCacheMemoryResource(std::size_t arenaBytes);
    ~VmCacheMemoryResource() override;

private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override;
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }

    std::mutex mtx;
    std::uint8_t* arena = nullptr;
    std::size_t arenaBytes = 0;
    std::size_t bump = 0; /// next never-yet-handed-out offset
    std::map<std::size_t, std::vector<std::uint8_t*>> freeBySize; /// page-rounded size -> reusable slices
    std::size_t liveBytes = 0;
    std::size_t peakBytes = 0;
    std::size_t allocCount = 0;
    std::size_t reuseCount = 0;
};

}
