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

namespace NES
{

/// Design alternative A2 (the DuckDB route): keep a single fixed block size and satisfy any larger
/// request by composing a *contiguous run* of fixed blocks. Blocks are carved from one anonymous mmap
/// arena (MAP_NORESERVE); allocation is address-ordered first-fit over a free-extent list, so it pays
/// A2's real costs: block-granular internal waste (a request rounds up to whole blocks) and *external*
/// fragmentation (a large request fails when no single contiguous run is free even though enough total
/// blocks are). Because the run is contiguous, callers get ordinary contiguous memory — no consumer
/// changes — which is exactly the measurable slice of A2 that does not require rewriting every operator.
///
/// ponytail: one global mutex + address-ordered first-fit. Fine for the chunk-granular unpooled path;
/// the free list stays short unless the workload fragments hard (which is itself the result we measure).
class ComposeFixedMemoryResource : public std::pmr::memory_resource
{
public:
    ComposeFixedMemoryResource(std::size_t arenaBytes, std::size_t blockBytes);
    ~ComposeFixedMemoryResource() override;

private:
    void* do_allocate(std::size_t bytes, std::size_t alignment) override;
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }

    std::mutex mtx;
    std::uint8_t* arena = nullptr;
    std::size_t arenaBytes = 0;
    std::size_t blockBytes = 0;
    std::size_t numBlocks = 0;
    std::map<std::size_t, std::size_t> freeExtents; /// startBlock -> runLength (in blocks), address-ordered
    std::size_t usedBlocks = 0;
    std::size_t peakUsedBlocks = 0;
    std::size_t allocCount = 0;
    std::size_t peakFreeExtents = 0; /// fragmentation proxy: how split the free space gets
    std::size_t externalFailures = 0; /// requests that found no contiguous run
};

}
