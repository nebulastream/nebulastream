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
#include <memory_resource>
#include <mutex>
#include <string>
#include <vector>

namespace NES
{

/// A std::pmr::memory_resource that backs every allocation with its own page-aligned mmap region at a fixed virtual
/// address. It is intended to be injected into NES::BufferManager::create() so that all buffers (and therefore all
/// operator state allocated through that BufferManager, e.g. ChainedHashMap / PagedVector pages) land in this single
/// resource without any changes to the allocating code. The whole resource then is one slice's resident set and the
/// unit of spilling.
///
/// Spilling preserves virtual addresses, which is the key property that lets live pointers into the state (hashmap
/// chains, paged-vector entries) survive an evict/reload cycle. Two modes:
///   - Anon:       MAP_PRIVATE|ANON|NORESERVE; evict = pwrite the pages to a spill file + madvise(DONTNEED);
///                 reload = pread them back to the same address. This is the explicit, vmcache-faithful path.
///                 NOTE: after evict, the pages fault back as zero-fill, so a reload() MUST happen before any access.
///   - FileBacked: MAP_SHARED on a backing file; evict = msync + madvise(DONTNEED); reload = madvise(WILLNEED) and
///                 touch the pages so the OS pages them back in.
///
/// Allocation/deallocation are guarded by an internal mutex, so multiple worker threads may allocate from one arena
/// concurrently (the page-boundary allocations are low frequency). evict/reload must not run concurrently with access
/// to the backed memory; the SpillManager's pin protocol guarantees this (a slice is reloaded+pinned before access and
/// only evicted while unpinned).
class ArenaMemoryResource final : public std::pmr::memory_resource
{
public:
    enum class Mode : uint8_t
    {
        Anon,
        FileBacked
    };

    /// @param mode spilling strategy (see class comment)
    /// @param backingFilePath path of the spill/backing file; created (O_TRUNC) on construction and unlinked on destruction
    ArenaMemoryResource(Mode mode, std::string backingFilePath);
    ~ArenaMemoryResource() override;

    ArenaMemoryResource(const ArenaMemoryResource&) = delete;
    ArenaMemoryResource& operator=(const ArenaMemoryResource&) = delete;
    /// Deleted: this resource exclusively owns an fd and a set of mmap regions, which cannot be moved.
    ArenaMemoryResource(ArenaMemoryResource&&) = delete;
    ArenaMemoryResource& operator=(ArenaMemoryResource&&) = delete;

    /// Move all resident pages out of DRAM. No-op if already evicted.
    /// @param ioLatencyUsPer4k optional synthetic per-4KB I/O latency (storage-speed knob for experiments; 0 = off)
    void evict(uint64_t ioLatencyUsPer4k = 0);
    /// Bring all pages back to their exact original virtual addresses. No-op if not evicted.
    void reload(uint64_t ioLatencyUsPer4k = 0);

    [[nodiscard]] bool isEvicted() const noexcept { return evicted; }

    /// Sum of the mapped (page-rounded) bytes of all live regions; i.e. this slice's resident-set size.
    [[nodiscard]] size_t liveBytes() const noexcept { return liveByteCount; }

    [[nodiscard]] uint64_t bytesWritten() const noexcept { return totalBytesWritten; }

    [[nodiscard]] uint64_t bytesRead() const noexcept { return totalBytesRead; }

protected:
    void* do_allocate(size_t bytes, size_t alignment) override;
    void do_deallocate(void* ptr, size_t bytes, size_t alignment) override;
    [[nodiscard]] bool do_is_equal(const std::pmr::memory_resource& other) const noexcept override;

private:
    struct Region
    {
        void* ptr;
        size_t mapLen;
        size_t spillOffset; /// offset of this region in the spill/backing file
        bool live;
    };

    mutable std::mutex mutex; /// guards regions/fileCursor/liveByteCount and evict/reload
    Mode mode;
    std::string backingPath;
    int fd{-1};
    size_t fileCursor{0}; /// next free offset in the backing file
    std::vector<Region> regions;
    size_t liveByteCount{0};
    bool evicted{false};
    uint64_t totalBytesWritten{0};
    uint64_t totalBytesRead{0};
};

}
