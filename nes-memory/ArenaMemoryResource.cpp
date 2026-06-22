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

#include <Runtime/Spill/ArenaMemoryResource.hpp>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory_resource>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/types.h>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// System page size; mmap regions and file offsets must be page-aligned (e.g. 4 KiB on x86-64, 16 KiB on arm64 macOS).
size_t pageSize()
{
    static const auto CachedPageSize = static_cast<size_t>(::sysconf(_SC_PAGE_SIZE));
    return CachedPageSize;
}

size_t roundUpToPage(size_t numberOfBytes)
{
    const size_t page = pageSize();
    return (numberOfBytes + page - 1) & ~(page - 1);
}

/// Injects synthetic per-page storage latency so the storage-speed axis can be a controlled knob in experiments.
void injectIoLatency(uint64_t ioLatencyUsPer4k, size_t bytes)
{
    if (ioLatencyUsPer4k == 0)
    {
        return;
    }
    const size_t pages = (bytes + pageSize() - 1) / pageSize();
    std::this_thread::sleep_for(std::chrono::microseconds(ioLatencyUsPer4k * pages));
}

void fullPwrite(int fileDescriptor, const void* buf, size_t len, off_t off)
{
    const auto* ptr = static_cast<const char*>(buf);
    size_t done = 0;
    while (done < len)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): walking the caller's contiguous mmap buffer
        const ssize_t written = ::pwrite(fileDescriptor, ptr + done, len - done, off + static_cast<off_t>(done));
        if (written < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            /// NOLINTNEXTLINE(concurrency-mt-unsafe): strerror used only on the fatal error path under the arena mutex
            throw SpillIoFailure("pwrite failed: {}", std::strerror(errno));
        }
        done += static_cast<size_t>(written);
    }
}

void fullPread(int fileDescriptor, void* buf, size_t len, off_t off)
{
    auto* ptr = static_cast<char*>(buf);
    size_t done = 0;
    while (done < len)
    {
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): walking the caller's contiguous mmap buffer
        const ssize_t read = ::pread(fileDescriptor, ptr + done, len - done, off + static_cast<off_t>(done));
        if (read < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            /// NOLINTNEXTLINE(concurrency-mt-unsafe): strerror used only on the fatal error path under the arena mutex
            throw SpillIoFailure("pread failed: {}", std::strerror(errno));
        }
        if (read == 0)
        {
            break; /// region was never written (e.g. untouched reserved space)
        }
        done += static_cast<size_t>(read);
    }
}
}

namespace
{
/// Backing spill file is owner read/write only (rw-------): it holds this query's private operator state.
constexpr mode_t SPILL_FILE_MODE = 0600;
}

ArenaMemoryResource::ArenaMemoryResource(Mode mode, std::string backingFilePath) : mode(mode), backingPath(std::move(backingFilePath))
{
    /// open() is a variadic POSIX syscall; fd is computed (open() may fail and throw) so it cannot live in the member-init list.
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-prefer-member-initializer)
    fd = ::open(backingPath.c_str(), O_RDWR | O_CREAT | O_TRUNC, SPILL_FILE_MODE);
    if (fd < 0)
    {
        /// NOLINTNEXTLINE(concurrency-mt-unsafe): strerror used only on the fatal error path during single-threaded construction
        throw SpillIoFailure("could not open backing file {}: {}", backingPath, std::strerror(errno));
    }
}

ArenaMemoryResource::~ArenaMemoryResource()
{
    for (auto& region : regions)
    {
        if (region.live && region.ptr != nullptr)
        {
            ::munmap(region.ptr, region.mapLen);
        }
    }
    if (fd >= 0)
    {
        ::close(fd);
        /// NOLINTNEXTLINE(concurrency-mt-unsafe): unlink runs only in the destructor, after all access to this arena has ceased
        ::unlink(backingPath.c_str());
    }
}

void* ArenaMemoryResource::do_allocate(size_t bytes, [[maybe_unused]] size_t alignment)
{
    PRECONDITION(alignment <= pageSize(), "ArenaMemoryResource: alignment {} larger than a page {} is unsupported", alignment, pageSize());
    const std::lock_guard guard(mutex);
    const size_t mapLen = roundUpToPage(bytes);
    void* ptr = nullptr;
    if (mode == Mode::Anon)
    {
        ptr = ::mmap(nullptr, mapLen, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    }
    else
    {
        if (::ftruncate(fd, static_cast<off_t>(fileCursor + mapLen)) != 0)
        {
            /// NOLINTNEXTLINE(concurrency-mt-unsafe): strerror used only on the fatal error path under the arena mutex
            throw SpillIoFailure("ftruncate failed: {}", std::strerror(errno));
        }
        ptr = ::mmap(nullptr, mapLen, PROT_READ | PROT_WRITE, MAP_SHARED, fd, static_cast<off_t>(fileCursor));
    }
    if (ptr == MAP_FAILED)
    {
        /// NOLINTNEXTLINE(concurrency-mt-unsafe): strerror used only on the fatal error path under the arena mutex
        throw CannotAllocateBuffer("ArenaMemoryResource: mmap of {} bytes failed: {}", mapLen, std::strerror(errno));
    }
    regions.push_back(Region{.ptr = ptr, .mapLen = mapLen, .spillOffset = fileCursor, .live = true});
    fileCursor += mapLen;
    liveByteCount += mapLen;
    return ptr;
}

void ArenaMemoryResource::do_deallocate(void* ptr, size_t /*bytes*/, size_t /*alignment*/)
{
    const std::lock_guard guard(mutex);
    for (auto& region : regions)
    {
        if (region.live && region.ptr == ptr)
        {
            ::munmap(region.ptr, region.mapLen);
            region.live = false;
            liveByteCount -= region.mapLen;
            return;
        }
    }
}

bool ArenaMemoryResource::do_is_equal(const std::pmr::memory_resource& other) const noexcept
{
    return this == &other;
}

void ArenaMemoryResource::evict(uint64_t ioLatencyUsPer4k)
{
    const std::lock_guard guard(mutex);
    if (evicted)
    {
        return;
    }
    for (auto& region : regions)
    {
        if (!region.live)
        {
            continue;
        }
        injectIoLatency(ioLatencyUsPer4k, region.mapLen);
        if (mode == Mode::Anon)
        {
            fullPwrite(fd, region.ptr, region.mapLen, static_cast<off_t>(region.spillOffset));
        }
        else
        {
            ::msync(region.ptr, region.mapLen, MS_SYNC);
        }
        totalBytesWritten += region.mapLen;
        ::madvise(region.ptr, region.mapLen, MADV_DONTNEED);
    }
    evicted = true;
}

void ArenaMemoryResource::reload(uint64_t ioLatencyUsPer4k)
{
    const std::lock_guard guard(mutex);
    if (!evicted)
    {
        return;
    }
    for (auto& region : regions)
    {
        if (!region.live)
        {
            continue;
        }
        injectIoLatency(ioLatencyUsPer4k, region.mapLen);
        if (mode == Mode::Anon)
        {
            fullPread(fd, region.ptr, region.mapLen, static_cast<off_t>(region.spillOffset));
        }
        else
        {
            ::madvise(region.ptr, region.mapLen, MADV_WILLNEED);
            volatile unsigned char sink = 0;
            for (size_t off = 0; off < region.mapLen; off += pageSize())
            {
                /// touch one byte per page to fault the mmap region back in
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                sink = sink | static_cast<volatile unsigned char*>(region.ptr)[off];
            }
            (void)sink;
        }
        totalBytesRead += region.mapLen;
    }
    evicted = false;
}

}
