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

#include <Runtime/Allocator/SharedMemoryAllocator.hpp>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <optional>
#include <vector>

#include <unistd.h>
#include <sys/mman.h>

#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
struct Mappings
{
    std::mutex mutex;
    /// In the order of allocation.
    std::vector<SharedMemoryAllocator::Mapping> live;
};

Mappings& mappings()
{
    static Mappings instance;
    return instance;
}
}

bool SharedMemoryAllocator::Mapping::contains(const void* address, const size_t length) const
{
    const auto* const begin = static_cast<const std::byte*>(address);
    return begin >= base && length <= size && begin - base <= static_cast<std::ptrdiff_t>(size - length);
}

std::optional<SharedMemoryAllocator::Mapping> SharedMemoryAllocator::primary()
{
    auto& [mutex, live] = mappings();
    const std::scoped_lock lock(mutex);
    if (live.empty())
    {
        return std::nullopt;
    }
    return live.front();
}

void* SharedMemoryAllocator::do_allocate(const size_t bytes, [[maybe_unused]] const size_t alignment)
{
    INVARIANT(alignment <= static_cast<size_t>(sysconf(_SC_PAGESIZE)), "A shared mapping is only page aligned, not to {}", alignment);
    const int fd = memfd_create("nes-buffers", MFD_CLOEXEC);
    if (fd < 0)
    {
        throw BufferAllocationFailure("Failed to create shared memory: {}", std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
    }
    void* memory = MAP_FAILED;
    if (ftruncate(fd, static_cast<off_t>(bytes)) == 0)
    {
        memory = mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    }
    if (memory == MAP_FAILED)
    {
        const auto error = errno;
        ::close(fd);
        throw BufferAllocationFailure(
            "Failed to map shared memory of {} bytes: {}", bytes, std::strerror(error)); /// NOLINT(concurrency-mt-unsafe)
    }
    auto& [mutex, live] = mappings();
    const std::scoped_lock lock(mutex);
    live.push_back({.fd = fd, .base = static_cast<std::byte*>(memory), .size = bytes});
    return memory;
}

void SharedMemoryAllocator::do_deallocate(void* p, const size_t bytes, size_t)
{
    {
        auto& [mutex, live] = mappings();
        const std::scoped_lock lock(mutex);
        if (const auto mapping = std::ranges::find(live, static_cast<std::byte*>(p), &Mapping::base); mapping != live.end())
        {
            ::close(mapping->fd);
            live.erase(mapping);
        }
    }
    munmap(p, bytes);
}

}
