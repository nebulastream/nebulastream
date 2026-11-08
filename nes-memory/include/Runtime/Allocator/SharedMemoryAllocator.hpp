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
#include <memory_resource>
#include <optional>

namespace NES
{

/// A memory resource whose every allocation is a memfd mapped shared, so that another process the file descriptor is passed to can map
/// the same memory, such as the processes of out-of-process UDFs reading and writing pool buffers in place.
class SharedMemoryAllocator final : public std::pmr::memory_resource
{
public:
    /// An allocation: its memfd, which stays open while it lives, and where it is mapped in this process.
    struct Mapping
    {
        int fd;
        std::byte* base;
        size_t size;

        [[nodiscard]] bool contains(const void* address, size_t length) const;
    };

    /// The first allocation that is still alive, of any SharedMemoryAllocator: the arena of the buffer pool built on it.
    [[nodiscard]] static std::optional<Mapping> primary();

private:
    void* do_allocate(size_t bytes, size_t alignment) override;
    void do_deallocate(void* p, size_t bytes, size_t alignment) override;
    bool do_is_equal(const memory_resource& other) const noexcept override { return this == &other; }
};

}
