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

#include <atomic>
#include <cstddef>
#include <cstring>
#include <Util/Logger/Logger.hpp>
#include <sys/mman.h>
#include <ErrorHandling.hpp>

namespace NES::Memory
{
namespace detail
{
/**
        * @brief allocates memory that is aligned
        * @tparam T
        * @param size
        * @param alignment
        * @return pointer to allocated memory
        */
template <typename T = void>
T* allocAligned(size_t size, size_t alignment = 16)
{
    void* tmp = nullptr;
    INVARIANT(0 == posix_memalign(&tmp, alignment, sizeof(T) * size), "Cannot allocate {} bytes: {}", sizeof(T) * size, strerror(errno));
    return reinterpret_cast<T*>(tmp);
}

/**
        * @brief allocates memory that are pinned to memory
        * @tparam T
        * @tparam huge_page_size
        * @param size
        * @return pointer to allocated memory
        */
template <typename T = void, size_t huge_page_size = 1 << 21>
T* allocHugePages(size_t size)
{
    void* tmp = nullptr;
    INVARIANT(
        0 == posix_memalign(&tmp, huge_page_size, sizeof(T) * size), "Cannot allocate {} bytes: {}", sizeof(T) * size, strerror(errno));
#ifdef __linux__
    madvise(tmp, size * sizeof(T), MADV_HUGEPAGE);
#endif

    INVARIANT(tmp != nullptr, "Cannot remap as huge pages");
    mlock(tmp, size * sizeof(T));
    return reinterpret_cast<T*>(tmp);
}

}

class FixedPagesAllocator
{
public:
    explicit FixedPagesAllocator(size_t totalSize)
    {
        head = detail::allocHugePages<uint8_t>(totalSize);
        overrunAddress = reinterpret_cast<uintptr_t>(head) + totalSize;
        tail.store(reinterpret_cast<uintptr_t>(head));
        this->totalSize = totalSize;
    }

    uint8_t* getNewPage(size_t pageSize)
    {
        auto ptr = tail.fetch_add(pageSize);
        allocCnt++;
        INVARIANT(
            ptr < overrunAddress,
            "Invalid address {} < {} head={} total size={} allocCnt={}",
            ptr,
            overrunAddress,
            reinterpret_cast<uintptr_t>(head),
            totalSize,
            allocCnt);

        return reinterpret_cast<uint8_t*>(ptr);
    }

    virtual ~FixedPagesAllocator() { std::free(head); }

private:
    uint8_t* head;
    std::atomic<uint64_t> tail;
    uint64_t overrunAddress;
    uint64_t totalSize;
    uint64_t allocCnt = 0;
};

}
