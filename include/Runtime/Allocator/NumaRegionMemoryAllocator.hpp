/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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


#ifndef NES_INCLUDE_RUNTIME_ALLOCATOR_NUMAREGIONALLOCATOR_HPP_
#define NES_INCLUDE_RUNTIME_ALLOCATOR_NUMAREGIONALLOCATOR_HPP_

#include <Util/Logger.hpp>
#include <memory>
#include <memory_resource>
#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <sys/mman.h>
#include <numaif.h>
#endif
#endif
#include <errno.h>
#include <cstring>

namespace NES::Runtime {
class NumaRegionMemoryAllocator : public std::pmr::memory_resource {
  public:
    explicit NumaRegionMemoryAllocator(uint32_t numaNodeIndex) : numaNodeIndex(numaNodeIndex) {};

    ~NumaRegionMemoryAllocator() override {}

  private:
    void* do_allocate(size_t sizeInBytes, size_t) override {
#ifdef NES_ENABLE_NUMA_SUPPORT
        const int mmapFlags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_LOCKED;
        const int prot = PROT_READ | PROT_WRITE;
        unsigned long mask = 1ul << static_cast<unsigned long>(numaNodeIndex);
        const int mode = MPOL_BIND;

        void* mem = mmap(nullptr, sizeInBytes, prot, mmapFlags, -1, 0);
        NES_ASSERT2_FMT(mem != MAP_FAILED, "Cannot allocate memory " <<  sizeInBytes  << " with errno " << strerror(errno));

        auto ret = mbind(mem, sizeInBytes, mode, &mask, 32, MPOL_MF_STRICT | MPOL_MF_MOVE);
        NES_ASSERT2_FMT(ret == 0, "mbind error");

        ret = mlock(mem, sizeInBytes);
        NES_ASSERT2_FMT(ret == 0, "mlock error");

        return reinterpret_cast<uint8_t*>(mem);
#else
        void* tmp = nullptr;
        NES_ASSERT(posix_memalign(&tmp, 64, sizeInBytes) == 0, "memory allocation failed with alignment");
        return tmp;
#endif
    }

    void do_deallocate(void* pointer, size_t sizeInBytes, size_t) override {
        NES_ASSERT2_FMT(pointer != nullptr, "invalid pointer");
#ifdef NES_ENABLE_NUMA_SUPPORT
        munlock(pointer, sizeInBytes);
        munmap(pointer, sizeInBytes);
#else
        ((void) numaNodeIndex);
        ((void) sizeInBytes);
        std::free(pointer);
#endif
    }
    bool do_is_equal(const memory_resource& other) const noexcept override {
        return this == &other;
    }

  private:
    uint32_t numaNodeIndex;

};
using NumaRegionMemoryAllocatorPtr = std::shared_ptr<NumaRegionMemoryAllocator>;
}

#endif//NES_INCLUDE_RUNTIME_ALLOCATOR_NUMAREGIONALLOCATOR_HPP_
