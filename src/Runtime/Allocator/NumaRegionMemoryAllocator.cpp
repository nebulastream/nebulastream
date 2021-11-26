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

#include <Runtime/Allocator/NumaRegionMemoryAllocator.hpp>
#ifdef NES_ENABLE_NUMA_SUPPORT
#if defined(__linux__)
#include <numaif.h>
#include <sys/mman.h>
#endif
#include <cstring>
#include <errno.h>

// The above code requires `--privileged` to be executed on a docker container.
// Furthermore, OS limits for mmapping and memory locking should be configured appropriately
// to allow memory allocations

namespace NES::Runtime {

void* NumaRegionMemoryAllocator::do_allocate(size_t sizeInBytes, size_t) {
    const int mmapFlags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_LOCKED;
    const int prot = PROT_READ | PROT_WRITE;
    unsigned long mask = 1ul << static_cast<unsigned long>(numaNodeIndex);
    const int mode = MPOL_BIND;
    // alloc sizeInBytes bytes using mmap as anon mapping
    void* mem = mmap(nullptr, sizeInBytes, prot, mmapFlags, -1, 0);
    NES_ASSERT2_FMT(mem != MAP_FAILED, "Cannot allocate memory " << sizeInBytes << " with errno " << strerror(errno));
    // bind it to a given numa region with strict assignment: the numa region must be capable of allocating the area
    auto ret = mbind(mem, sizeInBytes, mode, &mask, 32, MPOL_MF_STRICT | MPOL_MF_MOVE);
    NES_ASSERT2_FMT(ret == 0, "mbind error");
    // prevent swapping
    ret = mlock(mem, sizeInBytes);
    NES_ASSERT2_FMT(ret == 0, "mlock error");

    return reinterpret_cast<uint8_t*>(mem);
}

void NumaRegionMemoryAllocator::do_deallocate(void* pointer, size_t sizeInBytes, size_t) {
    NES_ASSERT2_FMT(pointer != nullptr, "invalid pointer");
    munlock(pointer, sizeInBytes);
    munmap(pointer, sizeInBytes);
}

}// namespace NES::Runtime
#endif