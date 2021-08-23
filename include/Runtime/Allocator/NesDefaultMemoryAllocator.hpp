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

#ifndef NES_INCLUDE_RUNTIME_ALLOCATOR_NESDEFAULTMEMORYALLOCATOR_HPP_
#define NES_INCLUDE_RUNTIME_ALLOCATOR_NESDEFAULTMEMORYALLOCATOR_HPP_

#include <Util/Logger.hpp>
#include <memory>
#include <memory_resource>
namespace NES::Runtime {
class NesDefaultMemoryAllocator : public std::pmr::memory_resource {
  public:
    explicit NesDefaultMemoryAllocator() {};

    ~NesDefaultMemoryAllocator() override {}

  private:
    void* do_allocate(size_t bytes, size_t alignment) override {
        void* tmp = nullptr;
        NES_ASSERT(posix_memalign(&tmp, alignment, bytes) == 0, "memory allocation failed with alignment");
        return tmp;
    }

    void do_deallocate(void* p, size_t, size_t) override {
        std::free(p);
    }
    bool do_is_equal(const memory_resource& other) const noexcept override {
        return this == &other;
    }

};
using NesDefaultMemoryAllocatorPtr = std::shared_ptr<NesDefaultMemoryAllocator>;
}
#endif//NES_INCLUDE_RUNTIME_ALLOCATOR_NESDEFAULTMEMORYALLOCATOR_HPP_
