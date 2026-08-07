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

#include <bit>
#include <cstddef>
#include <cstdint>
#include <new>
#include <nautilus/tracing/TracingUtil.hpp>
#include <ErrorHandling.hpp>
#include <val_ptr.hpp>

namespace NES
{
/// Dynamically-sized stack storage for a Nautilus function.
///
/// The size and alignment are constants while tracing. Compiled execution uses Nautilus's
/// AllocaOperation; direct/interpreted execution uses an equivalently aligned temporary allocation.
class NautilusStackAllocation
{
public:
    explicit NautilusStackAllocation(size_t size) : NautilusStackAllocation(size, alignof(std::max_align_t)) { }

    explicit NautilusStackAllocation(size_t size, size_t alignment)
        : alignment(alignment), address(allocate(size, alignment, interpretedAllocation))
    {
    }

    ~NautilusStackAllocation()
    {
        if (interpretedAllocation != nullptr)
        {
            ::operator delete(interpretedAllocation, std::align_val_t{alignment});
        }
    }

    NautilusStackAllocation(const NautilusStackAllocation&) = delete;
    NautilusStackAllocation& operator=(const NautilusStackAllocation&) = delete;
    NautilusStackAllocation(NautilusStackAllocation&&) = delete;
    NautilusStackAllocation& operator=(NautilusStackAllocation&&) = delete;

    [[nodiscard]] nautilus::val<int8_t*> data() const { return address; }

private:
    static nautilus::val<int8_t*> allocate(const size_t size, const size_t alignment, void*& interpretedAllocation)
    {
        PRECONDITION(size > 0, "Nautilus stack allocation size must be greater than zero");
        PRECONDITION(std::has_single_bit(alignment), "Nautilus stack allocation alignment must be a power of two");
#ifdef ENABLE_TRACING
        if (nautilus::tracing::inTracer())
        {
            return nautilus::val<int8_t*>{nautilus::tracing::traceAlloca(size, alignment)};
        }
#endif
        interpretedAllocation = ::operator new(size, std::align_val_t{alignment});
        return nautilus::val<int8_t*>{static_cast<int8_t*>(interpretedAllocation)};
    }

    void* interpretedAllocation{nullptr};
    size_t alignment;
    nautilus::val<int8_t*> address;
};
}
