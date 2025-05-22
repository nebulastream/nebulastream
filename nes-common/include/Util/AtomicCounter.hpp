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
#include <cstdint>

namespace NES
{

#ifdef __cpp_lib_hardware_interference_size
using std::hardware_constructive_interference_size;
using std::hardware_destructive_interference_size;
#else
    /// 64 bytes on x86-64 │ L1_CACHE_BYTES │ L1_CACHE_SHIFT │ __cacheline_aligned │ ...
    #include <cstddef>
constexpr std::size_t hardware_constructive_interference_size = 64;
constexpr std::size_t hardware_destructive_interference_size = 64;
#endif

template <typename T>
///TODO: change this to folly::hardware_destructive_interference_size but then we have to load folly
struct alignas(hardware_constructive_interference_size) AtomicCounter
{
    explicit AtomicCounter(T defValue = 0) : counter(defValue) { }

    AtomicCounter(const AtomicCounter<T>& other) : counter(other.counter.load()) { }

    AtomicCounter<T>& operator=(const AtomicCounter<T>& other)
    {
        counter.store(other.counter.load());
        return *this;
    }

    operator T() { return counter.load(); }

    T fetch_add(T delta) { return counter.fetch_add(delta); }

    std::atomic<T> counter;
};

static_assert(sizeof(AtomicCounter<uint64_t>) == 64);

}
