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
#include <Runtime/DefaultBufferSizes.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>

#include <magic_enum/magic_enum.hpp>
#include "Util/Logger/Logger.hpp"

namespace NES
{
namespace
{

/// A buffer-size policy is a plain function pointer, so the table below is constexpr with static
/// storage. Stateful policies key their state by BufferComponent into the shared counter table
/// (one slot per component, since every component maps to at most one policy).
using BufferSizePolicy = size_t (*)();

/// One request counter per component. Thread safe; keyed by the component's index.
std::atomic<size_t>& requestCounter(const BufferComponent component)
{
    static std::array<std::atomic<size_t>, static_cast<size_t>(BufferComponent::NUM_COMPONENTS)> counters{};
    return counters.at(static_cast<size_t>(component));
}

/// Always hands out the same size.
template <size_t Value>
size_t constantValue()
{
    return Value;
}

/// Grows the size by doubling: starts at Start, doubles every EveryN-th request, clamped at Max.
/// Thread safe: each call atomically claims a request ordinal from the component's counter.
template <size_t Start, size_t Max, size_t EveryN, BufferComponent Component>
size_t increasingValue()
{
    static_assert(EveryN > 0, "EveryN must be positive");
    const size_t ordinal = requestCounter(Component).fetch_add(1, std::memory_order_relaxed);
    size_t size = Start;
    /// The `size < Max` guard bounds the loop to ~log2(Max/Start) steps regardless of ordinal.
    for (size_t doublings = ordinal / EveryN; doublings > 0 && size < Max; --doublings)
    {
        size = std::min(size * 2, Max);
    }
    return size;
}

/// ---- Tweak the per-component policies here. ----
constexpr std::array<BufferSizePolicy, static_cast<size_t>(BufferComponent::NUM_COMPONENTS)> POLICIES{
    constantValue<4096>, /// ARENA_PAGE
    constantValue<4096>, /// PAGED_VECTOR_VAR_SIZED
    constantValue<4096>, /// OUTPUT_FORMATTER_CHILD
    constantValue<4096>, /// EMIT_OUTPUT
    /// SOURCE_OUTPUT: grow 4K -> 2M, doubling every 1024 requests
    increasingValue<4096, 2 * 1024 * 1024, 4, BufferComponent::SOURCE_OUTPUT>,
};

}

size_t getDefaultBufferSize(const BufferComponent component)
{
    auto result = POLICIES.at(static_cast<size_t>(component))();
    return result;
}

}
