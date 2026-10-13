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
#include <cstdint>
#include <memory>

#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>

namespace NES::ConnTest
{

/// Upper bound for unpooled buffers. The harness allocates them for whole JSONL payloads
/// (Formatter::decode) and oversized sink writes, so it is generous; it is a limit, not a reservation.
inline constexpr std::size_t UNPOOLED_MEMORY_BUDGET_IN_BYTES = 256UZ * 1024 * 1024;

/// The harness sizes its pools by buffer count, BufferManager::create by a total memory budget.
/// Rounding in create() only ever moves bytes from the unpooled to the pooled share, so the pool
/// holds at least `bufferCount` buffers.
inline std::shared_ptr<BufferManager> makeBufferPool(const std::uint32_t bufferSize, const std::uint32_t bufferCount)
{
    constexpr BufferAlignment alignment{64};
    const auto totalMemory = (static_cast<std::size_t>(bufferSize) * bufferCount) + UNPOOLED_MEMORY_BUDGET_IN_BYTES;
    return BufferManager::create(
        totalMemory,
        static_cast<double>(UNPOOLED_MEMORY_BUDGET_IN_BYTES) / static_cast<double>(totalMemory),
        alignment,
        bufferSize,
        std::make_shared<NesDefaultMemoryAllocator>());
}

}
