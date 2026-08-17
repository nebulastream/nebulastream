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

#include <Runtime/MallocUnpooledBufferManager.hpp>

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <utility>
#include <Runtime/MemoryUtils.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>
#include "Util/Sanitizer.hpp"

namespace NES
{

struct MallocUnpooledBufferManager::State
{
    struct Allocation
    {
        std::unique_ptr<detail::MemorySegment> memorySegment;
    };

    /// ponytail: one lock keeps allocation and accounting atomic; split it only if profiling shows contention here.
    mutable std::mutex mutex;
    std::unordered_map<detail::MemorySegment*, Allocation> allocations;
    size_t reservedBytes = 0;
};

MallocUnpooledBufferManager::MallocUnpooledBufferManager(const size_t unpooledMemoryBudgetInBytes)
    : state(std::make_shared<State>()), unpooledMemoryBudgetInBytes(unpooledMemoryBudgetInBytes)
{
}

MallocUnpooledBufferManager::~MallocUnpooledBufferManager() = default;

size_t MallocUnpooledBufferManager::getNumberOfUnpooledBuffers() const
{
    const std::lock_guard lock(state->mutex);
    return state->allocations.size();
}

std::optional<TupleBuffer> MallocUnpooledBufferManager::getUnpooledBuffer(
    const size_t neededSize, const size_t alignment, const std::shared_ptr<BufferRecycler>& bufferRecycler)
{
    PRECONDITION(alignment != 0 && !(alignment & (alignment - 1)), "Buffer alignment {} is not a power of two", alignment);

    const auto alignedBufferSize = alignBufferSize(neededSize, alignment);
    const auto redzoneSize = alignBufferSize(detail::CONTROL_BLOCK_REDZONE_SIZE, alignment);
    if (alignedBufferSize > std::numeric_limits<size_t>::max() - redzoneSize)
    {
        return std::nullopt;
    }
    const auto slotSize = redzoneSize + alignedBufferSize;
    if (slotSize > std::numeric_limits<size_t>::max() - (alignment - 1))
    {
        return std::nullopt;
    }

    /// malloc itself does not accept an alignment, so reserve enough bytes to align the exposed slot manually.
    const auto allocationSize = slotSize + alignment - 1;
    const std::lock_guard lock(state->mutex);
    if (state->reservedBytes >= unpooledMemoryBudgetInBytes || allocationSize > unpooledMemoryBudgetInBytes - state->reservedBytes)
    {
        NES_WARNING(
            "Unpooled memory budget of {}B would be exceeded by a {}B malloc allocation ({}B already in use); refusing allocation.",
            unpooledMemoryBudgetInBytes,
            allocationSize,
            state->reservedBytes);
        return std::nullopt;
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-owning-memory) - ownership is captured by and released in the recycler
    void* const allocation
        = std::malloc(allocationSize); /// NOLINT(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory) - freed by recycler
    if (allocation == nullptr)
    {
        NES_WARNING("Could not malloc {} bytes for an unpooled buffer!", allocationSize);
        return std::nullopt;
    }

    void* slotStart = allocation;
    auto availableSpace = allocationSize;
    auto* const slot = static_cast<uint8_t*>(std::align(alignment, slotSize, slotStart, availableSpace));
    INVARIANT(slot != nullptr, "Could not align malloc-backed unpooled buffer");
    ASAN_POISON_MEMORY_REGION(allocation, allocationSize);

    auto memorySegment = std::make_unique<detail::MemorySegment>(
        slot + redzoneSize,
        alignedBufferSize,
        [copyOfState = state, allocation, allocationSize](detail::MemorySegment* memorySegment, BufferRecycler*)
        {
            State::Allocation releasedAllocation;
            {
                const std::lock_guard stateLock(copyOfState->mutex);
                auto node = copyOfState->allocations.extract(memorySegment);
                INVARIANT(!node.empty(), "Released malloc-backed unpooled buffer is not tracked");
                releasedAllocation = std::move(node.mapped());
                copyOfState->reservedBytes -= allocationSize;
            }
            ASAN_UNPOISON_MEMORY_REGION(allocation, allocationSize);
            std::free(allocation); /// NOLINT(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory) - matching malloc
        });

    auto* const leakedMemorySegment = memorySegment.get();
    state->reservedBytes += allocationSize;
    state->allocations.emplace(leakedMemorySegment, State::Allocation{std::move(memorySegment)});

    if (leakedMemorySegment->controlBlock->prepare(bufferRecycler))
    {
        ASAN_UNPOISON_MEMORY_REGION(leakedMemorySegment->ptr, leakedMemorySegment->size);
        return TupleBuffer{leakedMemorySegment->controlBlock.get(), leakedMemorySegment->ptr, leakedMemorySegment->size};
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

}
