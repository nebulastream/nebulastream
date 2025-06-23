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


#include <Runtime/UnpooledChunksManager.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <memory_resource>
#include <numeric>
#include <ranges>
#include <thread>
#include <utility>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <TupleBufferImpl.hpp>

namespace NES
{
UnpooledChunksManager::UnpooledChunksManager(std::shared_ptr<std::pmr::memory_resource> memoryResource)
    : memoryResource(std::move(memoryResource))
{
}

UnpooledChunksManager::UnpooledChunk::UnpooledChunk(const uint64_t windowSize) : lastAllocateChunkKey(nullptr), rollingAverage(windowSize)
{
}

void UnpooledChunksManager::UnpooledChunk::emplaceChunkControlBlock(
    uint8_t* chunkKey, std::unique_ptr<Memory::detail::MemorySegment> newMemorySegment)
{
    auto& curUnpooledChunk = chunks.at(chunkKey);
    curUnpooledChunk.unpooledMemorySegments.emplace_back(std::move(newMemorySegment));
}

std::shared_ptr<folly::Synchronized<UnpooledChunksManager::UnpooledChunk>> UnpooledChunksManager::getChunk(const std::thread::id threadId)
{
    auto upgradeLockedUnpooledBuffers = allLocalUnpooledBuffers.ulock();
    if (const auto existingChunk = upgradeLockedUnpooledBuffers->find(threadId); existingChunk != upgradeLockedUnpooledBuffers->cend())
    {
        return existingChunk->second;
    }

    /// We have seen a new thread id and need to create a new UnpooledBufferChunkData for it
    auto newUnpooledBuffer = std::make_shared<folly::Synchronized<UnpooledChunk>>(UnpooledChunk(ROLLING_AVERAGE_UNPOOLED_BUFFER_SIZE));
    upgradeLockedUnpooledBuffers.moveFromUpgradeToWrite()->emplace(threadId, newUnpooledBuffer);
    return newUnpooledBuffer;
}

size_t UnpooledChunksManager::getNumberOfUnpooledBuffers() const
{
    const auto lockedAllBufferChunkData = allLocalUnpooledBuffers.rlock();
    size_t numOfUnpooledBuffers = 0;
    for (const auto& chunkData : *lockedAllBufferChunkData | std::views::values)
    {
        const auto rLockedChunkData = chunkData->rlock();
        numOfUnpooledBuffers += std::accumulate(
            rLockedChunkData->chunks.begin(),
            rLockedChunkData->chunks.end(),
            0,
            [](const auto& sum, const auto& item) { return sum + item.second.activeMemorySegments; });
    }
    return numOfUnpooledBuffers;
}

size_t UnpooledChunksManager::getTotalSizeOfUnpooledBufferChunks() const
{
    return unpooledBufferChunksSize;
}

std::pair<uint8_t*, uint8_t*>
UnpooledChunksManager::allocateSpace(const std::thread::id threadId, const size_t neededSize, const size_t alignment)
{
    /// There exist two possibilities that can happen
    /// 1. We have enough space in an already allocated chunk or 2. we need to allocate a new chunk of memory

    const auto lockedLocalUnpooledBufferData = getChunk(threadId)->wlock();
    const auto newRollingAverage = static_cast<size_t>(lockedLocalUnpooledBufferData->rollingAverage.add(neededSize));
    auto& localLastAllocatedChunkKey = lockedLocalUnpooledBufferData->lastAllocateChunkKey;
    auto& localUnpooledBufferChunkStorage = lockedLocalUnpooledBufferData->chunks;
    if (localUnpooledBufferChunkStorage.contains(localLastAllocatedChunkKey))
    {
        if (auto& currentAllocatedChunk = localUnpooledBufferChunkStorage.at(localLastAllocatedChunkKey);
            currentAllocatedChunk.usedSize + neededSize < currentAllocatedChunk.totalSize)
        {
            /// There is enough space in the last allocated chunk. Thus, we can create a tuple buffer from the available space
            const auto localMemoryForNewTupleBuffer = currentAllocatedChunk.startOfChunk + currentAllocatedChunk.usedSize;
            const auto localKeyForUnpooledBufferChunk = localLastAllocatedChunkKey;
            currentAllocatedChunk.activeMemorySegments += 1;
            currentAllocatedChunk.usedSize += neededSize;
            return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
        }
    }

    /// The last allocated chunk is not enough. Thus, we need to allocate a new chunk and insert it into the unpooled buffer storage
    /// The memory to allocate must be larger than bufferSize, while also taking the rolling average into account.
    /// For now, we allocate multiple localLastAllocateChunkKeyrolling averages. If this is too small for the current bufferSize, we allocate at least the bufferSize
    const auto newAllocationSizeExact = std::max(neededSize, newRollingAverage * NUM_PRE_ALLOCATED_CHUNKS);
    const auto newAllocationSize = (newAllocationSizeExact + 4095U) & ~4095U; /// Round to the nearest multiple of 4KB (page size)
    auto* const newlyAllocatedMemory = static_cast<uint8_t*>(memoryResource->allocate(newAllocationSize, alignment));
    if (newlyAllocatedMemory == nullptr)
    {
        NES_WARNING("Could not allocate {} bytes for unpooled chunk!", newAllocationSize);
        return {};
    }

    /// Updating the local last allocate chunk key and adding the new chunk to the local chunk storage
    localLastAllocatedChunkKey = newlyAllocatedMemory;
    const auto localKeyForUnpooledBufferChunk = newlyAllocatedMemory;
    const auto localMemoryForNewTupleBuffer = newlyAllocatedMemory;
    auto& currentAllocatedChunk = localUnpooledBufferChunkStorage[localKeyForUnpooledBufferChunk];
    currentAllocatedChunk.startOfChunk = newlyAllocatedMemory;
    currentAllocatedChunk.totalSize = newAllocationSize;
    currentAllocatedChunk.usedSize += neededSize;
    currentAllocatedChunk.activeMemorySegments += 1;
    unpooledBufferChunksSize += newAllocationSize;
    /*NES_INFO(
        "Added tuple buffer {} of {}B to: {}",
        fmt::ptr(localMemoryForNewTupleBuffer),
        neededSize,
        fmt::format("{}", currentAllocatedChunk));*/
    return {localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer};
}

Memory::TupleBuffer UnpooledChunksManager::getUnpooledBuffer(
    const size_t neededSize, size_t alignment, const std::shared_ptr<Memory::BufferRecycler>& bufferRecycler)
{
    const auto threadId = std::this_thread::get_id();

    /// we have to align the buffer size as ARM throws an SIGBUS if we have unaligned accesses on atomics.
    const auto alignedBufferSizePlusControlBlock
        = Memory::alignBufferSize(neededSize + sizeof(Memory::detail::BufferControlBlock), alignment);

    /// Getting space from the unpooled chunks manager
    const auto& [localKeyForUnpooledBufferChunk, localMemoryForNewTupleBuffer]
        = this->allocateSpace(threadId, alignedBufferSizePlusControlBlock, alignment);

    /// Creating a new memory segment, and adding it to the unpooledMemorySegments
    const auto alignedBufferSize = Memory::alignBufferSize(neededSize, alignment);
    const auto controlBlockSize = Memory::alignBufferSize(sizeof(Memory::detail::BufferControlBlock), alignment);
    const auto chunk = this->getChunk(threadId);
    auto memSegment = std::make_unique<Memory::detail::MemorySegment>(
        localMemoryForNewTupleBuffer + controlBlockSize,
        alignedBufferSize,
        [copyOfMemoryResource = this->memoryResource,
         copyOLastChunkPtr = localKeyForUnpooledBufferChunk,
         copyOfChunk = chunk,
         copyOfAlignment = alignment,
         this](Memory::detail::MemorySegment* memorySegment, Memory::BufferRecycler*)
        {
            auto lockedLocalUnpooledBufferData = copyOfChunk->wlock();
            auto& curUnpooledChunk = lockedLocalUnpooledBufferData->chunks[copyOLastChunkPtr];
            INVARIANT(
                curUnpooledChunk.activeMemorySegments > 0,
                "curUnpooledChunk.activeMemorySegments must be larger than 0 but is {}",
                curUnpooledChunk.activeMemorySegments);
            curUnpooledChunk.activeMemorySegments -= 1;
            memorySegment->size = 0;
            if (curUnpooledChunk.activeMemorySegments == 0)
            {
                /// All memory segments have been removed, therefore, we can deallocate the unpooled chunk
                const auto extractedChunk = lockedLocalUnpooledBufferData->chunks.extract(copyOLastChunkPtr);
                auto& extractedChunkControlBlock = extractedChunk.mapped();
                lockedLocalUnpooledBufferData->lastAllocateChunkKey = nullptr;
                lockedLocalUnpooledBufferData.unlock();
                unpooledBufferChunksSize -= curUnpooledChunk.totalSize;
                copyOfMemoryResource->deallocate(
                    extractedChunkControlBlock.startOfChunk, extractedChunkControlBlock.totalSize, copyOfAlignment);
            }
        });

    auto* leakedMemSegment = memSegment.get();
    {
        /// Inserting the memory segment into the unpooled buffer storage
        const auto lockedLocalUnpooledBufferData = chunk->wlock();
        lockedLocalUnpooledBufferData->emplaceChunkControlBlock(localKeyForUnpooledBufferChunk, std::move(memSegment));
    }

    if (leakedMemSegment->controlBlock->prepare(bufferRecycler))
    {
        return Memory::TupleBuffer(leakedMemSegment->controlBlock.get(), leakedMemSegment->ptr, neededSize);
    }
    throw InvalidRefCountForBuffer("[BufferManager] got buffer with invalid reference counter");
}

}
