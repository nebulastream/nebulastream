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
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/Spill/SpillManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

struct CreateNewNLJSliceArgs final : CreateNewSlicesArguments
{
    CreateNewNLJSliceArgs(AbstractBufferProvider& bufferProvider, uint64_t tupleSizeLeft, uint64_t tupleSizeRight)
        : bufferProvider(&bufferProvider)
        , tupleSizeLeft(tupleSizeLeft)
        , tupleSizeRight(tupleSizeRight) /// NOLINT(clang-analyzer-optin.cplusplus.UninitializedObject)
    {
    }

    ~CreateNewNLJSliceArgs() override = default;

    AbstractBufferProvider* bufferProvider;
    uint64_t tupleSizeLeft;
    uint64_t tupleSizeRight;
};

/// This class represents a single slice for the NestedLoopJoin. It stores all tuples for the left and right stream.
/// When state spilling is enabled it is also a SpillableState: all of its PagedVectors allocate from one per-slice
/// arena-backed BufferManager, so the whole slice (both sides, all workers) is a single spill unit.
class NLJSlice final : public Slice, public SpillableState
{
public:
    NLJSlice(
        AbstractBufferProvider& bufferProvider,
        SliceStart sliceStart,
        SliceEnd sliceEnd,
        uint64_t numberOfWorkerThreads,
        uint64_t tupleSizeLeft,
        uint64_t tupleSizeRight,
        std::shared_ptr<SpillManager> spillManager = nullptr);
    ~NLJSlice() override;

    /// Returns the number of tuples in this slice on either side.
    [[nodiscard]] uint64_t getNumberOfTuplesLeft() const;
    [[nodiscard]] uint64_t getNumberOfTuplesRight() const;

    /// Returns the pointer to the PagedVector on either side.
    [[nodiscard]] const TupleBuffer* getPagedVectorRefLeft(WorkerThreadId workerThreadId) const;
    [[nodiscard]] const TupleBuffer* getPagedVectorRefRight(WorkerThreadId workerThreadId) const;
    [[nodiscard]] const TupleBuffer* getPagedVectorTupleBufferRef(WorkerThreadId workerThreadId, JoinBuildSideType joinBuildSide) const;

    /// Moves all tuples in this slice to the PagedVector at 0th index on both sides.
    void combinePagedVectors();

    /// Returns the per-slice arena-backed BufferManager when spilling is enabled, else nullptr. The build operator uses
    /// it to route PagedVector page allocations into this slice's arena so the whole slice is a single spill unit.
    [[nodiscard]] AbstractBufferProvider* getSpillBufferProvider() const;

    /// SpillableState
    [[nodiscard]] size_t residentBytes() const override;
    [[nodiscard]] bool isEvicted() const override;
    void evictState() override;
    void reloadState() override;
    [[nodiscard]] uint64_t coldnessKey() const override;

private:
    std::vector<TupleBuffer> leftPagedVectorBuffers;
    std::vector<TupleBuffer> rightPagedVectorBuffers;
    std::mutex combinePagedVectorsMutex;

    /// Per-slice spill arena (only created when spilling is enabled); all PagedVectors are pinned to its BufferManager.
    std::shared_ptr<SpillManager> spillManager;
    std::shared_ptr<ArenaMemoryResource> spillArena;
    std::shared_ptr<BufferManager> spillBufferManager;
    bool spillEnabled{false};
    std::atomic<bool> spillEvicted{false};
};
}
