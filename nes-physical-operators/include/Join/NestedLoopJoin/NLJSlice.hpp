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

#include <cstdint>
#include <memory>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/FileBackedPagedVector.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

/// This class represents a single slice for the NestedLoopJoin. It stores all tuples for the left and right stream.
class NLJSlice final : public Slice
{
public:
    NLJSlice(SliceStart sliceStart, SliceEnd sliceEnd, uint64_t numberOfWorkerThreads);

    /// Returns the number of tuples in this slice on either side.
    [[nodiscard]] uint64_t getNumberOfTuplesLeft() const;
    [[nodiscard]] uint64_t getNumberOfTuplesRight() const;

    /// Returns the pointer to the PagedVector on either side.
    [[nodiscard]] Nautilus::Interface::PagedVector* getPagedVectorRefLeft(WorkerThreadId workerThreadId) const;
    [[nodiscard]] Nautilus::Interface::PagedVector* getPagedVectorRefRight(WorkerThreadId workerThreadId) const;
    [[nodiscard]] Nautilus::Interface::FileBackedPagedVector*
    getPagedVectorRef(WorkerThreadId workerThreadId, JoinBuildSideType joinBuildSide) const;

    /// Moves all tuples in this slice to the PagedVector at 0th index on both sides. Acquires a write lock for combinePagedVectorsMutex.
    void combinePagedVectors();

    /// Acquires and releases a write lock for combinePagedVectorsMutex.
    void acquireCombinePagedVectorsLock();
    void releaseCombinePagedVectorsLock();

    /// Returns true if combinePagedVectors() method has already been called. This does not acquire a lock for combinePagedVectorsMutex.
    bool pagedVectorsCombined() const;

    /// Returns the size of the pages in the left and right PagedVectors in bytes. Acquires a write lock for combinePagedVectorsMutex.
    /// Returns zero if paged vectors were already combined.
    size_t getStateSizeInMemoryForThreadId(
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

    /// Returns the size of the pages in the left and right PagedVectors in bytes. Acquires a write lock for combinePagedVectorsMutex.
    /// Returns zero if paged vectors were already combined.
    size_t getStateSizeOnDiskForThreadId(
        const Memory::MemoryLayouts::MemoryLayout* memoryLayout, WorkerThreadId threadId, JoinBuildSideType joinBuildSide);

private:
    std::vector<std::unique_ptr<Nautilus::Interface::FileBackedPagedVector>> leftPagedVectors;
    std::vector<std::unique_ptr<Nautilus::Interface::FileBackedPagedVector>> rightPagedVectors;
    std::mutex combinePagedVectorsMutex;
    bool combinedPagedVectors;
};
}
