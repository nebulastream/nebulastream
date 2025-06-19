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

#include <Join/NestedLoopJoin/NLJSlice.hpp>

#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/PagedVector/FileBackedPagedVector.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

NLJSlice::NLJSlice(const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfWorkerThreads)
    : Slice(sliceStart, sliceEnd), combinedPagedVectors(false)
{
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        leftPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::FileBackedPagedVector>());
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        rightPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::FileBackedPagedVector>());
    }
}

uint64_t NLJSlice::getNumberOfTuplesLeft()
{
    const std::scoped_lock lock(combinePagedVectorsMutex);
    return std::accumulate(
        leftPagedVectors.begin(),
        leftPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

uint64_t NLJSlice::getNumberOfTuplesRight()
{
    const std::scoped_lock lock(combinePagedVectorsMutex);
    return std::accumulate(
        rightPagedVectors.begin(),
        rightPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    return getPagedVectorRef(workerThreadId, JoinBuildSideType::Left);
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    return getPagedVectorRef(workerThreadId, JoinBuildSideType::Right);
}

Nautilus::Interface::FileBackedPagedVector*
NLJSlice::getPagedVectorRef(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide) const
{
    switch (joinBuildSide)
    {
        case JoinBuildSideType::Left: {
            const auto pos = workerThreadId % leftPagedVectors.size();
            return leftPagedVectors[pos].get();
        }
        case JoinBuildSideType::Right: {
            const auto pos = workerThreadId % rightPagedVectors.size();
            return rightPagedVectors[pos].get();
        }
    }
    std::unreachable();
}

void NLJSlice::combinePagedVectors()
{
    /// Due to the out-of-order nature of our execution engine, it might happen that we call this code here from multiple worker threads.
    /// For example, if different worker threads are emitting the same slice for different windows.
    /// To ensure correctness, we use a lock here
    const std::scoped_lock lock(combinePagedVectorsMutex);
    combinedPagedVectors = true;

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectors.size(); ++i)
        {
            leftPagedVectors[0]->moveAllPages(*leftPagedVectors[i]);
        }
        leftPagedVectors.erase(leftPagedVectors.begin() + 1, leftPagedVectors.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectors.size(); ++i)
        {
            rightPagedVectors[0]->moveAllPages(*rightPagedVectors[i]);
        }
        rightPagedVectors.erase(rightPagedVectors.begin() + 1, rightPagedVectors.end());
    }
}

void NLJSlice::acquireCombinePagedVectorsLock()
{
    combinePagedVectorsMutex.lock();
}

void NLJSlice::releaseCombinePagedVectorsLock()
{
    combinePagedVectorsMutex.unlock();
}

bool NLJSlice::pagedVectorsCombined() const
{
    return combinedPagedVectors;
}

size_t NLJSlice::getStateSizeInMemoryForThreadId(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    const std::scoped_lock lock(combinePagedVectorsMutex);
    if (combinedPagedVectors)
    {
        return 0;
    }
    const auto* const pagedVector = getPagedVectorRef(threadId, joinBuildSide);
    const auto pageSize = memoryLayout->getBufferSize();
    const auto numPages = pagedVector->getNumberOfPages();
    return pageSize * numPages;
}

size_t NLJSlice::getStateSizeOnDiskForThreadId(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout, const WorkerThreadId threadId, const JoinBuildSideType joinBuildSide)
{
    const std::scoped_lock lock(combinePagedVectorsMutex);
    if (combinedPagedVectors)
    {
        return 0;
    }
    const auto* const pagedVector = getPagedVectorRef(threadId, joinBuildSide);
    const auto entrySize = memoryLayout->getTupleSize();
    const auto numTuples = pagedVector->getNumberOfTuplesOnDisk();
    return entrySize * numTuples;
}

}
