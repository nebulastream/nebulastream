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

#include <cstdint>
#include <memory>
#include <mutex>
#include <numeric>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Nautilus/Interface/PagedVector/FileBackedPagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>

namespace NES::Runtime::Execution
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

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectors.begin(),
        leftPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectors.begin(),
        rightPagedVectors.end(),
        0,
        [](uint64_t sum, const auto& pagedVector) { return sum + pagedVector->getTotalNumberOfEntries(); });
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    return getPagedVectorRef(QueryCompilation::JoinBuildSideType::Left, workerThreadId);
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    return getPagedVectorRef(QueryCompilation::JoinBuildSideType::Right, workerThreadId);
}

Interface::FileBackedPagedVector*
NLJSlice::getPagedVectorRef(const QueryCompilation::JoinBuildSideType joinBuildSide, const WorkerThreadId threadId) const
{
    switch (joinBuildSide)
    {
        case QueryCompilation::JoinBuildSideType::Left: {
            const auto pos = threadId % leftPagedVectors.size();
            return leftPagedVectors[pos].get();
        }
        case QueryCompilation::JoinBuildSideType::Right: {
            const auto pos = threadId % rightPagedVectors.size();
            return rightPagedVectors[pos].get();
        }
        default:
            throw UnknownJoinBuildSide();
    }
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
            leftPagedVectors[0]->appendAllPages(*leftPagedVectors[i]);
        }
        leftPagedVectors.erase(leftPagedVectors.begin() + 1, leftPagedVectors.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectors.size(); ++i)
        {
            rightPagedVectors[0]->appendAllPages(*rightPagedVectors[i]);
        }
        rightPagedVectors.erase(rightPagedVectors.begin() + 1, rightPagedVectors.end());
    }
}

void NLJSlice::acquireCombinePagedVectorsMutex()
{
    combinePagedVectorsMutex.lock();
}

void NLJSlice::releaseCombinePagedVectorsMutex()
{
    combinePagedVectorsMutex.unlock();
}

bool NLJSlice::pagedVectorsCombined() const
{
    return combinedPagedVectors;
}

size_t NLJSlice::getStateSizeInBytesForThreadId(
    const Memory::MemoryLayouts::MemoryLayout* memoryLayout,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    const WorkerThreadId threadId) const
{
    const auto *const pagedVector = getPagedVectorRef(joinBuildSide, threadId);
    const auto pageSize = memoryLayout->getBufferSize();
    const auto numPages = pagedVector->getNumberOfPages();

    return pageSize * numPages;
}

}
