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
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <MemoryLayout/MemoryLayout.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>

namespace NES::Runtime::Execution
{

NLJSlice::NLJSlice(
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numWorkerThreads,
    const std::shared_ptr<Memory::AbstractBufferProvider>& bufferProvider,
    const Memory::MemoryLayouts::MemoryLayoutPtr& leftMemoryLayout,
    const Memory::MemoryLayouts::MemoryLayoutPtr& rightMemoryLayout)
    : Slice(sliceStart, sliceEnd)
{
    for (uint64_t i = 0; i < numWorkerThreads; ++i)
    {
        leftPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>(bufferProvider, leftMemoryLayout));
    }

    for (uint64_t i = 0; i < numWorkerThreads; ++i)
    {
        rightPagedVectors.emplace_back(std::make_unique<Nautilus::Interface::PagedVector>(bufferProvider, rightMemoryLayout));
    }
}

uint64_t NLJSlice::getNumberOfTuplesLeft()
{
    uint64_t sum = 0;
    for (const auto& pagedVec : leftPagedVectors)
    {
        sum += pagedVec->getTotalNumberOfEntries();
    }
    return sum;
}

uint64_t NLJSlice::getNumberOfTuplesRight()
{
    uint64_t sum = 0;
    for (const auto& pagedVec : rightPagedVectors)
    {
        sum += pagedVec->getTotalNumberOfEntries();
    }
    return sum;
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectors.size();
    return leftPagedVectors[pos].get();
}

Nautilus::Interface::PagedVector* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectors.size();
    return rightPagedVectors[pos].get();
}

void NLJSlice::combinePagedVectors()
{
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
}
