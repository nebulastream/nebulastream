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
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

NLJSlice::NLJSlice(
    AbstractBufferProvider* bufferProvider, const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfWorkerThreads)
    : Slice(sliceStart, sliceEnd)
{
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getBufferSize()))
        {
            /// initialize the paged vector tuple buffer
            PagedVector pagedVector = PagedVector::init(pagedVectorBuffer.value());
            /// attach to the hash map tuple buffer
            auto pv = PagedVector::init(pagedVectorBuffer.value());
            leftPagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map child buffer!");
        }
    }

    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getBufferSize()))
        {
            /// initialize the paged vector tuple buffer
            PagedVector pagedVector = PagedVector::init(pagedVectorBuffer.value());
            /// attach to the hash map tuple buffer
            auto pv = PagedVector::init(pagedVectorBuffer.value());
            rightPagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for chained hash map child buffer!");
        }
    }
}

uint64_t NLJSlice::getNumberOfTuplesLeft() const
{
    return std::accumulate(
        leftPagedVectorBuffers.begin(),
        leftPagedVectorBuffers.end(),
        0,
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pv = PagedVector::load(buf);
            return sum + pv.getTotalNumberOfEntries();
        });
}

uint64_t NLJSlice::getNumberOfTuplesRight() const
{
    return std::accumulate(
        rightPagedVectorBuffers.begin(),
        rightPagedVectorBuffers.end(),
        0,
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pv = PagedVector::load(buf);
            return sum + pv.getTotalNumberOfEntries();
        });
}

const TupleBuffer* NLJSlice::getPagedVectorRefLeft(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % leftPagedVectorBuffers.size();
    return &leftPagedVectorBuffers[pos];
}

const TupleBuffer* NLJSlice::getPagedVectorRefRight(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % rightPagedVectorBuffers.size();
    return &rightPagedVectorBuffers[pos];
}

const TupleBuffer* NLJSlice::getPagedVectorTupleBufferRef(const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide) const
{
    switch (joinBuildSide)
    {
        case JoinBuildSideType::Right:
            return getPagedVectorRefRight(workerThreadId);
        case JoinBuildSideType::Left:
            return getPagedVectorRefLeft(workerThreadId);
    }
    std::unreachable();
}

void NLJSlice::combinePagedVectors()
{
    /// Due to the out-of-order nature of our execution engine, it might happen that we call this code here from multiple worker threads.
    /// For example, if different worker threads are emitting the same slice for different windows.
    /// To ensure correctness, we use a lock here
    const std::scoped_lock lock(combinePagedVectorsMutex);

    /// Append all PagedVectors on the left join side and erase all items except for the first one
    /// We do this to ensure that we have only one PagedVector for each side during the probing phase
    if (leftPagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < leftPagedVectorBuffers.size(); ++i)
        {
            auto firstLeftPagedVector = PagedVector::load(leftPagedVectorBuffers[0]);
            auto currLeftPagedVector = PagedVector::load(leftPagedVectorBuffers[i]);
            firstLeftPagedVector.movePagesFrom(currLeftPagedVector);
        }
        leftPagedVectorBuffers.erase(leftPagedVectorBuffers.begin() + 1, leftPagedVectorBuffers.end());
    }

    /// Append all PagedVectors on the right join side and remove all items except for the first one
    if (rightPagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < rightPagedVectorBuffers.size(); ++i)
        {
            auto firstRightPagedVector = PagedVector::load(rightPagedVectorBuffers[0]);
            auto currRightPagedVector = PagedVector::load(rightPagedVectorBuffers[i]);
            firstRightPagedVector.movePagesFrom(currRightPagedVector);
        }
        rightPagedVectorBuffers.erase(rightPagedVectorBuffers.begin() + 1, rightPagedVectorBuffers.end());
    }
}
}
