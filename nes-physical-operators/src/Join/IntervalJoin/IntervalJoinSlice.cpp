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

#include <Join/IntervalJoin/IntervalJoinSlice.hpp>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <numeric>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

IntervalJoinSlice::IntervalJoinSlice(
    AbstractBufferProvider& bufferProvider,
    const SliceStart sliceStart,
    const SliceEnd sliceEnd,
    const uint64_t numberOfWorkerThreads,
    const uint64_t tupleSize)
    : Slice(sliceStart, sliceEnd)
{
    const uint64_t pvMainBufferSize = PagedVector::getMainBufferSize();
    const uint64_t pvPageBufferSize = bufferProvider.getBufferSize();
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        if (auto pagedVectorBuffer = bufferProvider.getUnpooledBuffer(pvMainBufferSize))
        {
            PagedVector::init(pagedVectorBuffer.value(), pvPageBufferSize, tupleSize);
            pagedVectorBuffers.emplace_back(pagedVectorBuffer.value());
        }
        else
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for interval-join paged vector main buffer");
        }
    }
}

uint64_t IntervalJoinSlice::getNumberOfTuples() const
{
    /// Serialize against the merge's page-move + erase of pagedVectorBuffers. Else the accumulation iterates mid-erase
    /// and loads a half-moved PagedVector -> SIGSEGV.
    const std::scoped_lock guard(combineMutex);
    return std::accumulate(
        pagedVectorBuffers.begin(),
        pagedVectorBuffers.end(),
        uint64_t{0},
        [](uint64_t sum, const TupleBuffer& buf)
        {
            auto pagedVector = PagedVector::load(buf);
            return sum + pagedVector.getTotalNumberOfRecords();
        });
}

const TupleBuffer* IntervalJoinSlice::getPagedVectorRef(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % pagedVectorBuffers.size();
    return &pagedVectorBuffers[pos];
}

const TupleBuffer* IntervalJoinSlice::getMergedPagedVector() const
{
    return pagedVectorBuffers.data();
}

void IntervalJoinSlice::combinePagedVectors()
{
    /// Double-checked locking: the slow path's release-store of `combined` happens-before the fast-path acquire-load,
    /// so a caller that sees `combined == true` sees the fully-populated pagedVectorBuffers[0]. Set it AFTER the
    /// merge, never before, or a racer reads partial state.
    if (combined.load(std::memory_order_acquire))
    {
        return;
    }
    const std::scoped_lock guard(combineMutex);
    if (combined.load(std::memory_order_relaxed))
    {
        return;
    }
    if (pagedVectorBuffers.size() > 1)
    {
        for (uint64_t i = 1; i < pagedVectorBuffers.size(); ++i)
        {
            auto firstPagedVector = PagedVector::load(pagedVectorBuffers[0]);
            auto currPagedVector = PagedVector::load(pagedVectorBuffers[i]);
            firstPagedVector.movePagesFrom(currPagedVector);
        }
        pagedVectorBuffers.erase(pagedVectorBuffers.begin() + 1, pagedVectorBuffers.end());
    }
    combined.store(true, std::memory_order_release);
}

bool IntervalJoinSlice::claimTriggered() noexcept
{
    bool expected = false;
    return triggered.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}

}
