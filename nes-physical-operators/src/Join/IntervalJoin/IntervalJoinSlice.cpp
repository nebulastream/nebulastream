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
            /// initialize the paged vector tuple buffer
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
    /// Serialize against combinePagedVectors(): it mutates pagedVectorBuffers (movePagesFrom + erase), and a partner
    /// slice shared by several anchors can be merged by one trigger-assembly thread while another counts tuples here.
    /// Without this lock the accumulate iterates the vector mid-erase and loads a half-moved PagedVector -> SIGSEGV.
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
    return &pagedVectorBuffers[0];
}

void IntervalJoinSlice::combinePagedVectors()
{
    /// Double-checked locking. The release-store at the end of the slow path establishes
    /// happens-before for the acquire-load on the fast path: a concurrent caller that
    /// observes `combined == true` is guaranteed to see the completed merge (i.e. the
    /// fully-populated pagedVectorBuffers[0]). An earlier draft set `combined = true` via CAS
    /// *before* the merge, which let racers exit early while pagedVectorBuffers[0] was still
    /// being filled — a probe-worker would then read partial state.
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

bool IntervalJoinSlice::isCombined() const noexcept
{
    return combined.load(std::memory_order_acquire);
}

bool IntervalJoinSlice::claimTriggered() noexcept
{
    bool expected = false;
    return triggered.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
}

bool IntervalJoinSlice::isTriggered() const noexcept
{
    return triggered.load(std::memory_order_acquire);
}

void IntervalJoinSlice::acquireForTrigger() noexcept
{
    outstandingTriggers.fetch_add(1, std::memory_order_acq_rel);
}

uint64_t IntervalJoinSlice::releaseFromTrigger() noexcept
{
    const auto previous = outstandingTriggers.fetch_sub(1, std::memory_order_acq_rel);
    INVARIANT(previous > 0, "releaseFromTrigger called on a slice with no outstanding triggers");
    return previous - 1;
}

bool IntervalJoinSlice::hasOutstandingTriggers() const noexcept
{
    return outstandingTriggers.load(std::memory_order_acquire) > 0;
}

}
