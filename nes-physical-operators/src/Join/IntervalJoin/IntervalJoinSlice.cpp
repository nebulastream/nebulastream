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
#include <memory>
#include <mutex>
#include <numeric>
#include <Identifiers/Identifiers.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <SliceStore/Slice.hpp>

namespace NES
{

IntervalJoinSlice::IntervalJoinSlice(const SliceStart sliceStart, const SliceEnd sliceEnd, const uint64_t numberOfWorkerThreads)
    : Slice(sliceStart, sliceEnd)
{
    for (uint64_t i = 0; i < numberOfWorkerThreads; ++i)
    {
        pagedVectors.emplace_back(std::make_unique<PagedVector>());
    }
}

uint64_t IntervalJoinSlice::getNumberOfTuples() const
{
    return std::accumulate(
        pagedVectors.begin(),
        pagedVectors.end(),
        uint64_t{0},
        [](uint64_t sum, const auto& pagedVector) { return sum + (pagedVector ? pagedVector->getTotalNumberOfEntries() : 0); });
}

PagedVector* IntervalJoinSlice::getPagedVectorRef(const WorkerThreadId workerThreadId) const
{
    const auto pos = workerThreadId % pagedVectors.size();
    return pagedVectors[pos].get();
}

PagedVector* IntervalJoinSlice::getMergedPagedVector() const
{
    return pagedVectors[0].get();
}

void IntervalJoinSlice::combinePagedVectors()
{
    /// Double-checked locking. The release-store at the end of the slow path establishes
    /// happens-before for the acquire-load on the fast path: a concurrent caller that
    /// observes `combined == true` is guaranteed to see the completed merge (i.e. the
    /// fully-populated pagedVectors[0]). An earlier draft set `combined = true` via CAS
    /// *before* the merge, which let racers exit early while pagedVectors[0] was still
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
    if (pagedVectors.size() > 1)
    {
        for (uint64_t i = 1; i < pagedVectors.size(); ++i)
        {
            pagedVectors[0]->moveAllPages(*pagedVectors[i]);
        }
        pagedVectors.erase(pagedVectors.begin() + 1, pagedVectors.end());
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

}
