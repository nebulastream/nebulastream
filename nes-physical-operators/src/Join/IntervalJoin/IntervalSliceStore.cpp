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

#include <Join/IntervalJoin/IntervalSliceStore.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinSlice.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES
{

IntervalSliceStore::IntervalSliceStore(const uint64_t sliceWidth, SliceCacheConfiguration sliceCacheConfiguration)
    : sliceWidth(sliceWidth)
    /// Tumbling: slide == size == sliceWidth.
    , sliceAssigner(sliceWidth, sliceWidth)
    , sliceCacheConfiguration(std::move(sliceCacheConfiguration))
    , numberOfActiveInputPipelines(0)
{
    PRECONDITION(sliceWidth > 0, "Interval-join slice width must be > 0; got {}", sliceWidth);
}

IntervalSliceStore::~IntervalSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> IntervalSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);
    {
        const auto slicesReadLocked = slices.rlock();
        if (const auto it = slicesReadLocked->find(sliceEnd); it != slicesReadLocked->end())
        {
            return {it->second};
        }
    }

    /// Lost the rlock race; produce a candidate slice and re-check under wlock.
    const auto newSlices = createNewSlice(sliceStart, sliceEnd);
    INVARIANT(newSlices.size() == 1, "Interval-join slice store creates exactly one slice per timestamp; got {}", newSlices.size());

    auto slicesWriteLocked = slices.wlock();
    if (const auto it = slicesWriteLocked->find(sliceEnd); it != slicesWriteLocked->end())
    {
        return {it->second};
    }
    auto newSlice = std::dynamic_pointer_cast<IntervalJoinSlice>(newSlices[0]);
    INVARIANT(newSlice != nullptr, "Interval-join slice store expects IntervalJoinSlice instances");
    slicesWriteLocked->emplace(sliceEnd, newSlice);
    return {newSlice};
}

std::optional<std::shared_ptr<Slice>> IntervalSliceStore::getSliceBySliceEnd(const SliceEnd sliceEnd)
{
    const auto slicesReadLocked = slices.rlock();
    if (const auto it = slicesReadLocked->find(sliceEnd); it != slicesReadLocked->end())
    {
        return std::static_pointer_cast<Slice>(it->second);
    }
    return std::nullopt;
}

std::vector<std::shared_ptr<IntervalJoinSlice>> IntervalSliceStore::claimTriggerable(const Timestamp triggerWatermark)
{
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimed;
    const auto slicesReadLocked = slices.rlock();
    for (const auto& [sliceEnd, slicePtr] : *slicesReadLocked)
    {
        if (sliceEnd > triggerWatermark)
        {
            /// Map is sorted; everything beyond the watermark stays.
            break;
        }
        if (slicePtr->claimTriggered())
        {
            claimed.emplace_back(slicePtr);
        }
    }
    return claimed;
}

std::vector<std::shared_ptr<IntervalJoinSlice>> IntervalSliceStore::claimAllNonTriggered()
{
    std::vector<std::shared_ptr<IntervalJoinSlice>> claimed;
    const auto slicesReadLocked = slices.rlock();
    for (const auto& [sliceEnd, slicePtr] : *slicesReadLocked)
    {
        if (slicePtr->claimTriggered())
        {
            claimed.emplace_back(slicePtr);
        }
    }
    return claimed;
}

void IntervalSliceStore::garbageCollectTriggered(const Timestamp gcWatermark)
{
    std::vector<std::shared_ptr<IntervalJoinSlice>> slicesToDelete;
    {
        auto slicesWriteLocked = slices.wlock();
        for (auto it = slicesWriteLocked->begin(); it != slicesWriteLocked->end();)
        {
            const auto& [sliceEnd, slicePtr] = *it;
            if (sliceEnd >= gcWatermark)
            {
                /// Map is sorted; everything from here onward has not yet expired.
                break;
            }
            if (slicePtr->isTriggered())
            {
                slicesToDelete.emplace_back(slicePtr);
                it = slicesWriteLocked->erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
    /// Destruct outside the lock.
    slicesToDelete.clear();
}

void IntervalSliceStore::garbageCollectExpired(const Timestamp gcWatermark)
{
    std::vector<std::shared_ptr<IntervalJoinSlice>> slicesToDelete;
    {
        auto slicesWriteLocked = slices.wlock();
        for (auto it = slicesWriteLocked->begin(); it != slicesWriteLocked->end();)
        {
            const auto& [sliceEnd, slicePtr] = *it;
            if (sliceEnd >= gcWatermark)
            {
                break;
            }
            slicesToDelete.emplace_back(slicePtr);
            it = slicesWriteLocked->erase(it);
        }
    }
    slicesToDelete.clear();
}

std::span<std::byte>
IntervalSliceStore::allocateSpaceForSliceCache(uint64_t sliceCacheMemorySize, PipelineId pipelineId, AbstractBufferProvider& bufferProvider)
{
    INVARIANT(
        not pipelineIdToSliceCacheStarts.rlock()->contains(pipelineId), "Slice cache for pipelineId {} already allocated", pipelineId);

    auto buffer = bufferProvider.getUnpooledBuffer(sliceCacheMemorySize);
    if (not buffer.has_value())
    {
        throw BufferAllocationFailure("Cannot allocate buffer for interval-join slice cache of size {}", sliceCacheMemorySize);
    }

    std::ranges::fill(buffer.value().getAvailableMemoryArea(), std::byte{0});
    auto sliceCacheStartBuffer = std::make_unique<TupleBuffer>(buffer.value());
    const auto memArea = sliceCacheStartBuffer->getAvailableMemoryArea();
    pipelineIdToSliceCacheStarts.wlock()->emplace(pipelineId, std::move(sliceCacheStartBuffer));
    return memArea;
}

void IntervalSliceStore::incrementNumberOfInputPipelines()
{
    numberOfActiveInputPipelines.fetch_add(1, std::memory_order_acq_rel);
}

bool IntervalSliceStore::decrementAndCheckIfLastPipeline() noexcept
{
    INVARIANT(
        numberOfActiveInputPipelines.load(std::memory_order_acquire) > 0,
        "decrementAndCheckIfLastPipeline called with zero active input pipelines");
    return numberOfActiveInputPipelines.fetch_sub(1, std::memory_order_acq_rel) == 1;
}

uint64_t IntervalSliceStore::getWindowSize() const
{
    return sliceWidth;
}

void IntervalSliceStore::deleteState()
{
    auto slicesWriteLocked = slices.wlock();
    slicesWriteLocked->clear();
}

}
