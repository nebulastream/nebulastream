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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Execution/Operators/SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/Locks.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution
{
DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(
    const uint64_t windowSize, const uint64_t windowSlide, const uint8_t numberOfInputOrigins)
    : sliceAssigner(windowSize, windowSlide), sequenceNumber(SequenceNumber::INITIAL), numberOfInputOrigins(numberOfInputOrigins)
{
}

DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(const DefaultTimeBasedSliceStore& other)
    : sliceAssigner(other.sliceAssigner), sequenceNumber(other.sequenceNumber.load()), numberOfInputOrigins(other.numberOfInputOrigins)
{
}

DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(DefaultTimeBasedSliceStore&& other) noexcept
    : sliceAssigner(std::move(other.sliceAssigner))
    , sequenceNumber(std::move(other.sequenceNumber.load()))
    , numberOfInputOrigins(std::move(other.numberOfInputOrigins))
{
}

DefaultTimeBasedSliceStore& DefaultTimeBasedSliceStore::operator=(const DefaultTimeBasedSliceStore& other)
{
    sliceAssigner = other.sliceAssigner;
    sequenceNumber = other.sequenceNumber.load();
    numberOfInputOrigins = other.numberOfInputOrigins;
    return *this;
}

DefaultTimeBasedSliceStore& DefaultTimeBasedSliceStore::operator=(DefaultTimeBasedSliceStore&& other) noexcept
{
    sliceAssigner = std::move(other.sliceAssigner);
    sequenceNumber = std::move(other.sequenceNumber.load());
    numberOfInputOrigins = std::move(other.numberOfInputOrigins);
    return *this;
}

std::vector<WindowInfo> DefaultTimeBasedSliceStore::getAllWindowInfosForSlice(const Slice& slice) const
{
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart().getRawValue();
    const auto sliceEnd = slice.getSliceEnd().getRawValue();
    const auto windowSize = sliceAssigner.getWindowSize();
    const auto windowSlide = sliceAssigner.getWindowSlide();

    /// We need to get here the max of the sliceEnd or the window size.
    /// With this, we would not create windows, such as 0-5 for slide 5 and size 100.
    /// In our window model, a window is always at least the size of the window size.
    const auto firstWindowEnd = std::max(sliceEnd, windowSize);
    const auto lastWindowEnd = sliceStart + windowSize;

    for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide)
    {
        allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
    }

    return allWindows;
}

DefaultTimeBasedSliceStore::~DefaultTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> DefaultTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);

    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);

    if (slicesWriteLocked->contains(sliceEnd))
    {
        return {slicesWriteLocked->find(sliceEnd)->second};
    }

    /// We assume that only one slice is created per timestamp
    auto newSlice = createNewSlice(sliceStart, sliceEnd)[0];
    slicesWriteLocked->emplace(sliceEnd, newSlice);

    /// Update the state of all windows that contain this slice as we have to expect new tuples
    for (auto windowInfo : getAllWindowInfosForSlice(*newSlice))
    {
        auto& [windowSlices, windowState] = (*windowsWriteLocked)[windowInfo];
        windowState = WindowInfoState::WINDOW_FILLING;
        windowSlices.emplace_back(newSlice);
    }

    return {newSlice};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
DefaultTimeBasedSliceStore::getTriggerableWindowSlices(Timestamp globalWatermark)
{
    /// We are iterating over all windows and check if they can be triggered
    /// A window can be triggered if both sides have been filled and the window end is smaller than the new global watermark
    const auto windowsWriteLocked = windows.wlock();
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        if (windowInfo.windowEnd > globalWatermark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        if (windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            /// This window can not be triggered yet or has already been triggered
            continue;
        }

        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            /// As the windows are sorted, we can simply increment the sequence number here.
            const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
    }
    return windowsToSlices;
}

std::optional<std::shared_ptr<Slice>> DefaultTimeBasedSliceStore::getSliceBySliceEnd(SliceEnd sliceEnd)
{
    if (const auto slicesReadLocked = slices.rlock(); slicesReadLocked->contains(sliceEnd))
    {
        return slicesReadLocked->find(sliceEnd)->second;
    }
    return {};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> DefaultTimeBasedSliceStore::getAllNonTriggeredSlices()
{
    const auto windowsWriteLocked = windows.wlock();
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        switch (windowSlicesAndState.windowState)
        {
            case WindowInfoState::EMITTED_TO_PROBE:
                continue;
            case WindowInfoState::WINDOW_FILLING: {
                if (numberOfInputOrigins == 2)
                {
                    windowSlicesAndState.windowState = WindowInfoState::ONCE_SEEN_DURING_TERMINATION;
                    continue;
                }
            }
            case WindowInfoState::ONCE_SEEN_DURING_TERMINATION: {
                windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
                for (auto& slice : windowSlicesAndState.windowSlices)
                {
                    /// As the windows are sorted, we can simply increment the sequence number here.
                    const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
                    windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
                }
            }
        }
    }

    return windowsToSlices;
}

void DefaultTimeBasedSliceStore::garbageCollectSlicesAndWindows(const Timestamp newGlobalWaterMark)
{
    auto lockedSlicesAndWindows = tryAcquireLocked(slices, windows);
    if (not lockedSlicesAndWindows)
    {
        /// We could not acquire the lock, so we opt for not performing the garbage collection this time.
        return;
    }
    auto& [slicesWriteLocked, windowsWriteLocked] = *lockedSlicesAndWindows;

    /// 1. We iterate over all windows and set their state to CAN_BE_DELETED if they can be deleted
    /// This condition is true, if the window end is smaller than the new global watermark of the probe phase.
    for (auto windowsLockedIt = windowsWriteLocked->cbegin(); windowsLockedIt != windowsWriteLocked->cend();)
    {
        const auto& [windowInfo, windowSlicesAndState] = *windowsLockedIt;
        if (windowInfo.windowEnd <= newGlobalWaterMark and windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            windowsWriteLocked->erase(windowsLockedIt++);
        }
        else if (windowInfo.windowEnd > newGlobalWaterMark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        else
        {
            ++windowsLockedIt;
        }
    }

    /// 2. We gather all slices if they are not used in any window that has not been triggered/can not be deleted yet
    std::vector<SliceEnd> const slicesToDelete;
    for (auto slicesLockedIt = slicesWriteLocked->cbegin(); slicesLockedIt != slicesWriteLocked->cend();)
    {
        const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
        if (sliceEnd + sliceAssigner.getWindowSize() <= newGlobalWaterMark)
        {
            slicesWriteLocked->erase(slicesLockedIt++);
        }
        else
        {
            /// As the slices are sorted (due to std::map), we can break here as we will not find any slices with a smaller slice end
            break;
        }
    }
}

void DefaultTimeBasedSliceStore::deleteState()
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    slicesWriteLocked->clear();
    windowsWriteLocked->clear();
}

uint64_t DefaultTimeBasedSliceStore::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}
}
