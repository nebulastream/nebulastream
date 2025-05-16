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
#include <Identifiers/Identifiers.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Locks.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{
DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(
    const uint64_t windowSize, const uint64_t windowSlide, const uint8_t numberOfInputOrigins)
    : sliceAssigner(windowSize, windowSlide), sequenceNumber(SequenceNumber::INITIAL), numberOfActiveOrigins(numberOfInputOrigins)
{
}

DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(const DefaultTimeBasedSliceStore& other)
    : sliceAssigner(other.sliceAssigner), sequenceNumber(other.sequenceNumber.load()), numberOfActiveOrigins(other.numberOfActiveOrigins)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesReadLocked, otherWindowsReadLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = *otherSlicesReadLocked;
    *windowsWriteLocked = *otherWindowsReadLocked;
}

DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(DefaultTimeBasedSliceStore&& other) noexcept
    : sliceAssigner(std::move(other.sliceAssigner))
    , sequenceNumber(std::move(other.sequenceNumber.load()))
    , numberOfActiveOrigins(std::move(other.numberOfActiveOrigins))
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = std::move(*otherSlicesWriteLocked);
    *windowsWriteLocked = std::move(*otherWindowsWriteLocked);
}

DefaultTimeBasedSliceStore& DefaultTimeBasedSliceStore::operator=(const DefaultTimeBasedSliceStore& other)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesReadLocked, otherWindowsReadLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = *otherSlicesReadLocked;
    *windowsWriteLocked = *otherWindowsReadLocked;

    sliceAssigner = other.sliceAssigner;
    sequenceNumber = other.sequenceNumber.load();
    numberOfActiveOrigins = other.numberOfActiveOrigins;
    return *this;
}

DefaultTimeBasedSliceStore& DefaultTimeBasedSliceStore::operator=(DefaultTimeBasedSliceStore&& other) noexcept
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    auto [otherSlicesWriteLocked, otherWindowsWriteLocked] = acquireLocked(other.slices, other.windows);
    *slicesWriteLocked = std::move(*otherSlicesWriteLocked);
    *windowsWriteLocked = std::move(*otherWindowsWriteLocked);

    sliceAssigner = std::move(other.sliceAssigner);
    sequenceNumber = std::move(other.sequenceNumber.load());
    numberOfActiveOrigins = std::move(other.numberOfActiveOrigins);
    return *this;
}

std::vector<WindowInfo> DefaultTimeBasedSliceStore::getAllWindowInfosForSlice(const Slice& slice) const
{
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart().getRawValue();
    const auto sliceEnd = slice.getSliceEnd().getRawValue();
    const auto windowSize = sliceAssigner.getWindowSize();
    const auto windowSlide = sliceAssigner.getWindowSlide();

    /// Taking the max out of sliceEnd and windowSize, allows us to not create windows, such as 0-5 for slide 5 and size 100.
    /// In our window model, a window is always the size of the window size.
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
    const auto newSlices = createNewSlice(sliceStart, sliceEnd);
    INVARIANT(newSlices.size() == 1, "We assume that only one slice is created per timestamp for our default time-based slice store.");
    auto newSlice = newSlices[0];
    slicesWriteLocked->emplace(sliceEnd, newSlice);

    /// Update the state of all windows that contain this slice as we have to expect new tuples
    for (auto windowInfo : getAllWindowInfosForSlice(*newSlice))
    {
        auto& [windowSlices, windowState] = (*windowsWriteLocked)[windowInfo];
        INVARIANT(
            windowState != WindowInfoState::EMITTED_TO_PROBE, "We should not add slices to a window that has already been triggered.");
        windowState = WindowInfoState::WINDOW_FILLING;
        windowSlices.emplace_back(newSlice);
    }

    return {newSlice};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
DefaultTimeBasedSliceStore::getTriggerableWindowSlices(const Timestamp globalWatermark)
{
    /// We are iterating over all windows and check if they can be triggered
    /// A window can be triggered if both sides have been filled and the window end is smaller than the new global watermark
    const auto windowsWriteLocked = windows.wlock();
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        if (windowInfo.windowEnd >= globalWatermark)
        {
            /// As the windows are sorted (due to std::map), we can break here as we will not find any windows with a smaller window end
            break;
        }
        if (windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            /// This window has already been triggered
            continue;
        }

        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
        /// As the windows are sorted, we can simply increment the sequence number here.
        const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
    }
    return windowsToSlices;
}

std::optional<std::shared_ptr<Slice>> DefaultTimeBasedSliceStore::getSliceBySliceEnd(const SliceEnd sliceEnd)
{
    if (const auto slicesReadLocked = slices.rlock(); slicesReadLocked->contains(sliceEnd))
    {
        return slicesReadLocked->find(sliceEnd)->second;
    }
    return {};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> DefaultTimeBasedSliceStore::getAllNonTriggeredSlices()
{
    /// Acquiring a lock for the windows, as we have to iterate over all windows and trigger all non-triggered windows
    const auto windowsWriteLocked = windows.wlock();

    /// numberOfActiveOrigins is guarded by the windows lock.
    /// If this method gets called, we know that an origin has terminated.
    INVARIANT(numberOfActiveOrigins > 0, "Method should not be called if all origin have terminated.");
    --numberOfActiveOrigins;

    /// Creating a lambda to add all slices to the return map windowsToSlices
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> windowsToSlices;
    auto addAllSlicesToReturnMap = [&windowsToSlices, this](const WindowInfo& windowInfo, SlicesAndState& windowSlicesAndState)
    {
        const auto newSequenceNumber = SequenceNumber(sequenceNumber++);
        for (auto& slice : windowSlicesAndState.windowSlices)
        {
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
    };

    /// We are iterating over all windows and check if they can be triggered
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        switch (windowSlicesAndState.windowState)
        {
            case WindowInfoState::EMITTED_TO_PROBE:
                continue;
            case WindowInfoState::WINDOW_FILLING: {
                /// If we are waiting on more than one origin to terminate, we can not trigger the window yet
                if (numberOfActiveOrigins > 0)
                {
                    windowSlicesAndState.windowState = WindowInfoState::WAITING_ON_TERMINATION;
                    NES_TRACE(
                        "Waiting on termination for window end {} and number of origins terminated {}",
                        windowInfo.windowEnd,
                        numberOfActiveOrigins);
                    break;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
            }
            case WindowInfoState::WAITING_ON_TERMINATION: {
                /// Checking if all origins have terminated (i.e., the number of origins terminated is 0, as we will decrement it during fetch_sub)
                NES_TRACE(
                    "Checking if all origins have terminated for window with window end {} and number of origins terminated {}",
                    windowInfo.windowEnd,
                    numberOfActiveOrigins);
                if (numberOfActiveOrigins > 0)
                {
                    continue;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
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

    NES_TRACE("Performing garbage collection for new global watermark {}", newGlobalWaterMark);

    /// 1. We iterate over all windows and erase them if they can be deleted
    /// This condition is true, if the window end is smaller than the new global watermark of the probe phase.
    for (auto windowsLockedIt = windowsWriteLocked->cbegin(); windowsLockedIt != windowsWriteLocked->cend();)
    {
        const auto& [windowInfo, windowSlicesAndState] = *windowsLockedIt;
        if (windowInfo.windowEnd < newGlobalWaterMark and windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            windowsLockedIt = windowsWriteLocked->erase(windowsLockedIt);
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
    for (auto slicesLockedIt = slicesWriteLocked->begin(); slicesLockedIt != slicesWriteLocked->end();)
    {
        const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
        if (sliceEnd + sliceAssigner.getWindowSize() < newGlobalWaterMark)
        {
            NES_TRACE("Deleting slice with sliceEnd {} as it is not used anymore", sliceEnd);
            slicesLockedIt = slicesWriteLocked->erase(slicesLockedIt);
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
