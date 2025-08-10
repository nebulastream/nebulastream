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

#include <SliceStore/DefaultTimeBasedSliceStore.hpp>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceAssigner.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Locks.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{
DefaultTimeBasedSliceStore::DefaultTimeBasedSliceStore(const uint64_t windowSize, const uint64_t windowSlide)
    : sliceAssigner(windowSize, windowSlide), sequenceNumber(SequenceNumber::INITIAL), numberOfActiveInputPipelines(0)
{
}

DefaultTimeBasedSliceStore::~DefaultTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> DefaultTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    /// We first check, if the slice already exist in the slice store
    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);
    {
        const auto slicesWriteLocked = slices.rlock();
        if (const auto existingSlice = slicesWriteLocked->find(sliceEnd); existingSlice != slicesWriteLocked->end())
        {
            return {existingSlice->second};
        }
    }

    /// The current thread has not found a slice, so we need to create one.
    /// It might have happened that another thread acquires the lock before the current thread is finished creating the new slices.
    /// But by not locking the slice store, we reduce the time the current thread holds the lock, increasing the performance.
    /// Therefore, we need to perform another check.
    const auto newSlices = createNewSlice(sliceStart, sliceEnd);
    INVARIANT(newSlices.size() == 1, "We assume that only one slice is created per timestamp for our default time-based slice store.");
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    if (slicesWriteLocked->contains(sliceEnd))
    {
        return {slicesWriteLocked->find(sliceEnd)->second};
    }

    /// At this moment, we can be sure that no slice exists and we can insert the newly created slice into the slice store
    auto newSlice = newSlices[0];
    slicesWriteLocked->emplace(sliceEnd, newSlice);
    slicesWriteLocked.unlock();

    /// Update the state of all windows that contain this slice as we have to expect new tuples
    for (auto windowInfo : sliceAssigner.getAllWindowsForSlice(*newSlice))
    {
        const auto numberOfExpectedSlices = sliceAssigner.getWindowSize() / sliceAssigner.getWindowSlide();
        const auto [it, success] = windowsWriteLocked->try_emplace(windowInfo, numberOfExpectedSlices);
        INVARIANT(
            it->second.windowState != WindowInfoState::EMITTED_TO_PROBE,
            "We should not add slices to a window that has already been triggered.");
        it->second.windowState = WindowInfoState::WINDOW_FILLING;
        it->second.windowSlices.emplace_back(newSlice);
    }

    return {newSlice};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>
DefaultTimeBasedSliceStore::getTriggerableWindowSlices(const Timestamp globalWatermark)
{
    /// For performance reasons, we check if we can acquire a lock and if not we then simply skip checking if we can trigger anything
    const auto windowsWriteLocked = windows.tryWLock();
    if (windowsWriteLocked.isNull())
    {
        return {};
    }

    /// We are iterating over all windows and check if they can be triggered
    /// A window can be triggered if all sides have been filled and the window end is smaller than the new global watermark
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

    /// numberOfActiveInputPipelines is guarded by the windows lock.
    /// If this method gets called, we know that an input pipeline has terminated.
    INVARIANT(numberOfActiveInputPipelines > 0, "Method should not be called if all input pipelines have terminated.");
    numberOfActiveInputPipelines -= 1;

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
                /// If we are waiting on another pipeline to terminate, we can not trigger the window yet
                if (numberOfActiveInputPipelines > 0)
                {
                    windowSlicesAndState.windowState = WindowInfoState::WAITING_ON_TERMINATION;
                    NES_TRACE(
                        "Waiting on termination for window end {} and number of active input pipelines {}",
                        windowInfo.windowEnd,
                        numberOfActiveInputPipelines);
                    break;
                }
                addAllSlicesToReturnMap(windowInfo, windowSlicesAndState);
                break;
            }
            case WindowInfoState::WAITING_ON_TERMINATION: {
                /// Checking if all input pipelines have terminated (i.e., the number of active input pipelines is 0, as we will decrement it during fetch_sub)
                NES_TRACE(
                    "Checking if all input pipelines have terminated for window with window end {} and number of active pipelines {}",
                    windowInfo.windowEnd,
                    numberOfActiveInputPipelines);
                if (numberOfActiveInputPipelines > 0)
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
    std::vector<std::shared_ptr<Slice>> slicesToDelete;
    {
        NES_TRACE("Performing garbage collection for new global watermark {}", newGlobalWaterMark);

        {
            /// Solely acquiring a lock for the windows
            if (const auto windowsWriteLocked = windows.tryWLock())
            {
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
            }
        }

        {
            /// Solely acquiring a lock for the slices
            if (const auto slicesWriteLocked = slices.tryWLock())
            {
                /// 2. We gather all slices if they are not used in any window that has not been triggered/can not be deleted yet
                for (auto slicesLockedIt = slicesWriteLocked->begin(); slicesLockedIt != slicesWriteLocked->end();)
                {
                    const auto& [sliceEnd, slicePtr] = *slicesLockedIt;
                    if (sliceEnd + sliceAssigner.getWindowSize() < newGlobalWaterMark)
                    {
                        NES_TRACE("Deleting slice with sliceEnd {} as it is not used anymore", sliceEnd);
                        /// As we are first copying the shared_ptr the destructor of Slice will not be called.
                        /// This allows us to solely collect what slices to delete during holding the lock, while the time-consuming destructor is called without holding any locks
                        slicesToDelete.emplace_back(slicePtr);
                        slicesLockedIt = slicesWriteLocked->erase(slicesLockedIt);
                    }
                    else
                    {
                        /// As the slices are sorted (due to std::map), we can break here as we will not find any slices with a smaller slice end
                        break;
                    }
                }
            }
        }
    }

    /// Now we can remove/call destructor on every slice without still holding the lock
    slicesToDelete.clear();
}

void DefaultTimeBasedSliceStore::deleteState()
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    slicesWriteLocked->clear();
    windowsWriteLocked->clear();
}

void DefaultTimeBasedSliceStore::incrementNumberOfInputPipelines()
{
    numberOfActiveInputPipelines += 1;
}

uint64_t DefaultTimeBasedSliceStore::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}
}
