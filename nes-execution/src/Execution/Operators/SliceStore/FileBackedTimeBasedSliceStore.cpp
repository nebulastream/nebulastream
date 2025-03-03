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
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <Execution/Operators/SliceStore/FileBackedTimeBasedSliceStore.hpp>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <Util/Locks.hpp>
#include <folly/Synchronized.h>

namespace NES::Runtime::Execution
{
FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(
    const uint64_t windowSize, const uint64_t windowSlide, const uint8_t numberOfInputOrigins)
    : sliceAssigner(windowSize, windowSlide), sequenceNumber(SequenceNumber::INITIAL), numberOfInputOrigins(numberOfInputOrigins)
{
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(const FileBackedTimeBasedSliceStore& other)
    : sliceAssigner(other.sliceAssigner), sequenceNumber(other.sequenceNumber.load()), numberOfInputOrigins(other.numberOfInputOrigins)
{
}

FileBackedTimeBasedSliceStore::FileBackedTimeBasedSliceStore(FileBackedTimeBasedSliceStore&& other) noexcept
    : sliceAssigner(std::move(other.sliceAssigner))
    , sequenceNumber(std::move(other.sequenceNumber.load()))
    , numberOfInputOrigins(std::move(other.numberOfInputOrigins))
{
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(const FileBackedTimeBasedSliceStore& other)
{
    sliceAssigner = other.sliceAssigner;
    sequenceNumber = other.sequenceNumber.load();
    numberOfInputOrigins = other.numberOfInputOrigins;
    return *this;
}

FileBackedTimeBasedSliceStore& FileBackedTimeBasedSliceStore::operator=(FileBackedTimeBasedSliceStore&& other) noexcept
{
    sliceAssigner = std::move(other.sliceAssigner);
    sequenceNumber = std::move(other.sequenceNumber.load());
    numberOfInputOrigins = std::move(other.numberOfInputOrigins);
    return *this;
}

std::vector<WindowInfo> FileBackedTimeBasedSliceStore::getAllWindowInfosForSlice(const Slice& slice) const
{
    std::vector<WindowInfo> allWindows;

    const auto sliceStart = slice.getSliceStart().getRawValue();
    const auto sliceEnd = slice.getSliceEnd().getRawValue();
    const auto windowSize = sliceAssigner.getWindowSize();
    const auto windowSlide = sliceAssigner.getWindowSlide();

    const auto firstWindowEnd = sliceEnd;
    const auto lastWindowEnd = sliceStart + windowSize;

    for (auto curWindowEnd = firstWindowEnd; curWindowEnd <= lastWindowEnd; curWindowEnd += windowSlide)
    {
        allWindows.emplace_back(curWindowEnd - windowSize, curWindowEnd);
    }

    return allWindows;
}

FileBackedTimeBasedSliceStore::~FileBackedTimeBasedSliceStore()
{
    deleteState();
}

std::vector<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSlicesOrCreate(
    const Timestamp timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>& createNewSlice)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);

    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);

    if (slicesWriteLocked->contains(sliceEnd))
    {
        // TODO we don't need to actually fetch slice as we only want to write to it in build
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
FileBackedTimeBasedSliceStore::getTriggerableWindowSlices(Timestamp globalWatermark)
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
            // TODO fetch slice from main mem or do it in getSliceBySliceEnd() which is called from probe
            windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
        }
    }
    return windowsToSlices;
}

std::optional<std::shared_ptr<Slice>> FileBackedTimeBasedSliceStore::getSliceBySliceEnd(SliceEnd sliceEnd)
{
    if (const auto slicesReadLocked = slices.rlock(); slicesReadLocked->contains(sliceEnd))
    {
        // TODO fetch slice if not done in getTriggerableWindowSlices() or getAllNonTriggeredSlices()
        auto slice = slicesReadLocked->find(sliceEnd)->second;
        readSliceFromFiles(slice);
        return slice;
    }
    return {};
}

std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> FileBackedTimeBasedSliceStore::getAllNonTriggeredSlices()
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
                    // TODO fetch slice from main mem or do it in getSliceBySliceEnd() which is called from probe
                    windowsToSlices[{windowInfo, newSequenceNumber}].emplace_back(slice);
                }
            }
        }
    }

    return windowsToSlices;
}

void FileBackedTimeBasedSliceStore::garbageCollectSlicesAndWindows(const Timestamp newGlobalWaterMark)
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
            // TODO delete state from ssd if there is any
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
            // TODO delete state from ssd if there is any
            slicesWriteLocked->erase(slicesLockedIt++);
        }
        else
        {
            /// As the slices are sorted (due to std::map), we can break here as we will not find any slices with a smaller slice end
            break;
        }
    }
}

void FileBackedTimeBasedSliceStore::deleteState()
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    slicesWriteLocked->clear();
    windowsWriteLocked->clear();
    // TODO delete state from ssd if there is any
    // TODO clear memCtrl
}

uint64_t FileBackedTimeBasedSliceStore::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}

void FileBackedTimeBasedSliceStore::updateSlices(const SliceStoreMetaData metaData)
{
    const auto threadId = metaData.threadId;

    auto slicesLocked = slices.rlock();
    for (const auto& [sliceEnd, slice] : *slicesLocked)
    {
        auto leftFileWriter = memCtrl.getLeftFileWriter(sliceEnd, threadId);
        auto rightFileWriter = memCtrl.getRightFileWriter(sliceEnd, threadId);
        slice->writeToFile(leftFileWriter, rightFileWriter, threadId);
        slice->truncate(threadId);
    }

    // TODO predictiveRead()
}

void FileBackedTimeBasedSliceStore::readSliceFromFiles(std::shared_ptr<Slice> slice) const
{
    for (auto threadId = 0UL; threadId < slice->getNumberOfWorkerThreads(); ++threadId)
    {
        auto leftFileReader = memCtrl.getLeftFileReader(slice->getSliceEnd(), WorkerThreadId(threadId));
        auto rightFileReader = memCtrl.getRightFileReader(slice->getSliceEnd(), WorkerThreadId(threadId));
        slice->readFromFile(leftFileReader, rightFileReader, WorkerThreadId(threadId));
    }
}

}
