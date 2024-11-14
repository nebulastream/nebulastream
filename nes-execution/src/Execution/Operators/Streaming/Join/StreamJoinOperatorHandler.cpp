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
#include <optional>
#include <ranges>
#include <vector>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators
{

StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    const uint64_t windowSize,
    const uint64_t windowSlide,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider)
    : watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins))
    , watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector(1, outputOriginId)))
    , outputOriginId(outputOriginId)
    , numberOfWorkerThreads(1)
    , sliceAssigner(windowSize, windowSlide)
    , sequenceNumber(INITIAL_SEQ_NUMBER.getRawValue())
    , leftMemoryProvider(leftMemoryProvider)
    , rightMemoryProvider(rightMemoryProvider)
{
}

void StreamJoinOperatorHandler::setWorkerThreads(const uint64_t numberOfWorkerThreads)
{
    StreamJoinOperatorHandler::numberOfWorkerThreads = numberOfWorkerThreads;
}

void StreamJoinOperatorHandler::setBufferProvider(std::shared_ptr<Memory::AbstractBufferProvider> bufferProvider)
{
    StreamJoinOperatorHandler::bufferProvider = bufferProvider;
}

void StreamJoinOperatorHandler::start(const PipelineExecutionContextPtr, uint32_t)
{
    NES_INFO("Started StreamJoinOperatorHandler!");
}

void StreamJoinOperatorHandler::stop(const QueryTerminationType, const PipelineExecutionContextPtr)
{
    NES_INFO("Stopped StreamJoinOperatorHandler!");
}

std::shared_ptr<StreamJoinSlice> StreamJoinOperatorHandler::getSliceByTimestampOrCreateIt(const Timestamp timestamp)
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);

    const auto sliceStart = sliceAssigner.getSliceStartTs(timestamp);
    const auto sliceEnd = sliceAssigner.getSliceEndTs(timestamp);

    if (slicesWriteLocked->contains(sliceEnd))
    {
        return slicesWriteLocked->find(sliceEnd)->second;
    }

    auto newSlice = createNewSlice(sliceStart, sliceEnd);
    slicesWriteLocked->emplace(sliceEnd, newSlice);

    /// Update the state of all windows that contain this slice as we have to expect new tuples
    for (auto windowInfo : getAllWindowInfosForSlice(*newSlice))
    {
        auto& [windowSlices, windowState] = (*windowsWriteLocked)[windowInfo];
        windowState = WindowInfoState::BOTH_SIDES_FILLING;
        windowSlices.emplace_back(newSlice);
    }

    return newSlice;
}

std::optional<std::shared_ptr<StreamJoinSlice>> StreamJoinOperatorHandler::getSliceBySliceEnd(const SliceEnd sliceEnd)
{
    if (const auto slicesReadLocked = slices.rlock(); slicesReadLocked->contains(sliceEnd))
    {
        return slicesReadLocked->find(sliceEnd)->second;
    }
    return {};
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// The watermark processor handles the minimal watermark across both streams
    const auto newGlobalWatermark
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);

    /// We are iterating over all windows and check if they can be triggered
    /// A window can be triggered if both sides have been filled and the window end is smaller than the new global watermark
    const auto windowsWriteLocked = windows.wlock();
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        if (windowInfo.windowEnd > newGlobalWatermark || windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            /// This window can not be triggered yet or has already been triggered
            continue;
        }

        windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
        for (auto& sliceLeft : windowSlicesAndState.windowSlices)
        {
            for (auto& sliceRight : windowSlicesAndState.windowSlices)
            {
                emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
            }
        }
    }
}

void StreamJoinOperatorHandler::triggerAllWindows(PipelineExecutionContext* pipelineCtx)
{
    const auto windowsWriteLocked = windows.wlock();
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        switch (windowSlicesAndState.windowState)
        {
            case WindowInfoState::BOTH_SIDES_FILLING:
                windowSlicesAndState.windowState = WindowInfoState::ONCE_SEEN_DURING_TERMINATION;
            case WindowInfoState::EMITTED_TO_PROBE:
            case WindowInfoState::CAN_BE_DELETED:
                continue;
            case WindowInfoState::ONCE_SEEN_DURING_TERMINATION: {
                windowSlicesAndState.windowState = WindowInfoState::EMITTED_TO_PROBE;
                for (auto& sliceLeft : windowSlicesAndState.windowSlices)
                {
                    for (auto& sliceRight : windowSlicesAndState.windowSlices)
                    {
                        emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, pipelineCtx);
                    }
                }
            }
        }
    }
}

void StreamJoinOperatorHandler::garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData)
{
    const auto newGlobalWaterMarkProbe
        = watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);


    /// 1. We iterate over all windows and set their state to CAN_BE_DELETED if they can be deleted
    /// This condition is true, if the window end is smaller than the new global watermark of the probe phase.
    for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
    {
        if (windowInfo.windowEnd <= newGlobalWaterMarkProbe and windowSlicesAndState.windowState == WindowInfoState::EMITTED_TO_PROBE)
        {
            windowSlicesAndState.windowState = WindowInfoState::CAN_BE_DELETED;
        }
    }

    /// 2. We gather all slices if they are not used in any window that has not been triggered/can not be deleted yet
    std::vector<SliceEnd> slicesToDelete;
    for (auto& [sliceEnd, slice] : *slicesWriteLocked)
    {
        bool sliceUsedInUntriggeredWindow = false;
        for (auto& [windowInfo, windowSlicesAndState] : *windowsWriteLocked)
        {
            if (windowSlicesAndState.windowState != WindowInfoState::CAN_BE_DELETED
                and std::ranges::find(windowSlicesAndState.windowSlices, slice) != windowSlicesAndState.windowSlices.end())
            {
                sliceUsedInUntriggeredWindow = true;
                break;
            }
        }

        if (not sliceUsedInUntriggeredWindow)
        {
            slicesToDelete.emplace_back(sliceEnd);
        }
    }

    /// 3. Remove all slices that are not used in any window that has not been triggered/can not be deleted yet
    for (const auto& sliceEnd : slicesToDelete)
    {
        slicesWriteLocked->erase(sliceEnd);
    }

    /// 4. Remove all windows that can be deleted
    std::erase_if(*windowsWriteLocked, [](const auto& window) { return window.second.windowState == WindowInfoState::CAN_BE_DELETED; });
}

void StreamJoinOperatorHandler::deleteAllSlicesAndWindows()
{
    auto [slicesWriteLocked, windowsWriteLocked] = acquireLocked(slices, windows);
    slicesWriteLocked->clear();
    windowsWriteLocked->clear();
}

uint64_t StreamJoinOperatorHandler::getNumberOfSlices()
{
    return slices.rlock()->size();
}

uint64_t StreamJoinOperatorHandler::getNextSequenceNumber()
{
    return sequenceNumber++;
}

uint64_t StreamJoinOperatorHandler::getWindowSize() const
{
    return sliceAssigner.getWindowSize();
}

uint64_t StreamJoinOperatorHandler::getWindowSlide() const
{
    return sliceAssigner.getWindowSlide();
}

OriginId StreamJoinOperatorHandler::getOutputOriginId() const
{
    return outputOriginId;
}

std::vector<WindowInfo> StreamJoinOperatorHandler::getAllWindowInfosForSlice(const StreamJoinSlice& slice) const
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
}
