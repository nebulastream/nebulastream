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

#include <WindowBasedOperatorHandler.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Spill/SpillManager.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

WindowBasedOperatorHandler::WindowBasedOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : sliceAndWindowStore(std::move(sliceAndWindowStore))
    , watermarkProcessorBuild(std::make_unique<MultiOriginWatermarkProcessor>(inputOrigins))
    , watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector{outputOriginId}))
    , numberOfWorkerThreads(0)
    , outputOriginId(outputOriginId)
    , inputOrigins(inputOrigins)
{
}

void WindowBasedOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext, uint32_t)
{
    numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();
    spillManager = pipelineExecutionContext.getSpillManager();
    if (spillManager != nullptr && spillManager->configuration().enabled)
    {
        pinnedBuildSlicePerWorker.assign(numberOfWorkerThreads, nullptr);
    }
}

void WindowBasedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
    /// Release any slices still pinned by the build path so they can be evicted/destroyed.
    if (spillManager != nullptr)
    {
        for (auto& pinned : pinnedBuildSlicePerWorker)
        {
            if (pinned != nullptr)
            {
                if (auto* spillable = dynamic_cast<SpillableState*>(pinned.get()))
                {
                    spillManager->unpin(*spillable);
                }
                pinned = nullptr;
            }
        }
    }
}

void WindowBasedOperatorHandler::pinSliceForBuild(const WorkerThreadId workerThreadId, const std::shared_ptr<Slice>& slice)
{
    if (spillManager == nullptr || !spillManager->configuration().enabled || pinnedBuildSlicePerWorker.empty())
    {
        return;
    }
    auto* spillable = dynamic_cast<SpillableState*>(slice.get());
    if (spillable == nullptr)
    {
        return;
    }
    auto& pinned = pinnedBuildSlicePerWorker[workerThreadId % pinnedBuildSlicePerWorker.size()];
    if (pinned.get() == slice.get())
    {
        /// Same slice as the previous access: already pinned, nothing to do.
        return;
    }
    /// Moving to a different slice: unpin the previous one, pin (and reload if evicted) the new one.
    if (pinned != nullptr)
    {
        if (auto* previous = dynamic_cast<SpillableState*>(pinned.get()))
        {
            spillManager->unpin(*previous);
        }
    }
    spillManager->pin(*spillable);
    pinned = slice;
    /// State grew (a new slice is in use): give the governor a chance to evict cold, unpinned slices.
    spillManager->maybeSpill();
}

Slice* WindowBasedOperatorHandler::getPinnedBuildSlice(const WorkerThreadId workerThreadId) const
{
    if (pinnedBuildSlicePerWorker.empty())
    {
        return nullptr;
    }
    return pinnedBuildSlicePerWorker[workerThreadId % pinnedBuildSlicePerWorker.size()].get();
}

WindowSlicesStoreInterface& WindowBasedOperatorHandler::getSliceAndWindowStore() const
{
    return *sliceAndWindowStore;
}

void WindowBasedOperatorHandler::garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData) const
{
    const auto newGlobalWaterMarkProbe
        = watermarkProcessorProbe->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);

    NES_TRACE(
        "New global watermark probe: {} for origin: {} and sequence data: {} and watermarkTs of buffer {}",
        newGlobalWaterMarkProbe,
        bufferMetaData.originId,
        bufferMetaData.seqNumber,
        bufferMetaData.watermarkTs);
    sliceAndWindowStore->garbageCollectSlicesAndWindows(newGlobalWaterMarkProbe);
}

void WindowBasedOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// The watermark processor handles the minimal watermark across both streams
    const auto newGlobalWatermark
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);

    NES_TRACE(
        "New global watermark: {} for origin: {} and sequence data: {} and watermarkTs of buffer {}",
        newGlobalWatermark,
        bufferMetaData.originId,
        bufferMetaData.seqNumber,
        bufferMetaData.watermarkTs);

    /// Getting all slices that can be triggered and triggering them
    const auto slicesAndWindowInfo = sliceAndWindowStore->getTriggerableWindowSlices(newGlobalWatermark);
    triggerSlices(slicesAndWindowInfo, pipelineCtx);
}

void WindowBasedOperatorHandler::triggerAllWindows(PipelineExecutionContext* pipelineCtx)
{
    const auto slicesAndWindowInfo = sliceAndWindowStore->getAllNonTriggeredSlices();
    NES_TRACE("Triggering {} windows for origin: {}", slicesAndWindowInfo.size(), outputOriginId);
    triggerSlices(slicesAndWindowInfo, pipelineCtx);
}

}
