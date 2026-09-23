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
    , outputOriginId(outputOriginId)
    , inputOrigins(inputOrigins)
{
}

void WindowBasedOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext)
{
    numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();
}

void WindowBasedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

WindowSlicesStoreInterface& WindowBasedOperatorHandler::getSliceAndWindowStore() const
{
    return *sliceAndWindowStore;
}

void WindowBasedOperatorHandler::garbageCollectSlicesAndWindows(BufferMetaData bufferMetaData) const
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

void WindowBasedOperatorHandler::checkAndTriggerWindows(BufferMetaData bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// Barriers carried by this tuple buffer are stored under its watermark, so they can be emitted once the corresponding window triggers.
    if (!bufferMetaData.barriers.empty())
    {
        auto wlocked = barriers.wlock();
        auto& entry = (*wlocked)[bufferMetaData.watermarkTs];
        entry.insert(
            entry.end(),
            std::make_move_iterator(bufferMetaData.barriers.begin()),
            std::make_move_iterator(bufferMetaData.barriers.end()));
    }

    /// The watermark processor handles the minimal watermark across both streams
    const auto watermarkProcessorRes
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);

    NES_TRACE(
        "New global watermark: {} for origin: {} and sequence data: {} and watermarkTs of buffer {}",
        watermarkProcessorRes,
        bufferMetaData.originId,
        bufferMetaData.seqNumber,
        bufferMetaData.watermarkTs);

    /// Getting all slices that can be triggered and triggering them
    const auto slicesAndWindowInfo = sliceAndWindowStore->getTriggerableWindowSlices(watermarkProcessorRes);

    // Collect all pending barriers that are < the start time of the last window emitted in this step
    std::vector<std::string> collectedBarriers;
    if (!slicesAndWindowInfo.empty())
    {
        const auto windowStart = slicesAndWindowInfo.rbegin()->first.windowInfo.windowStart;

        auto wlocked = barriers.wlock();
        auto it = wlocked->begin();
        while (it != wlocked->end() && it->first < windowStart)
        {
            collectedBarriers.insert(
                collectedBarriers.end(), std::make_move_iterator(it->second.begin()), std::make_move_iterator(it->second.end()));
            it = wlocked->erase(it);
        }
    }

    triggerSlices(slicesAndWindowInfo, std::move(collectedBarriers), pipelineCtx);
}

void WindowBasedOperatorHandler::triggerAllWindows(PipelineExecutionContext* pipelineCtx)
{
    const auto slicesAndWindowInfo = sliceAndWindowStore->getAllNonTriggeredSlices();
    NES_TRACE("Triggering {} windows for origin: {}", slicesAndWindowInfo.size(), outputOriginId);
    triggerSlices(slicesAndWindowInfo, {}, pipelineCtx);
}

}
