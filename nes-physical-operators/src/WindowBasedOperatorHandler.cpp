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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <ranges>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

WindowBasedOperatorHandler::WindowBasedOperatorHandler(
    SourcesOfInputOrigins sourcesOfInputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : sliceAndWindowStore(std::move(sliceAndWindowStore))
    , watermarkProcessorBuild(
          std::make_unique<MultiOriginWatermarkProcessor>(sourcesOfInputOrigins | std::views::keys | std::ranges::to<std::vector>()))
    , watermarkProcessorProbe(std::make_unique<MultiOriginWatermarkProcessor>(std::vector{outputOriginId}))
    , outputOriginId(outputOriginId)
    , sourcesOfInputOrigins(std::move(sourcesOfInputOrigins))
{
}

void WindowBasedOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext)
{
    numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();

    auto backpressure = pipelineExecutionContext.getWatermarkBackpressure();
    const auto allSources = sourcesOfInputOrigins | std::views::values | std::views::join;
    if (backpressure && sourcesOfInputOrigins.size() > 1
        && std::ranges::all_of(allSources, [&](const auto source) { return backpressure->controller.controls(source); }))
    {
        const std::scoped_lock lock(triggerMutex);
        watermarkBackpressure = std::move(backpressure);
    }
}

void WindowBasedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
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
    const std::scoped_lock lock(triggerMutex);
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
    applyWatermarkBackpressure();

    /// Window results use the window start as their timestamp. An input watermark can therefore only
    /// advance the output watermark past starts whose entire window has already closed.
    const auto windowSize = sliceAndWindowStore->getWindowSize();
    const auto outputWatermark = newGlobalWatermark.saturatingSubtract(windowSize);
    if (outputWatermark > lastForwardedWatermark)
    {
        auto watermarkBuffer = pipelineCtx->allocateTupleBuffer();
        watermarkBuffer.setNumberOfTuples(0);
        watermarkBuffer.setOriginId(outputOriginId);
        watermarkBuffer.setSequenceNumber(sliceAndWindowStore->nextSequenceNumber());
        watermarkBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        watermarkBuffer.setLastChunk(true);
        watermarkBuffer.setWatermark(outputWatermark);
        watermarkBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
        pipelineCtx->emitBuffer(watermarkBuffer);
        lastForwardedWatermark = outputWatermark;
    }
}

void WindowBasedOperatorHandler::applyWatermarkBackpressure()
{
    if (!watermarkBackpressure)
    {
        return;
    }
    auto& [controller, maxWatermarkGap] = *watermarkBackpressure;
    const auto watermarks = watermarkProcessorBuild->getCurrentWatermarkPerOrigin();
    const auto [slowestOrigin, slowest]
        = std::ranges::min(watermarks, {}, [](const auto& originAndWatermark) { return originAndWatermark.second; });

    /// Releasing at half the gap avoids toggling on every buffer.
    for (const auto& [origin, watermark] : watermarks)
    {
        const auto gap = watermark.getRawValue() - slowest.getRawValue();
        if (watermark.getRawValue() == Timestamp::INFINITE_VALUE || gap <= maxWatermarkGap / 2)
        {
            throttledInputOrigins.erase(origin);
        }
        else if (gap > maxWatermarkGap)
        {
            throttledInputOrigins.insert(origin);
        }
    }

    /// Sources that drive the slowest origin are never throttled, which guarantees progress even if a source drives several origins.
    const auto& sourcesOfSlowest = sourcesOfInputOrigins.at(slowestOrigin);
    std::unordered_set<OriginId> sourcesToThrottle;
    for (const auto origin : throttledInputOrigins)
    {
        for (const auto source : sourcesOfInputOrigins.at(origin))
        {
            if (!std::ranges::contains(sourcesOfSlowest, source))
            {
                sourcesToThrottle.insert(source);
            }
        }
    }

    for (const auto source : throttledSources)
    {
        if (!sourcesToThrottle.contains(source) && controller.releasePressure(source))
        {
            NES_DEBUG("Released watermark backpressure on source {}: slowest origin {} at {}", source, slowestOrigin, slowest);
        }
    }
    for (const auto source : sourcesToThrottle)
    {
        if (controller.applyPressure(source))
        {
            NES_DEBUG("Applied watermark backpressure on source {}: slowest origin {} at {}", source, slowestOrigin, slowest);
        }
    }
    throttledSources = std::move(sourcesToThrottle);
}

}
