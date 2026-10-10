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
#include <ErrorHandling.hpp>
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
        const std::scoped_lock lock(watermarkBackpressureMutex);
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

    /// A buffer in a build operator may still write to every slice at or above its lower bound.
    auto garbageCollectionWatermark = newGlobalWaterMarkProbe;
    if (const auto lowerBounds = buildLowerBounds.rlock(); not lowerBounds->empty())
    {
        garbageCollectionWatermark = std::min(garbageCollectionWatermark, Timestamp(*lowerBounds->begin()));
    }
    sliceAndWindowStore->garbageCollectSlicesAndWindows(garbageCollectionWatermark);
}

Timestamp WindowBasedOperatorHandler::startBuildBuffer(const OriginId originId) const
{
    /// The watermark must be read while holding the lock, so that a concurrent garbage collection either sees this lower bound
    /// or runs with a watermark that is not larger than it.
    const auto lowerBounds = buildLowerBounds.wlock();
    const auto lowerBound = watermarkProcessorBuild->getCurrentWatermark(originId);
    lowerBounds->insert(lowerBound.getRawValue());
    return lowerBound;
}

void WindowBasedOperatorHandler::finishBuildBuffer(const Timestamp lowerBound) const
{
    const auto lowerBounds = buildLowerBounds.wlock();
    const auto it = lowerBounds->find(lowerBound.getRawValue());
    INVARIANT(it != lowerBounds->end(), "No build buffer was started with the lower bound {}", lowerBound);
    lowerBounds->erase(it);
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

    /// The gap between the origins also changes when the global watermark does not advance.
    applyWatermarkBackpressure();

    auto highestWatermark = highestTriggeredWatermark.load();
    while (newGlobalWatermark.getRawValue() > highestWatermark
           and not highestTriggeredWatermark.compare_exchange_weak(highestWatermark, newGlobalWatermark.getRawValue()))
    {
    }
    if (newGlobalWatermark.getRawValue() <= highestWatermark)
    {
        return;
    }

    /// The store assigns all sequence numbers, so the buffers can be emitted in any order and without holding a lock.
    const auto [slicesAndWindowInfo, outputWatermarkAndSequenceNumber]
        = sliceAndWindowStore->getTriggerableWindowSlices(newGlobalWatermark);
    triggerSlices(slicesAndWindowInfo, pipelineCtx);

    if (outputWatermarkAndSequenceNumber)
    {
        const auto [outputWatermark, outputSequenceNumber] = *outputWatermarkAndSequenceNumber;
        auto watermarkBuffer = pipelineCtx->allocateTupleBuffer();
        watermarkBuffer.setNumberOfTuples(0);
        watermarkBuffer.setOriginId(outputOriginId);
        watermarkBuffer.setSequenceNumber(outputSequenceNumber);
        watermarkBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
        watermarkBuffer.setLastChunk(true);
        watermarkBuffer.setWatermark(outputWatermark);
        watermarkBuffer.setCreationTimestampInMS(Timestamp(
            std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));
        pipelineCtx->emitBuffer(watermarkBuffer);
    }
}

void WindowBasedOperatorHandler::applyWatermarkBackpressure()
{
    const std::scoped_lock lock(watermarkBackpressureMutex);
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
