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
#include <cstdint>
#include <memory>
#include <ranges>
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
    , watermarkGapThrottle(std::move(sourcesOfInputOrigins))
{
}

void WindowBasedOperatorHandler::start(PipelineExecutionContext& pipelineExecutionContext)
{
    numberOfWorkerThreads = pipelineExecutionContext.getNumberOfWorkerThreads();

    watermarkGapThrottle.start(pipelineExecutionContext.getWatermarkBackpressure());
}

void WindowBasedOperatorHandler::stop(QueryTerminationType, PipelineExecutionContext&)
{
}

WindowSlicesStoreInterface& WindowBasedOperatorHandler::getSliceAndWindowStore() const
{
    return *sliceAndWindowStore;
}

void WindowBasedOperatorHandler::garbageCollectSlicesAndWindows(const BufferMetaData& bufferMetaData)
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

Timestamp WindowBasedOperatorHandler::startBuildBuffer(const OriginId originId)
{
    /// The watermark must be read while holding the lock, so that a concurrent garbage collection either sees this lower bound
    /// or runs with a watermark that is not larger than it.
    const auto lowerBounds = buildLowerBounds.wlock();
    const auto lowerBound = watermarkProcessorBuild->getCurrentWatermark(originId);
    lowerBounds->insert(lowerBound.getRawValue());
    return lowerBound;
}

/// A buffer whose execution throws never calls this, so its lower bound stays registered and holds back garbage collection.
/// This only affects a query that is failing already.
void WindowBasedOperatorHandler::finishBuildBuffer(const Timestamp lowerBound)
{
    const auto lowerBounds = buildLowerBounds.wlock();
    const auto it = lowerBounds->find(lowerBound.getRawValue());
    INVARIANT(it != lowerBounds->end(), "No build buffer was started with the lower bound {}", lowerBound);
    lowerBounds->erase(it);
}

void WindowBasedOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    const auto originWatermarkBefore = watermarkProcessorBuild->getCurrentWatermark(bufferMetaData.originId);
    /// The watermark processor handles the minimal watermark across both streams
    const auto newGlobalWatermark
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    const bool originWatermarkMoved = watermarkProcessorBuild->getCurrentWatermark(bufferMetaData.originId) != originWatermarkBefore;

    NES_TRACE(
        "New global watermark: {} for origin: {} and sequence data: {} and watermarkTs of buffer {}",
        newGlobalWatermark,
        bufferMetaData.originId,
        bufferMetaData.seqNumber,
        bufferMetaData.watermarkTs);

    /// The gap between the origins also changes when the global watermark does not advance, but only if some origin's watermark
    /// moved. The thread whose emplace moves an origin's watermark observes that move, so no change goes unapplied.
    if (originWatermarkMoved)
    {
        watermarkGapThrottle.onWatermarkMoved(*watermarkProcessorBuild);
    }

    const auto newValue = newGlobalWatermark.getRawValue();
    auto highestWatermark = highestTriggeredWatermark.load();
    while (true)
    {
        if (newValue <= highestWatermark)
        {
            return; /// already triggered up to here, possibly by another thread
        }
        if (highestTriggeredWatermark.compare_exchange_weak(highestWatermark, newValue))
        {
            break; /// this thread advanced the watermark
        }
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
        watermarkBuffer.setCreationTimestampInMS(Timestamp::nowInMilliseconds());
        pipelineCtx->emitBuffer(watermarkBuffer);
    }
}

}
