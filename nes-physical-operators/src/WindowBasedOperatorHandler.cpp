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

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
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

WindowBasedOperatorHandler::WatermarkSnapshot WindowBasedOperatorHandler::snapshotWatermarkState() const
{
    return {
        .build = watermarkProcessorBuild->snapshotContiguous(),
        .probe = watermarkProcessorProbe->snapshotContiguous(),
        .lastForwarded = lastForwardedWatermark.getRawValue()};
}

void WindowBasedOperatorHandler::restoreWatermarkState(const WatermarkSnapshot& state)
{
    watermarkProcessorBuild->restoreContiguous(state.build);
    watermarkProcessorProbe->restoreContiguous(state.probe);
    lastForwardedWatermark = Timestamp(state.lastForwarded);
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

}
