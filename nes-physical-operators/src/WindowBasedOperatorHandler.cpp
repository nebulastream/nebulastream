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
#include <filesystem>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <Watermark/MultiOriginWatermarkProcessor.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSliceCheckpoint.hpp>
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
    if (checkpoint and getRecoveryStateDirectory())
    {
        std::call_once(
            stateRestored, [&] { checkpoint->restore(*this, *getRecoveryStateDirectory(), *pipelineExecutionContext.getBufferManager()); });
    }
}

void WindowBasedOperatorHandler::enableCheckpointing(HashMapSliceCheckpoint hashMapSliceCheckpoint)
{
    checkpoint.emplace(std::move(hashMapSliceCheckpoint));
}

bool WindowBasedOperatorHandler::supportsCheckpointing() const
{
    return checkpoint.has_value();
}

void WindowBasedOperatorHandler::serializeState(const std::filesystem::path& stateDirectory)
{
    PRECONDITION(checkpoint.has_value(), "Checkpointing was not enabled for this window operator");
    checkpoint->write(*this, stateDirectory);
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
