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

#include <map>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Execution/Operators/SliceStore/Slice.hpp>
#include <Execution/Operators/SliceStore/WindowSlicesStoreInterface.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Watermark/MultiOriginWatermarkProcessor.hpp>
#include <Execution/Operators/WindowBasedOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Execution.hpp>

namespace NES::Runtime::Execution::Operators
{
StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
    const std::shared_ptr<Nautilus::Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , leftMemoryProvider(leftMemoryProvider)
    , rightMemoryProvider(rightMemoryProvider)
{
}

void StreamJoinOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// The watermark processor handles the minimal watermark across both streams
    const auto newGlobalWatermark
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);

    /// Getting all slices that can be triggered and triggering them
    const auto slicesAndWindowInfo = sliceAndWindowStore->getTriggerableWindowSlices(newGlobalWatermark);
    triggerSlices(slicesAndWindowInfo, pipelineCtx);
}

void StreamJoinOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    /// For every window, we have to trigger all combination of slices. This is necessary, as we have to give the probe operator all
    /// combinations of slices for a given window to ensure that it has seen all tuples of the window.
    for (const auto& [windowInfo, allSlices] : slicesAndWindowInfo)
    {
        ChunkNumber::Underlying chunkNumber = ChunkNumber::INITIAL;
        for (const auto& sliceLeft : allSlices)
        {
            for (const auto& sliceRight : allSlices)
            {
                const bool isLastChunk = chunkNumber == (allSlices.size() * allSlices.size() - 1);
                emitSliceIdsToProbe(*sliceLeft, *sliceRight, windowInfo, ChunkNumber(chunkNumber), isLastChunk, pipelineCtx);
                ++chunkNumber;
            }
        }
    }
}

void StreamJoinOperatorHandler::triggerAllWindows(PipelineExecutionContext* pipelineCtx)
{
    const auto slicesAndWindowInfo = sliceAndWindowStore->getAllNonTriggeredSlices();
    triggerSlices(slicesAndWindowInfo, pipelineCtx);
}


}
