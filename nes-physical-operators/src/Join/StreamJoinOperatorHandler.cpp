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

#include <Join/StreamJoinOperatorHandler.hpp>

#include <map>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceCache/SliceCache.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{
StreamJoinOperatorHandler::StreamJoinOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore)
    : WindowBasedOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
{
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
                const bool isLastChunk = chunkNumber == (allSlices.size() * allSlices.size());
                const SequenceData sequenceData{windowInfo.sequenceNumber, ChunkNumber(chunkNumber), isLastChunk};
                emitSlicesToProbe(*sliceLeft, *sliceRight, windowInfo.windowInfo, sequenceData, pipelineCtx);
                ++chunkNumber;
            }
        }
    }
}

void StreamJoinOperatorHandler::allocateSliceCacheEntries(
    const uint64_t sizeOfEntry, const uint64_t numberOfEntries, Memory::AbstractBufferProvider* bufferProvider)
{
    /// If the slice cache has already been created, we simply return
    if (hasSliceCacheCreated.exchange(true))
    {
        return;
    }

    PRECONDITION(bufferProvider != nullptr, "Buffer provider should not be null");
    /// As we have a left and right side, we have to allocate the slice cache entries for both sides
    for (uint64_t i = 0; i < numberOfWorkerThreads * 2; ++i)
    {
        const auto neededSize = numberOfEntries * sizeOfEntry + sizeof(HitsAndMisses);
        INVARIANT(neededSize > 0, "Size of entry should be larger than 0");

        auto bufferOpt = bufferProvider->getUnpooledBuffer(neededSize);
        INVARIANT(bufferOpt.has_value(), "Buffer provider should return a buffer");
        std::memset(bufferOpt.value().getBuffer(), 0, bufferOpt.value().getBufferSize());
        sliceCacheEntriesBufferForWorkerThreads.emplace_back(bufferOpt.value());
    }
}

const int8_t* StreamJoinOperatorHandler::getStartOfSliceCacheEntries(const StartSliceCacheEntriesArgs& startSliceCacheEntriesArgs) const
{
    PRECONDITION(numberOfWorkerThreads > 0, "Number of worker threads should be set before calling this method");
    const auto startSliceCacheEntriesJoin = dynamic_cast<const StartSliceCacheEntriesStreamJoin&>(startSliceCacheEntriesArgs);
    const auto pos = startSliceCacheEntriesJoin.workerThreadId % sliceCacheEntriesBufferForWorkerThreads.size()
        + numberOfWorkerThreads * static_cast<uint64_t>(startSliceCacheEntriesJoin.joinBuildSide);
    INVARIANT(
        not sliceCacheEntriesBufferForWorkerThreads.empty() and pos < sliceCacheEntriesBufferForWorkerThreads.size(),
        "Position should be smaller than the size of the sliceCacheEntriesBufferForWorkerThreads");
    return sliceCacheEntriesBufferForWorkerThreads.at(pos).getBuffer();
}


}
