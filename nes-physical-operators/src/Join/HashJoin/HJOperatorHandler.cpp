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


#include <Join/HashJoin/HJOperatorHandler.hpp>

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{
std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>
HJOperatorHandler::getCreateNewSlicesFunction(const CreateNewSlicesArguments& newSlicesArguments) const
{
    PRECONDITION(
        numberOfWorkerThreads > 0, "Number of worker threads not set for window based operator. Was setWorkerThreads() being called?");

    const auto newHashMapArgs = dynamic_cast<const CreateNewHashMapSliceArgs&>(newSlicesArguments);
    return std::function(
        [outputOriginId = outputOriginId, numberOfWorkerThreads = numberOfWorkerThreads, copyOfNewHashMapArgs = newHashMapArgs](
            SliceStart sliceStart, SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        {
            NES_TRACE("Creating new hash-join slice for slice {}-{} for output origin {}", sliceStart, sliceEnd, outputOriginId);
            return {std::make_shared<HJSlice>(sliceStart, sliceEnd, copyOfNewHashMapArgs, numberOfWorkerThreads)};
        });
}

void HJOperatorHandler::setNautilusCleanupExec(
    std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec> nautilusCleanupExec, const JoinBuildSideType& buildSide)
{
    switch (buildSide)
    {
        case JoinBuildSideType::Right:
            rightCleanupStateNautilusFunction = std::move(nautilusCleanupExec);
            break;
        case JoinBuildSideType::Left:
            leftCleanupStateNautilusFunction = std::move(nautilusCleanupExec);
            break;
            std::unreachable();
    }
}

std::vector<std::shared_ptr<CreateNewHashMapSliceArgs::NautilusCleanupExec>> HJOperatorHandler::getNautilusCleanupExec() const
{
    return {leftCleanupStateNautilusFunction, rightCleanupStateNautilusFunction};
}

void HJOperatorHandler::emitSlicesToProbe(
    Slice& sliceLeft,
    Slice& sliceRight,
    const WindowInfo& windowInfo,
    const SequenceData& sequenceData,
    PipelineExecutionContext* pipelineCtx)
{
    /// Counting how many tuples the probe has to check for this probe task
    uint64_t totalNumberOfTuples = 0;


    /// Getting all hash maps for the left and right slice
    auto getHashMapsForSlice = [&](const Slice& slice, const JoinBuildSideType& buildSide)
    {
        std::vector<Nautilus::Interface::HashMap*> allHashMaps;
        const auto* const hashJoinSlice = dynamic_cast<const HJSlice*>(&slice);
        INVARIANT(hashJoinSlice != nullptr, "Slice must be of type HashMapSlice!");
        for (uint64_t hashMapIdx = 0; hashMapIdx < hashJoinSlice->getNumberOfHashMapsForSide(); ++hashMapIdx)
        {
            if (auto* hashMap = hashJoinSlice->getHashMapPtr(WorkerThreadId(hashMapIdx), buildSide);
                hashMap and hashMap->getNumberOfTuples() > 0)
            {
                allHashMaps.emplace_back(hashMap);
                totalNumberOfTuples += hashMap->getNumberOfTuples();
            }
        }
        return allHashMaps;
    };
    const auto leftHashMaps = getHashMapsForSlice(sliceLeft, JoinBuildSideType::Left);
    const auto rightHashMaps = getHashMapsForSlice(sliceRight, JoinBuildSideType::Right);

    /// We need a buffer that is large enough to store:
    /// - all pointers to (left + right) hashmaps of the window to be triggered
    /// - size of EmittedHJWindowTrigger
    const auto neededBufferSize
        = sizeof(EmittedHJWindowTrigger) + ((leftHashMaps.size() + rightHashMaps.size()) * sizeof(Nautilus::Interface::HashMap*));
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("Could not get a buffer of size {} for the aggregation window trigger", neededBufferSize);
    }

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, the watermark and the "number of tuples".
    /// The watermark cannot be the slice end as some buffers might be still waiting to get processed.
    auto tupleBuffer = tupleBufferVal.value();
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(sequenceData.sequenceNumber));
    tupleBuffer.setChunkNumber(ChunkNumber(sequenceData.chunkNumber));
    tupleBuffer.setLastChunk(sequenceData.lastChunk);
    tupleBuffer.setWatermark(windowInfo.windowStart);
    tupleBuffer.setNumberOfTuples(totalNumberOfTuples);


    /// Writing all necessary information for the probe to the buffer
    auto* bufferMemory = tupleBuffer.getBuffer<EmittedHJWindowTrigger>();
    bufferMemory->windowInfo = windowInfo;
    bufferMemory->leftNumberOfHashMaps = leftHashMaps.size();
    bufferMemory->rightNumberOfHashMaps = rightHashMaps.size();

    /// Copying the left and right hashmap pointer to the buffer
    const auto leftHashMapPtrSizeInByte = leftHashMaps.size() * sizeof(Nautilus::Interface::HashMap*);
    const auto rightHashMapPtrSizeInByte = rightHashMaps.size() * sizeof(Nautilus::Interface::HashMap*);
    auto* addressFirstLeftHashMapPtr = reinterpret_cast<int8_t*>(bufferMemory) + sizeof(EmittedHJWindowTrigger);
    auto* addressFirstRightHashMapPtr = reinterpret_cast<int8_t*>(bufferMemory) + sizeof(EmittedHJWindowTrigger) + leftHashMapPtrSizeInByte;
    bufferMemory->leftHashMaps = reinterpret_cast<Nautilus::Interface::HashMap**>(addressFirstLeftHashMapPtr);
    bufferMemory->rightHashMaps = reinterpret_cast<Nautilus::Interface::HashMap**>(addressFirstRightHashMapPtr);
    std::memcpy(addressFirstLeftHashMapPtr, leftHashMaps.data(), leftHashMapPtrSizeInByte);
    std::memcpy(addressFirstRightHashMapPtr, rightHashMaps.data(), rightHashMapPtrSizeInByte);

    /// Dispatching the buffer to the probe operator via the task queue.
    pipelineCtx->emitBuffer(tupleBuffer);
    NES_TRACE(
        "Emitted window {}-{} with watermarkTs {} sequenceNumber {} originId {} and {}-{} hashmaps",
        windowInfo.windowStart,
        windowInfo.windowEnd,
        tupleBuffer.getWatermark(),
        tupleBuffer.getSequenceNumber(),
        tupleBuffer.getOriginId(),
        leftHashMaps.size(),
        rightHashMaps.size());
}

}
