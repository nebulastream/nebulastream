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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
namespace NES::Runtime::Execution::Operators {

StreamHashJoinOperatorHandler::StreamHashJoinOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                                             const OriginId outputOriginId,
                                                             uint64_t windowSize,
                                                             uint64_t windowSlide,
                                                             uint64_t sizeOfRecordLeft,
                                                             uint64_t sizeOfRecordRight,
                                                             uint64_t totalSizeForDataStructures,
                                                             uint64_t pageSize,
                                                             uint64_t preAllocPageSizeCnt,
                                                             uint64_t numPartitions,
                                                             QueryCompilation::StreamJoinStrategy joinStrategy)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, joinStrategy, sizeOfRecordLeft, sizeOfRecordRight),
      totalSizeForDataStructures(totalSizeForDataStructures), preAllocPageSizeCnt(preAllocPageSizeCnt), pageSize(pageSize),
      numPartitions(numPartitions) {
    NES_ASSERT2_FMT(0 < numPartitions, "NumPartitions is 0: " << numPartitions);
}

void StreamHashJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start HashJoinOperatorHandler");
}

void StreamHashJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop HashJoinOperatorHandler");
}

void StreamHashJoinOperatorHandler::triggerSlices(TriggerableWindows& idsToBeTriggered,
                                                   PipelineExecutionContext* pipelineCtx) {

    // This is not the most efficient implementation but good enough for now, as this should not be on the hottest code path
    // We merge all hash tables to a global hash table
    for (const auto& [windowId, triggerableSlicesForWindow] : idsToBeTriggered.windowIdToTriggerableSlices) {
        for (const auto& curSlice : triggerableSlicesForWindow.slicesToTrigger) {
            auto hashSlice = std::dynamic_pointer_cast<StreamHashJoinSlice>(curSlice);
            hashSlice->mergeLocalToGlobalHashTable();
        }
    }

    /**
     * We expect the idsToBeTriggered to be sorted.
     * Otherwise, it can happen that the buffer <seq 2, ts 1000> gets processed after <seq 1, ts 20000>, which will lead to wrong results upstream
     * Also, we have to set the seq number in order of the slice end timestamps.
     * Furthermore, we expect the sliceIdentifier to be the sliceEnd.
     */
    for (const auto& [windowId, sliceIdsForWindow] : idsToBeTriggered.windowIdToTriggerableSlices) {
        // for every left and right slice id combination
        for (auto& [curSliceLeft, curSliceRight] : getSlicesLeftRightForWindow(windowId)) {
            if (curSliceLeft->getNumberOfTuplesLeft() > 0 && curSliceRight->getNumberOfTuplesRight() > 0) {

                for (auto i = 0UL; i < getNumPartitions(); ++i) {

                    //create task for current window and current partition
                    auto buffer = pipelineCtx->getBufferManager()->getBufferBlocking();
                    auto bufferAs = buffer.getBuffer<JoinPartitionIdSliceIdWindow>();
                    bufferAs->partitionId = i;
                    bufferAs->sliceIdentifierLeft = curSliceLeft->getSliceIdentifier();
                    bufferAs->sliceIdentifierRight = curSliceRight->getSliceIdentifier();
                    bufferAs->windowInfo = sliceIdsForWindow.windowInfo;
                    buffer.setNumberOfTuples(1);

                    /** As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
                     * As we emit one buffer for each partition, the watermark can not be the slice end as some buffer might be
                     * still waiting for getting processed.
                     */
                    buffer.setOriginId(getOutputOriginId());
                    buffer.setSequenceNumber(getNextSequenceNumber());
                    buffer.setWatermark(std::min(curSliceLeft->getSliceStart(), curSliceRight->getSliceStart()));

                    pipelineCtx->dispatchBuffer(buffer);
                    NES_INFO("Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left tuples {} and no. right tuples {}",
                             bufferAs->sliceIdentifierLeft,
                             bufferAs->sliceIdentifierRight,
                             buffer.getWatermark(),
                             buffer.getSequenceNumber(),
                             buffer.getOriginId(),
                             curSliceLeft->getNumberOfTuplesLeft(),
                             curSliceRight->getNumberOfTuplesRight());
                }
            }
        }
    }
}

uint64_t StreamHashJoinOperatorHandler::getNumberOfTuplesInSlice(uint64_t sliceIdentifier,
                                                                  uint64_t workerId,
                                                                  QueryCompilation::JoinBuildSideType joinBuildSide) {
    const auto window = getSliceBySliceIdentifier(sliceIdentifier);
    if (window.has_value()) {
        auto hashWindow = dynamic_cast<StreamHashJoinSlice*>(window.value().get());
        return hashWindow->getNumberOfTuplesOfWorker(joinBuildSide, workerId);
    }

    return -1;
}

StreamHashJoinOperatorHandlerPtr StreamHashJoinOperatorHandler::create(const std::vector<OriginId>& inputOrigins,
                                                                       const OriginId outputOriginId,
                                                                       uint64_t windowSize,
                                                                       uint64_t windowSlide,
                                                                       uint64_t sizeOfRecordLeft,
                                                                       uint64_t sizeOfRecordRight,
                                                                       uint64_t totalSizeForDataStructures,
                                                                       uint64_t pageSize,
                                                                       uint64_t preAllocPageSizeCnt,
                                                                       uint64_t numPartitions,
                                                                       QueryCompilation::StreamJoinStrategy joinStrategy) {

    return std::make_shared<StreamHashJoinOperatorHandler>(inputOrigins,
                                                           outputOriginId,
                                                           windowSize,
                                                           windowSlide,
                                                           sizeOfRecordLeft,
                                                           sizeOfRecordRight,
                                                           totalSizeForDataStructures,
                                                           pageSize,
                                                           preAllocPageSizeCnt,
                                                           numPartitions,
                                                           joinStrategy);
}

size_t StreamHashJoinOperatorHandler::getPreAllocPageSizeCnt() const { return preAllocPageSizeCnt; }
size_t StreamHashJoinOperatorHandler::getPageSize() const { return pageSize; }
size_t StreamHashJoinOperatorHandler::getNumPartitions() const { return numPartitions; }
size_t StreamHashJoinOperatorHandler::getTotalSizeForDataStructures() const { return totalSizeForDataStructures; }

}// namespace NES::Runtime::Execution::Operators