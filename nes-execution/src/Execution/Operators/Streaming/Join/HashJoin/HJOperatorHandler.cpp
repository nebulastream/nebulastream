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

#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

StreamSlicePtr HJOperatorHandler::createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) {
    return std::make_shared<HJSlice>(numberOfWorkerThreads,
                                     sliceStart,
                                     sliceEnd,
                                     sizeOfRecordLeft,
                                     sizeOfRecordRight,
                                     totalSizeForDataStructures,
                                     pageSize,
                                     preAllocPageSizeCnt,
                                     numPartitions,
                                     joinStrategy);
}

void HJOperatorHandler::emitSliceIdsToProbe(StreamSlice& sliceLeft,
                                            StreamSlice& sliceRight,
                                            const WindowInfo& windowInfo,
                                            PipelineExecutionContext* pipelineCtx) {
    if (sliceLeft.getNumberOfTuplesLeft() > 0 && sliceRight.getNumberOfTuplesRight() > 0) {
        dynamic_cast<HJSlice&>(sliceLeft).mergeLocalToGlobalHashTable();
        dynamic_cast<HJSlice&>(sliceRight).mergeLocalToGlobalHashTable();

        for (auto i = 0UL; i < getNumPartitions(); ++i) {

            //create task for current window and current partition
            auto buffer = pipelineCtx->getBufferManager()->getBufferBlocking();
            auto bufferAs = buffer.getBuffer<JoinPartitionIdSliceIdWindow>();
            bufferAs->partitionId = i;
            bufferAs->sliceIdentifierLeft = sliceLeft.getSliceIdentifier();
            bufferAs->sliceIdentifierRight = sliceRight.getSliceIdentifier();
            bufferAs->windowInfo = windowInfo;
            buffer.setNumberOfTuples(1);

            /** As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
             * As we emit one buffer for each partition, the watermark can not be the slice end as some buffer might be
             * still waiting for getting processed.
             */
            auto watermark = windowInfo.windowStart;
            if (bufferAs->partitionId + 1 == getNumPartitions()) {
                watermark = std::min(sliceLeft.getSliceStart(), sliceRight.getSliceStart());
            }

            buffer.setOriginId(getOutputOriginId());
            buffer.setSequenceData({getNextSequenceNumber(), /*chunkNumber*/ 1, true});
            buffer.setWatermark(watermark);

            pipelineCtx->dispatchBuffer(buffer);
            NES_INFO("Emitted leftSliceId {} rightSliceId {} with watermarkTs {} sequenceNumber {} originId {} for no. left "
                     "tuples {} and no. right tuples {}",
                     bufferAs->sliceIdentifierLeft,
                     bufferAs->sliceIdentifierRight,
                     buffer.getWatermark(),
                     buffer.getSequenceNumber(),
                     buffer.getOriginId(),
                     sliceLeft.getNumberOfTuplesLeft(),
                     sliceRight.getNumberOfTuplesRight());
        }
    }
}

uint64_t HJOperatorHandler::getPreAllocPageSizeCnt() const { return preAllocPageSizeCnt; }

uint64_t HJOperatorHandler::getPageSize() const { return pageSize; }

uint64_t HJOperatorHandler::getNumPartitions() const { return numPartitions; }

uint64_t HJOperatorHandler::getTotalSizeForDataStructures() const { return totalSizeForDataStructures; }

HJOperatorHandler::HJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                     const OriginId outputOriginId,
                                     const uint64_t windowSize,
                                     const uint64_t windowSlide,
                                     const SchemaPtr& leftSchema,
                                     const SchemaPtr& rightSchema,
                                     const QueryCompilation::StreamJoinStrategy joinStrategy,
                                     uint64_t totalSizeForDataStructures,
                                     uint64_t preAllocPageSizeCnt,
                                     uint64_t pageSize,
                                     uint64_t numPartitions)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, leftSchema, rightSchema),
      joinStrategy(joinStrategy), totalSizeForDataStructures(totalSizeForDataStructures),
      preAllocPageSizeCnt(preAllocPageSizeCnt), pageSize(pageSize), numPartitions(numPartitions) {}

void* insertFunctionProxy(void* ptrLocalHashTable, uint64_t key) {
    NES_ASSERT2_FMT(ptrLocalHashTable != nullptr, "ptrLocalHashTable should not be null");
    LocalHashTable* localHashTable = static_cast<LocalHashTable*>(ptrLocalHashTable);
    return localHashTable->insert(key);
}

}// namespace NES::Runtime::Execution::Operators