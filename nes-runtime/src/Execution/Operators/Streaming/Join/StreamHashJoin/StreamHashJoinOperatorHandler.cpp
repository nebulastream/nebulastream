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

StreamHashJoinOperatorHandler::StreamHashJoinOperatorHandler(const std::vector<OriginId>& origins,
                                                             size_t windowSize,
                                                             size_t sizeOfRecordLeft,
                                                             size_t sizeOfRecordRight,
                                                             size_t totalSizeForDataStructures,
                                                             size_t pageSize,
                                                             size_t preAllocPageSizeCnt,
                                                             size_t numPartitions,
                                                             StreamJoinStrategy joinStrategy)
    : StreamJoinOperatorHandler(origins, windowSize, joinStrategy, sizeOfRecordLeft, sizeOfRecordRight),
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

void StreamHashJoinOperatorHandler::triggerWindows(std::vector<uint64_t> windowIdentifiersToBeTriggered,
                                                   WorkerContext* workerCtx,
                                                   PipelineExecutionContext* pipelineCtx) {
    //for every window
    for (auto& windowIdentifier : windowIdentifiersToBeTriggered) {
        //for every partition within the window create one task
        //get current window
        auto currentWindow = getWindowByWindowIdentifier(windowIdentifier);
        NES_ASSERT2_FMT(currentWindow.has_value(), "Triggering window does not exist for ts=" << windowIdentifier);
        auto hashWindow = static_cast<StreamHashJoinWindow*>(currentWindow->get());
        auto& sharedJoinHashTableLeft = hashWindow->getMergingHashTable(/* isLeftSide=*/true);
        auto& sharedJoinHashTableRight = hashWindow->getMergingHashTable(/* isLeftSide=*/false);

        for (auto i = 0UL; i < getNumPartitions(); ++i) {
            //for local we have to merge the tables first
            if (joinStrategy == StreamJoinStrategy::HASH_JOIN_LOCAL) {
                //push actual bucket from local to global hash table for left side

                //page before merging:

                auto localHashTableLeft = hashWindow->getHashTable(workerCtx->getId(), /* isLeftSide=*/true);
                sharedJoinHashTableLeft.insertBucket(i, localHashTableLeft->getBucketLinkedList(i));

                //push actual bucket from local to global hash table for right side
                auto localHashTableRight = hashWindow->getHashTable(workerCtx->getId(), /* isLeftSide=*/false);
                sharedJoinHashTableRight.insertBucket(i, localHashTableRight->getBucketLinkedList(i));
            }

            //create task for current window and current partition
            auto buffer = workerCtx->allocateTupleBuffer();
            auto bufferAs = buffer.getBuffer<JoinPartitionIdTWindowIdentifier>();
            bufferAs->partitionId = i;
            bufferAs->windowIdentifier = windowIdentifier;
            buffer.setNumberOfTuples(1);
            pipelineCtx->dispatchBuffer(buffer);
            NES_TRACE("Emitted windowIdentifier {}", windowIdentifier);
        }
    }
}

uint64_t StreamHashJoinOperatorHandler::getNumberOfTuplesInWindow(uint64_t windowIdentifier, uint64_t workerId, bool isLeftSide) {
    const auto window = getWindowByWindowIdentifier(windowIdentifier);
    if (window.has_value()) {
        auto hashWindow = static_cast<StreamHashJoinWindow*>(window.value().get());
        return hashWindow->getNumberOfTuplesOfWorker(workerId, isLeftSide);
    }

    return -1;
}

StreamHashJoinOperatorHandlerPtr StreamHashJoinOperatorHandler::create(const std::vector<OriginId>& origins,
                                                                       size_t windowSize,
                                                                       size_t sizeOfRecordLeft,
                                                                       size_t sizeOfRecordRight,
                                                                       size_t totalSizeForDataStructures,
                                                                       size_t pageSize,
                                                                       size_t preAllocPageSizeCnt,
                                                                       size_t numPartitions,
                                                                       StreamJoinStrategy joinStrategy) {

    return std::make_shared<StreamHashJoinOperatorHandler>(origins,
                                                           windowSize,
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