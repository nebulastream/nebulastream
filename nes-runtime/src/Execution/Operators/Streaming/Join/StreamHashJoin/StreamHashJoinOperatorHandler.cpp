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

#include <Execution/Operators/Streaming/Join/StreamHashJoin/StreamHashJoinOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <atomic>
#include <cstddef>

namespace NES::Runtime::Execution::Operators {

StreamHashJoinOperatorHandler::StreamHashJoinOperatorHandler(SchemaPtr joinSchemaLeft,
                                                             SchemaPtr joinSchemaRight,
                                                             std::string joinFieldNameLeft,
                                                             std::string joinFieldNameRight,
                                                             const std::vector<OriginId>& origins,
                                                             uint64_t numberOfWorker,
                                                             size_t windowSize,
                                                             size_t totalSizeForDataStructures,
                                                             size_t pageSize,
                                                             size_t preAllocPageSizeCnt,
                                                             size_t numPartitions)
    : StreamJoinOperatorHandler(windowSize,
                                joinSchemaLeft,
                                joinSchemaRight,
                                joinFieldNameLeft,
                                joinFieldNameRight,
                                origins,
                                StreamJoinOperatorHandler::JoinType::HASH_JOIN),
      numberOfWorker(numberOfWorker), totalSizeForDataStructures(totalSizeForDataStructures),
      preAllocPageSizeCnt(preAllocPageSizeCnt), pageSize(pageSize), numPartitions(numPartitions) {

    NES_DEBUG("Setup StreamHashJoinOperatorHandler counterFinishedBuildingStart="
              << numberOfWorker << " counterFinishedSinkStart=" << numberOfWorker
              << " totalSizeForDataStructures=" << totalSizeForDataStructures);
    NES_ASSERT2_FMT(0 < numPartitions, "NumPartitions is 0: " << numPartitions);
}

void StreamHashJoinOperatorHandler::start(PipelineExecutionContextPtr, StateManagerPtr, uint32_t) {
    NES_DEBUG("start HashJoinOperatorHandler");
}

void StreamHashJoinOperatorHandler::stop(QueryTerminationType, PipelineExecutionContextPtr) {
    NES_DEBUG("stop HashJoinOperatorHandler");
}

StreamHashJoinOperatorHandlerPtr StreamHashJoinOperatorHandler::create(const SchemaPtr& joinSchemaLeft,
                                                                       const SchemaPtr& joinSchemaRight,
                                                                       const std::string& joinFieldNameLeft,
                                                                       const std::string& joinFieldNameRight,
                                                                       const std::vector<OriginId>& origins,
                                                                       uint64_t numberOfWorker,
                                                                       size_t windowSize,
                                                                       size_t totalSizeForDataStructures,
                                                                       size_t pageSize,
                                                                       size_t preAllocPageSizeCnt,
                                                                       size_t numPartitions) {

    return std::make_shared<StreamHashJoinOperatorHandler>(joinSchemaLeft,
                                                           joinSchemaRight,
                                                           joinFieldNameLeft,
                                                           joinFieldNameRight,
                                                           origins,
                                                           numberOfWorker,
                                                           windowSize,
                                                           totalSizeForDataStructures,
                                                           pageSize,
                                                           preAllocPageSizeCnt,
                                                           numPartitions);
}
size_t StreamHashJoinOperatorHandler::getPreAllocPageSizeCnt() const { return preAllocPageSizeCnt; }
size_t StreamHashJoinOperatorHandler::getPageSize() const { return pageSize; }
size_t StreamHashJoinOperatorHandler::getNumPartitions() const { return numPartitions; }

}// namespace NES::Runtime::Execution::Operators