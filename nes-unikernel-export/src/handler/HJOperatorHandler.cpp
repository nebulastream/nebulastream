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
#include <Util/Logger/Logger.hpp>

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
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

uint64_t HJOperatorHandler::getPreAllocPageSizeCnt() const { return preAllocPageSizeCnt; }

uint64_t HJOperatorHandler::getPageSize() const { return pageSize; }

uint64_t HJOperatorHandler::getNumPartitions() const { return numPartitions; }

uint64_t HJOperatorHandler::getTotalSizeForDataStructures() const { return totalSizeForDataStructures; }

HJOperatorHandler::HJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                     const OriginId outputOriginId,
                                     const uint64_t windowSize,
                                     const uint64_t windowSlide,
                                     uint64_t sizeOfRecordLeft,
                                     uint64_t sizeOfRecordRight,
                                     const QueryCompilation::StreamJoinStrategy joinStrategy,
                                     uint64_t totalSizeForDataStructures,
                                     uint64_t preAllocPageSizeCnt,
                                     uint64_t pageSize,
                                     uint64_t numPartitions)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, sizeOfRecordLeft, sizeOfRecordRight),
      joinStrategy(joinStrategy), totalSizeForDataStructures(totalSizeForDataStructures),
      preAllocPageSizeCnt(preAllocPageSizeCnt), pageSize(pageSize), numPartitions(numPartitions) {
    TRACE_OPERATOR_HANDLER("NES::Runtime::Execution::Operators::HJOperatorHandler",
                           "Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp",
                           inputOrigins,
                           outputOriginId,
                           windowSize,
                           windowSlide,
                           sizeOfRecordLeft,
                           sizeOfRecordRight,
                           joinStrategy,
                           totalSizeForDataStructures,
                           preAllocPageSizeCnt,
                           pageSize);
}

void* insertFunctionProxy(void* ptrLocalHashTable, uint64_t key) { NES_THROW_RUNTIME_ERROR("Not Implemented!"); }

}// namespace NES::Runtime::Execution::Operators