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

#include <Execution/Operators/Streaming/Join/HashJoin/Slicing/HJOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

HJOperatorHandlerSlicing::HJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                                                   const OriginId outputOriginId,
                                                   const uint64_t windowSize,
                                                   const uint64_t windowSlide,
                                                   const uint64_t sizeOfRecordLeft,
                                                   const uint64_t sizeOfRecordRight,
                                                   const QueryCompilation::StreamJoinStrategy joinStrategy,
                                                   const uint64_t totalSizeForDataStructures,
                                                   const uint64_t preAllocPageSizeCnt,
                                                   const uint64_t pageSize,
                                                   const uint64_t numPartitions)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, sizeOfRecordLeft, sizeOfRecordRight),
      HJOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, sizeOfRecordLeft, sizeOfRecordRight, joinStrategy,
                        totalSizeForDataStructures, preAllocPageSizeCnt, pageSize, numPartitions){}

HJOperatorHandlerPtr HJOperatorHandlerSlicing::create(const std::vector<OriginId>& inputOrigins,
                                                   const OriginId outputOriginId,
                                                   const uint64_t windowSize,
                                                   const uint64_t windowSlide,
                                                   const uint64_t sizeOfRecordLeft,
                                                   const uint64_t sizeOfRecordRight,
                                                   const QueryCompilation::StreamJoinStrategy joinStrategy,
                                                   const uint64_t totalSizeForDataStructures,
                                                   const uint64_t preAllocPageSizeCnt,
                                                   const uint64_t pageSize,
                                                   const uint64_t numPartitions) {
    return std::make_shared<HJOperatorHandlerSlicing>(inputOrigins, outputOriginId, windowSize, windowSlide, sizeOfRecordLeft,
                                                      sizeOfRecordRight, joinStrategy, totalSizeForDataStructures, preAllocPageSizeCnt, pageSize, numPartitions);
}

}// namespace NES::Runtime::Execution::Operators