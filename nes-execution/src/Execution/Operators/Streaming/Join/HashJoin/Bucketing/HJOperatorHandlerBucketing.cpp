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

#include <Execution/Operators/Streaming/Join/HashJoin/Bucketing/HJOperatorHandlerBucketing.hpp>

namespace NES::Runtime::Execution::Operators {

HJOperatorHandlerBucketing::HJOperatorHandlerBucketing(const std::vector<OriginId>& inputOrigins,
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
      HJOperatorHandler(inputOrigins,
                        outputOriginId,
                        windowSize,
                        windowSlide,
                        leftSchema,
                        rightSchema,
                        joinStrategy,
                        totalSizeForDataStructures,
                        preAllocPageSizeCnt,
                        pageSize,
                        numPartitions) {}

HJOperatorHandlerPtr HJOperatorHandlerBucketing::create(const std::vector<OriginId>& inputOrigins,
                                                        const OriginId outputOriginId,
                                                        const uint64_t windowSize,
                                                        const uint64_t windowSlide,
                                                        const SchemaPtr& leftSchema,
                                                        const SchemaPtr& rightSchema,
                                                        const QueryCompilation::StreamJoinStrategy joinStrategy,
                                                        uint64_t totalSizeForDataStructures,
                                                        uint64_t preAllocPageSizeCnt,
                                                        uint64_t pageSize,
                                                        uint64_t numPartitions) {
    return std::make_shared<HJOperatorHandlerBucketing>(inputOrigins,
                                                        outputOriginId,
                                                        windowSize,
                                                        windowSlide,
                                                        leftSchema,
                                                        rightSchema,
                                                        joinStrategy,
                                                        totalSizeForDataStructures,
                                                        preAllocPageSizeCnt,
                                                        pageSize,
                                                        numPartitions);
}

}// namespace NES::Runtime::Execution::Operators