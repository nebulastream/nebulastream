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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

NLJOperatorHandlerSlicing::NLJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     const uint64_t sizeOfRecordLeft,
                                                     const uint64_t sizeOfRecordRight,
                                                     const uint64_t pageSizeLeft,
                                                     const uint64_t pageSizeRight)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, sizeOfRecordLeft, sizeOfRecordRight),
      NLJOperatorHandler(inputOrigins,
                         outputOriginId,
                         windowSize,
                         windowSlide,
                         sizeOfRecordLeft,
                         sizeOfRecordRight,
                         pageSizeLeft,
                         pageSizeRight) {}
NLJOperatorHandlerPtr NLJOperatorHandlerSlicing::create(const std::vector<OriginId>& inputOrigins,
                                                     const OriginId outputOriginId,
                                                     const uint64_t windowSize,
                                                     const uint64_t windowSlide,
                                                     const uint64_t sizeOfRecordLeft,
                                                     const uint64_t sizeOfRecordRight,
                                                     const uint64_t pageSizeLeft,
                                                     const uint64_t pageSizeRight) {
    return std::make_shared<NLJOperatorHandlerSlicing>(inputOrigins,
                                                       outputOriginId,
                                                       windowSize,
                                                       windowSlide,
                                                       sizeOfRecordLeft,
                                                       sizeOfRecordRight,
                                                       pageSizeLeft,
                                                       pageSizeRight);
}
}// namespace NES::Runtime::Execution::Operators