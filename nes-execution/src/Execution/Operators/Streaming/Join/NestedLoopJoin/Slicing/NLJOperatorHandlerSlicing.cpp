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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/Slicing/NLJOperatorHandlerSlicing.hpp>

namespace NES::Runtime::Execution::Operators {

NLJOperatorHandlerSlicing::NLJOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                                                     OriginId outputOriginId,
                                                     uint64_t windowSize,
                                                     uint64_t windowSlide,
                                                     PagedVectorSize leftSize,
                                                     PagedVectorSize rightSize)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide),
      NLJOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, leftSize, rightSize),
      StreamJoinOperatorHandlerSlicing() {}
NLJOperatorHandlerPtr NLJOperatorHandlerSlicing::create(const std::vector<OriginId>& inputOrigins,
                                                        OriginId outputOriginId,
                                                        uint64_t windowSize,
                                                        uint64_t windowSlide,
                                                        PagedVectorSize left,
                                                        PagedVectorSize right) {
    return std::make_shared<NLJOperatorHandlerSlicing>(inputOrigins, outputOriginId, windowSize, windowSlide, left, right);
}
}// namespace NES::Runtime::Execution::Operators
