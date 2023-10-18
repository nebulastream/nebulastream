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

#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandlerSlicing.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Runtime::Execution::Operators {

StreamSlicePtr StreamJoinOperatorHandlerSlicing::getSliceByTimestampOrCreateIt(uint64_t timestamp) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

StreamJoinOperatorHandlerSlicing::StreamJoinOperatorHandlerSlicing(const std::vector<OriginId>& inputOrigins,
                                                                   const OriginId outputOriginId,
                                                                   const uint64_t windowSize,
                                                                   const uint64_t windowSlide,
                                                                   uint64_t sizeOfRecordLeft,
                                                                   uint64_t sizeOfRecordRight)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide, sizeOfRecordLeft, sizeOfRecordRight) {
}

StreamSlice* StreamJoinOperatorHandlerSlicing::getCurrentSliceOrCreate() { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

std::vector<WindowInfo> StreamJoinOperatorHandlerSlicing::getAllWindowsForSlice(StreamSlice& slice) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
}// namespace NES::Runtime::Execution::Operators