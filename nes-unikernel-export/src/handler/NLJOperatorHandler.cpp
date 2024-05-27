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
#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Runtime::Execution::Operators {
void NLJOperatorHandler::emitSliceIdsToProbe(StreamSlice& sliceLeft,
                                             StreamSlice& sliceRight,
                                             const WindowInfo& windowInfo,
                                             PipelineExecutionContext* pipelineCtx) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

StreamSlicePtr NLJOperatorHandler::createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}
NLJOperatorHandler::NLJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                                       const OriginId outputOriginId,
                                       const uint64_t windowSize,
                                       const uint64_t windowSlide,
                                       PagedVectorSize left,
                                       PagedVectorSize right)
    : StreamJoinOperatorHandler(inputOrigins, outputOriginId, windowSize, windowSlide), left(left), right(right) {}

void* getNLJPagedVectorProxy(void* ptrNljSlice, uint64_t workerId, uint64_t joinBuildSideInt) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

void* getNLJPagedVectorProxy(void* ptrNljSlice, WorkerThreadId workerThreadId, uint64_t joinBuildSideInt) {
    NES_THROW_RUNTIME_ERROR("Not Implemented!");
}

}// namespace NES::Runtime::Execution::Operators
