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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_

#include <Execution/Operators/Streaming/Join/NestedLoopJoin/NLJSlice.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This task models the information for a join window trigger, so what left and right slice identifier to join together
 */
struct EmittedNLJWindowTriggerTask {
    uint64_t leftSliceIdentifier;
    uint64_t rightSliceIdentifier;
    WindowInfo windowInfo;
};

class NLJOperatorHandler;
using NLJOperatorHandlerPtr = std::shared_ptr<NLJOperatorHandler>;

class NLJOperatorHandler : public virtual StreamJoinOperatorHandler {
  public:
    NLJOperatorHandler(const std::vector<OriginId>& inputOrigins,
                       const OriginId outputOriginId,
                       const uint64_t windowSize,
                       const uint64_t windowSlide,
                       const SchemaPtr& leftSchema,
                       const SchemaPtr& rightSchema,
                       const uint64_t pageSizeLeft,
                       const uint64_t pageSizeRight);

    ~NLJOperatorHandler() override = default;

    StreamSlicePtr createNewSlice(uint64_t sliceStart, uint64_t sliceEnd) override;

    void emitSliceIdsToProbe(StreamSlice& sliceLeft,
                             StreamSlice& sliceRight,
                             const WindowInfo& windowInfo,
                             PipelineExecutionContext* pipelineCtx) override;

  protected:
    const uint64_t pageSizeLeft;
    const uint64_t pageSizeRight;
};

/**
 * @brief Proxy function for returning the pointer to the correct PagedVector
 * @param ptrNljWindow
 * @param workerId
 * @param joinBuildSide
 * @return void* that will be translated to a MemRef
 */
void* getNLJPagedVectorProxy(void* ptrNljWindow, uint64_t workerId, uint64_t joinBuildSideInt);

};    // namespace NES::Runtime::Execution::Operators
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_NLJOPERATORHANDLER_HPP_
