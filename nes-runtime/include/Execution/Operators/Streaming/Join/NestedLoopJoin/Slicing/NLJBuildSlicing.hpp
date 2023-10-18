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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_SLICING_NLJBUILDSLICING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_SLICING_NLJBUILDSLICING_HPP_
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
 * corresponding slice one after the other. Afterwards, the second phase (NLJProbe) will start joining the tuples
 * via two nested loops.
 */
class NLJBuildSlicing : public StreamJoinBuild {
  public:
    /**
     * @brief Local state, which stores the slice start, slice end, joinOpHandlerReference, sliceReference, and pagedVectorRef
     */
    class LocalNestedLoopJoinState : public Operators::OperatorState {
      public:
        LocalNestedLoopJoinState(const Value<MemRef>& operatorHandler,
                                 const Value<MemRef>& sliceReference,
                                 Nautilus::Interface::PagedVectorRef pagedVectorRef)
            : joinOperatorHandler(operatorHandler), sliceReference(sliceReference), pagedVectorRef(std::move(pagedVectorRef)),
              sliceStart(0_u64), sliceEnd(0_u64) {};
        Value<MemRef> joinOperatorHandler;
        Value<MemRef> sliceReference;
        Nautilus::Interface::PagedVectorRef pagedVectorRef;
        Value<UInt64> sliceStart;
        Value<UInt64> sliceEnd;
    };

    /**
     * @brief Constructor for a NLJBuildSlicing
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     * @param entrySize
     * @param timeFunction
     * @param joinStrategy
     * @param windowingStrategy
     */
    NLJBuildSlicing(const uint64_t operatorHandlerIndex,
                    const SchemaPtr& schema,
                    const std::string& joinFieldName,
                    const QueryCompilation::JoinBuildSideType joinBuildSide,
                    const uint64_t entrySize,
                    TimeFunctionPtr timeFunction,
                    QueryCompilation::StreamJoinStrategy joinStrategy,
                    QueryCompilation::WindowingStrategy windowingStrategy);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
     * @brief Updates the localJoinState by getting the values via Nautilus::FunctionCalls()
     * @param localJoinState: The pointer to the joinstate that we want to update
     * @param operatorHandlerMemRef: MemRef to the operator handler
     * @param timestamp: Timestamp, for which to get the sliceRef, sliceStart, and sliceEnd
     * @param workerId: WorkerId necessary for getting the correct pagedVectorRef
     */
    void updateLocalJoinState(LocalNestedLoopJoinState* localJoinState,
                              Nautilus::Value<Nautilus::MemRef>& operatorHandlerMemRef,
                              Nautilus::Value<Nautilus::UInt64>& timestamp,
                              Nautilus::Value<Nautilus::UInt64>& workerId) const;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_SLICING_NLJBUILDSLICING_HPP_
