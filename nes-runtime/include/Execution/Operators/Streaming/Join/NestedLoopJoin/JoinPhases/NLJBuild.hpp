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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJBUILD_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJBUILD_HPP_

#include <API/Schema.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;

/**
 * @brief This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
 * corresponding window one after the other. Afterwards, the second phase (NLJProbe) will start joining the tuples
 * via two nested loops.
 */
class NLJBuild : public ExecutableOperator {

  public:
    /**
     * @brief Local state, which stores the window start, window end, joinOpHandlerReference, windowReference, and pagedVectorRef
     */
    class LocalNestedLoopJoinState : public Operators::OperatorState {
      public:
        LocalNestedLoopJoinState(const Value<MemRef>& operatorHandler,
                                 const Value<MemRef>& windowReference,
                                 Nautilus::Interface::PagedVectorRef pagedVectorRef)
            : joinOperatorHandler(operatorHandler), windowReference(windowReference), pagedVectorRef(std::move(pagedVectorRef)),
              windowStart(0_u64), windowEnd(0_u64){};
        Value<MemRef> joinOperatorHandler;
        Value<MemRef> windowReference;
        Nautilus::Interface::PagedVectorRef pagedVectorRef;
        Value<UInt64> windowStart;
        Value<UInt64> windowEnd;
    };

    /**
     * @brief Constructor for a NLJBuild
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     */
    NLJBuild(uint64_t operatorHandlerIndex,
             const SchemaPtr& schema,
             const std::string& joinFieldName,
             const QueryCompilation::JoinBuildSideType joinBuildSide,
             TimeFunctionPtr timeFunction);

    void setup(ExecutionContext& executionCtx) const override;

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Stores the record in the corresponding window
     * @param ctx: The RuntimeExecutionContext
     * @param record: Record that should be processed
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
     * @brief Updates the watermark and if needed, pass some windows to the second join phase (NLJProbe) for further processing
     * @param ctx: The RuntimeExecutionContext
     * @param recordBuffer: RecordBuffer
     */
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Triggers all windows that have been seen by both sides of the join
     * @param executionCtx
     */
    void terminate(ExecutionContext& executionCtx) const override;

    /**
     * @brief Updates the localJoinState by getting the values via Nautilus::FunctionCalls()
     * @param localJoinState: The pointer to the joinstate that we want to update
     * @param operatorHandlerMemRef: Memref to the operator handler
     * @param timestamp: Timestamp, for which to get the windowRef, windowStart, and windowEnd
     * @param workerId: WorkerId necessary for getting the correct pagedVectorRef
     */
    void updateLocalJoinState(LocalNestedLoopJoinState* localJoinState,
                              Nautilus::Value<Nautilus::MemRef>& operatorHandlerMemRef,
                              Nautilus::Value<Nautilus::UInt64>& timestamp,
                              Nautilus::Value<Nautilus::UInt64>& workerId) const;

  private:
    const uint64_t operatorHandlerIndex;
    const SchemaPtr schema;
    const std::string joinFieldName;
    const QueryCompilation::JoinBuildSideType joinBuildSide;
    const uint64_t entrySize;
    const TimeFunctionPtr timeFunction;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJBUILD_HPP_
