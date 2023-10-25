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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINBUILD_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINBUILD_HPP_

#include <API/Schema.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperator.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Util/Common.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;

/**
 * @brief This class is the first phase of the stream join. The actual implementation (e.g., storing tuples, building hash tables, ...)
 * is not part of this class. This class takes care of the close() and terminate() functionality as these are universal
 */
class StreamJoinBuild : public StreamJoinOperator, public ExecutableOperator {
  public:
    /**
     * @brief Constructor for a StreamJoinBuild
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     * @param entrySize
     * @param timeFunction
     * @param joinStrategy
     * @param windowingStrategy
     */
    StreamJoinBuild(const uint64_t operatorHandlerIndex,
                    const SchemaPtr& schema,
                    const std::string& joinFieldName,
                    const QueryCompilation::JoinBuildSideType joinBuildSide,
                    const uint64_t entrySize,
                    TimeFunctionPtr timeFunction,
                    QueryCompilation::StreamJoinStrategy joinStrategy,
                    QueryCompilation::WindowingStrategy windowingStrategy);

    /**
     * @brief Updates the watermark and if needed, pass some slices to the second join phase (NLJProbe) for further processing
     * @param ctx: The RuntimeExecutionContext
     * @param recordBuffer: RecordBuffer
     */
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Triggers all slices that have been seen by both sides of the join
     * @param executionCtx
     */
    void terminate(ExecutionContext& executionCtx) const override;

    void setup(ExecutionContext& executionCtx) const override;

  protected:
    const uint64_t operatorHandlerIndex;
    const SchemaPtr schema;
    const std::string joinFieldName;
    const QueryCompilation::JoinBuildSideType joinBuildSide;
    const uint64_t entrySize;
    const TimeFunctionPtr timeFunction;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_STREAMJOINBUILD_HPP_
