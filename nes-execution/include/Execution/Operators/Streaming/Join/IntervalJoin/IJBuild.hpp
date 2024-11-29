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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJBUILD_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJBUILD_HPP_

#include <Execution/Operators/Streaming/Join/IntervalJoin/IJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorVarSizedRef.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This class is the build phase of the interval join.
 * For each join side (left and right), one IJBuild object is created. Depending if an incoming tuple
 * belongs to the left or the right stream, it is handled by the respective IJBuildLeft instance.
 */
class IJBuild : public ExecutableOperator {

  public:
    /**
     * @brief Constructor for a IntervalJoinBuild
     * @param operatorHandlerIndex
     * @param buildSideSchema
     * @param otherSideSchema
     * @param timeFunction
     */
    IJBuild(const uint64_t operatorHandlerIndex,
            const SchemaPtr buildSideSchema,
            const SchemaPtr otherSideSchema,
            TimeFunctionPtr timeFunction);

    /**
     * @brief Gets called whenever a tupleBuffer was completely processed.
     * @param ctx
     * @param recordBuffer
     */
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  protected:
    const uint64_t operatorHandlerIndex;
    const SchemaPtr buildSideSchema;
    const SchemaPtr otherSideSchema;
    const TimeFunctionPtr timeFunction;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJBUILD_HPP_
