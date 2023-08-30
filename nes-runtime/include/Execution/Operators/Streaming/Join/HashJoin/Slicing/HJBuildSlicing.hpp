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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJBUILDSLICING_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJBUILDSLICING_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>

namespace NES::Runtime::Execution::Operators {

class HJBuildSlicing;
using HJBuildSlicingPtr = std::shared_ptr<HJBuildSlicing>;

class HJBuildSlicing : public StreamJoinBuild {
  public:
    /**
     * @brief Constructor for a HJBuildSlicing join phase
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     * @param entrySize
     * @param timeFunction
     * @param joinStrategy
     * @param windowingStrategy
     */
    HJBuildSlicing(const uint64_t operatorHandlerIndex,
                   const SchemaPtr& schema,
                   const std::string& joinFieldName,
                   const QueryCompilation::JoinBuildSideType joinBuildSide,
                   const uint64_t entrySize,
                   TimeFunctionPtr timeFunction,
                   QueryCompilation::StreamJoinStrategy joinStrategy,
                   QueryCompilation::WindowingStrategy windowingStrategy);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
};


}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJBUILDSLICING_HPP_
