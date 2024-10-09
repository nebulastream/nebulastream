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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJBUILDSLICINGVARSIZED_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJBUILDSLICINGVARSIZED_HPP_

#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>

namespace NES::Runtime::Execution::Operators {

class HJBuildSlicingVarSized;
using HJBuildSlicingVarSizedPtr = std::shared_ptr<HJBuildSlicingVarSized>;

/**
 * @brief This class inserts a record into the correct slice. It checks the timestamp and then retrieves the correct slice
 * from the operator handler. The insertion is done via a Nautilus::FunctionCall that proxies to the hashTable->insert() and
 * returns a pointer to the appropriate underlying pagedVector.
 */
class HJBuildSlicingVarSized : public virtual StreamJoinBuild {
  public:
    /**
     * @brief Constructor for a HJBuildSlicingVarSized join phase
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param joinBuildSide
     * @param entrySize
     * @param timeFunction
     * @param joinStrategy
     */
    HJBuildSlicingVarSized(const uint64_t operatorHandlerIndex,
                           const SchemaPtr& schema,
                           const std::string& joinFieldName,
                           const QueryCompilation::JoinBuildSideType joinBuildSide,
                           const uint64_t entrySize,
                           TimeFunctionPtr timeFunction,
                           QueryCompilation::StreamJoinStrategy joinStrategy);

    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const std::string joinFieldName;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_HASHJOIN_SLICING_HJBUILDSLICINGVARSIZED_HPP_
