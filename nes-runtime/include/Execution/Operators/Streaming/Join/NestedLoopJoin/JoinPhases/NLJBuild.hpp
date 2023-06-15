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

namespace NES::Runtime::Execution::Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;

/**
 * @brief This class is the first phase of the join. For both streams (left and right), the tuples are stored in the
 * corresponding window one after the other. Afterwards, the second phase (NLJSink) will start joining the tuples
 * via two nested loops.
 */
class NLJBuild : public ExecutableOperator {

  public:
    /**
     * @brief Constructor for a NLJBuild
     * @param operatorHandlerIndex
     * @param schema
     * @param joinFieldName
     * @param timeStampField
     * @param isLeftSide
     */
    NLJBuild(uint64_t operatorHandlerIndex,
             const SchemaPtr schema,
             const std::string& joinFieldName,
             const std::string& timeStampField,
             bool isLeftSide,
             std::shared_ptr<TimeFunction> timeFunction);


    void open(ExecutionContext& ctx, RecordBuffer &recordBuffer) const override;

    /**
     * @brief Stores the record in the corresponding window
     * @param ctx
     * @param record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

    /**
     * @brief Updates the watermark and if needed, pass some windows to the second join phase (NLJSink) for further processing
     * @param ctx
     * @param recordBuffer
     */
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    SchemaPtr schema;
    std::string joinFieldName;
    std::string timeStampField;
    const bool isLeftSide;
    std::shared_ptr<TimeFunction> timeFunction;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_NESTEDLOOPJOIN_JOINPHASES_NLJBUILD_HPP_
