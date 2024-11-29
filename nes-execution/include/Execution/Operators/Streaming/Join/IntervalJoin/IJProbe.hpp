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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJPROBE_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJPROBE_HPP_

#include <Execution/MemoryProvider/MemoryProvider.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinProbe.hpp>

namespace NES::Runtime::Execution::Operators {
class IJProbe : public Operator {
  public:
    /**
     * @brief Constructor for a NLJProbe join phase
     * @param operatorHandlerIndex
     * @param joinSchema
     * @param joinFieldNameLeft
     * @param joinFieldNameRight
     * @param leftSchema
     * @param rightSchema
     */
    IJProbe(const uint64_t operatorHandlerIndex,
            Expressions::ExpressionPtr joinExpression,
            const std::string& intervalStartField,
            const std::string& intervalEndField,
            TimeFunctionPtr timeFunction,
            const SchemaPtr& leftSchema,
            const SchemaPtr& rightSchema);

    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

    /**
     * @brief Performs the join itself for two records. Adds also three metadata columns
     * @param joinedRecord
     * @param leftRecord
     * @param rightRecord
     * @param keyValue
     * @param intervalStart
     * @param intervalEnd
     */
    void createJoinedRecord(Record& joinedRecord,
                            Record& leftRecord,
                            Record& rightRecord,
                            Value<Int64> intervalStart,
                            Value<Int64> intervalEnd) const;

    /**
     * @brief Checks the current watermark and then deletes all intervals that are not valid anymore
     * @param executionCtx
     * @param recordBuffer
     */
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

    void terminate(ExecutionContext& executionCtx) const override;

  protected:
    const uint64_t operatorHandlerIndex;
    const SchemaPtr leftSchema;
    const SchemaPtr rightSchema;
    Expressions::ExpressionPtr joinExpression;
    const std::string intervalStartField;
    const std::string intervalEndField;
    TimeFunctionPtr timeFunction;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_JOIN_INTERVALJOIN_IJPROBE_HPP_
