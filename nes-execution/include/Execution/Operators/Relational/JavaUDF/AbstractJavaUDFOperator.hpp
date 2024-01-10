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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_ABSTRACTJAVAUDFOPERATOR_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_ABSTRACTJAVAUDFOPERATOR_HPP_

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/OperatorState.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief An abstract java udf operator, which is used by the Map and Flatmap operator.
 * Its state is managed inside a JavaUDFOperatorHandler.
 */
class AbstractJavaUDFOperator : public ExecutableOperator {
  protected:
    class LocalUDFState : public OperatorState {
      public:
        explicit LocalUDFState(Value<MemRef>& handler, Value<MemRef>& instance) : handler(handler), instance(instance) {}
        Value<MemRef> handler;
        Value<MemRef> instance;
    };

  public:
    /**
     * @brief Creates a AbstractJavaUDFOperator operator
     * @param operatorHandlerIndex The index to a valid JavaUDFOperatorHandler
     * @param operatorInputSchema The input schema of the flat map operator.
     * @param operatorOutputSchema The output schema of the flat map operator.
     */
    AbstractJavaUDFOperator(uint64_t operatorHandlerIndex, SchemaPtr operatorInputSchema, SchemaPtr operatorOutputSchema);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

  protected:
    const uint64_t operatorHandlerIndex;

    // These needs to be the same Schemas as used in the operator handler.
    // We need them here to support some functionality during for-loops in execute where we cannot access the handler.
    const SchemaPtr operatorInputSchema, operatorOutputSchema;

    Nautilus::Value<MemRef> createInputPojo(Record& record, Value<MemRef>& handler) const;
    Record extractRecordFromPojo(const Value<MemRef>& handler, const Value<MemRef>& outputPojoPtr) const;
};

}// namespace NES::Runtime::Execution::Operators

#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JAVAUDF_ABSTRACTJAVAUDFOPERATOR_HPP_
