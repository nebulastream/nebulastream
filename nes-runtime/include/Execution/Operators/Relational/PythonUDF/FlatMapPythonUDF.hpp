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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_FLATMAPPYTHONUDF_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_FLATMAPPYTHONUDF_HPP_

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This operator evaluates a flat map expression defined as java function on input records.
 * Its state is managed inside a PythonUDFOperatorHandler.
 */
class FlatMapPythonUDF : public ExecutableOperator {
  public:
    /**
     * @brief Creates a FlatMapPythonUDF operator
     * @param operatorHandlerIndex The index to a valid PythonUDFOperatorHandler
     * @param operatorInputSchema The input schema of the flat map operator.
     * @param operatorOutputSchema The output schema of the flat map operator.
     */
    FlatMapPythonUDF(uint64_t operatorHandlerIndex, SchemaPtr inputSchema, SchemaPtr outputSchema, std::string pythonCompiler);
    /**
     * Operator execution function
     * @param ctx operator context
     * @param record input record
     */
    void execute(ExecutionContext& ctx, Record& record) const override;

  protected:
    const uint64_t operatorHandlerIndex;

    // These needs to be the same Schemas as used in the operator handler.
    // We need them here to support some functionality during for-loops in execute where we cannot access the handler.
    const SchemaPtr operatorInputSchema, operatorOutputSchema;
    const std::string pythonCompiler;

    void createInputPojo(Record& record, Value<MemRef>& handler) const;
    Record extractRecordFromObject(const Value<MemRef>& outputPojoPtr) const;
};

}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_FLATMAPPYTHONUDF_HPP_
