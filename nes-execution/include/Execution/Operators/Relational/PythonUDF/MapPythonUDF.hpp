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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_MAPPYTHONUDF_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_MAPPYTHONUDF_HPP_

#ifdef NAUTILUS_PYTHON_UDF_ENABLED

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief This operator evaluates a map expression defined as a python function on input records.
 * Its state is managed inside a PythonUDFOperatorHandler.
 */
class MapPythonUDF : public ExecutableOperator {
  public:
    MapPythonUDF(uint64_t operatorHandlerIndex, SchemaPtr inputSchema, SchemaPtr outputSchema)
        : operatorHandlerIndex(operatorHandlerIndex), inputSchema(inputSchema), outputSchema(outputSchema){};
    void execute(ExecutionContext& ctx, Record& record) const override;
    void terminate(ExecutionContext& ctx) const override;

  private:
    const uint64_t operatorHandlerIndex;

    // These needs to be the same Schemas as used in the operator handler.
    // We need them here to support some functionality during for-loops in execute where we cannot access the handler.
    const SchemaPtr inputSchema, outputSchema;
};

}// namespace NES::Runtime::Execution::Operators

#endif // NAUTILUS_PYTHON_UDF_ENABLED
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_PYTHONUDF_MAPPYTHONUDF_HPP_
