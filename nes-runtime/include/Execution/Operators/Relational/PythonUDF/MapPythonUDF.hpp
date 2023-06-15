#ifdef MIS_PYTHON_UDF_ENABLED
#ifndef NES_MAPPYTHONUDF_HPP
#define NES_MAPPYTHONUDF_HPP

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
class MapPythonUDF  : public ExecutableOperator {
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

#endif //NES_MAPPYTHONUDF_HPP
#endif