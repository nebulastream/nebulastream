#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLEWINDOWOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLEWINDOWOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableWindowOperator : public GeneratableOperator {

  protected:
    GeneratableWindowOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Windowing::WindowOperatorHandlerPtr operatorHandler);
    Windowing::WindowOperatorHandlerPtr operatorHandler;
};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES

#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_WINDOWING_GENERATABLEWINDOWOPERATOR_HPP_
