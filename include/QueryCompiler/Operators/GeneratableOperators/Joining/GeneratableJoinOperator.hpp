#ifndef NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEJOINOPERATOR_HPP_
#define NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEJOINOPERATOR_HPP_

#include <QueryCompiler/Operators/GeneratableOperators/GeneratableOperator.hpp>

namespace NES {
namespace QueryCompilation {
namespace GeneratableOperators {

/**
 * @brief Base class for all generatable operators. It defines the general produce and consume methods as defined by Neumann.
 */
class GeneratableJoinOperator : public GeneratableOperator {
  protected:
    GeneratableJoinOperator(OperatorId id, SchemaPtr inputSchema, SchemaPtr outputSchema, Join::JoinOperatorHandlerPtr operatorHandler);
    Join::JoinOperatorHandlerPtr operatorHandler;

};
}// namespace GeneratableOperators
}// namespace QueryCompilation
}// namespace NES
#endif//NES_INCLUDE_QUERYCOMPILER_OPERATORS_GENERATABLEOPERATORS_GENERATABLEJOINOPERATOR_HPP_
