#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SELECTION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SELECTION_HPP_
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Operators/ExecuteOperator.hpp>

namespace NES::Interpreter {

class Selection : public ExecuteOperator {
  public:
    Selection(ExpressionPtr expression) : expression(expression){};
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const ExpressionPtr expression;
};

}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SELECTION_HPP_
