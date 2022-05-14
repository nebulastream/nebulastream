#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SELECTION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SELECTION_HPP_
#include <Interpreter/Expressions/Expression.hpp>
#include <Interpreter/Operators/ExecuteOperator.hpp>

namespace NES::Interpreter {

class Selection : public ExecuteOperator {
  public:
    Selection(ExpressionPtr expression) : expression(expression){};
    void open() override;
    void execute(Record& record) override;

  private:
    ExpressionPtr expression;
};

}// namespace NES::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_SELECTION_HPP_
