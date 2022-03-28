#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Expressions/Expression.hpp>
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_MULEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_MULEXPRESSION_HPP_

namespace NES::Interpreter {

class SubExpression : public Expression {
  private:
    ExpressionPtr leftSubExpression;
    ExpressionPtr rightSubExpression;

  public:
    Value<> execute(Record& record) override;
};

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_MULEXPRESSION_HPP_
