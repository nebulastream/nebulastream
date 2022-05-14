#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Expressions/Expression.hpp>
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EQUALSEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EQUALSEXPRESSION_HPP_

namespace NES::Interpreter {

class EqualsExpression : public Expression {
  private:
    ExpressionPtr leftSubExpression;
    ExpressionPtr rightSubExpression;

  public:
    EqualsExpression(ExpressionPtr leftSubExpression, ExpressionPtr rightSubExpression);
    Value<> execute(Record& record) override;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EQUALSEXPRESSION_HPP_
