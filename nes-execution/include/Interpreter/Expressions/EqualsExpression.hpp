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
    EqualsExpression(ExpressionPtr leftSubExpression, ExpressionPtr rightSubExpression)
        : leftSubExpression(std::move(leftSubExpression)), rightSubExpression(rightSubExpression){};
    Value<> execute(Record& record) override {
        Value leftValue = leftSubExpression->execute(record);
        Value rightValue = rightSubExpression->execute(record);
        return leftValue == rightValue;
    }
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EQUALSEXPRESSION_HPP_
