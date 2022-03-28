#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Expressions/Expression.hpp>
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_ADDEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_ADDEXPRESSION_HPP_

namespace NES {

class AddExpression : Expression {
    ExpressionPtr leftSubExpression;
    ExpressionPtr rightSubExpression;

  public:
    ValuePtr execute(RecordPtr record) override {
        auto leftValue = leftSubExpression->execute(record);
        auto rightValue = rightSubExpression->execute(record);
        return leftValue + rightValue;
    }
};

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_ADDEXPRESSION_HPP_
