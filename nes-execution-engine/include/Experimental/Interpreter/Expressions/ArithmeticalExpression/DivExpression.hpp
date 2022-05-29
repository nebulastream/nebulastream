#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_DIVEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_DIVEXPRESSION_HPP_

namespace NES::Interpreter {

class DivExpression : public Expression {
  private:
    ExpressionPtr leftSubExpression;
    ExpressionPtr rightSubExpression;

  public:
    Value<> execute(Record& record) override;
};

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_DIVEXPRESSION_HPP_
