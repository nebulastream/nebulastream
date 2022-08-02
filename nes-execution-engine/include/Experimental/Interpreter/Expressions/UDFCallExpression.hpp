#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_EXPRESSIONS_UDFCALLEXPRESSION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_EXPRESSIONS_UDFCALLEXPRESSION_HPP_

#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/Expression.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

class UDFCallExpression : public Expression {
  private:
    ExpressionPtr argument;

  public:
    UDFCallExpression(ExpressionPtr argument);
    Value<> execute(Record& record) override;
};
}// namespace NES::ExecutionEngine::Experimental::Interpreter


#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_EXPRESSIONS_UDFCALLEXPRESSION_HPP_
