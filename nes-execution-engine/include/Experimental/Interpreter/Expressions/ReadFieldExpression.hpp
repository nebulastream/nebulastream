#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_

#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Record.hpp>

namespace NES::Interpreter {

class ReadFieldExpression : public Expression {
  private:
    uint64_t fieldIndex;

  public:
    ReadFieldExpression(uint64_t fieldIndex);
    Value<> execute(Record& record) override;
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_
