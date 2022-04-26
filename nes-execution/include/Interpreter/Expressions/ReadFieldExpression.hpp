#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_

#include <Interpreter/DataValue/Value.hpp>
#include <Interpreter/Expressions/Expression.hpp>
#include <Interpreter/Record.hpp>

namespace NES::Interpreter {

class ReadFieldExpression : public Expression {
  private:
    uint64_t fieldIndex;

  public:
    ReadFieldExpression(uint64_t fieldIndex) : fieldIndex(fieldIndex) {}
    Value<> execute(Record& record) override { return record.read(fieldIndex); }
};

}// namespace NES::Interpreter

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_READFIELDEXPRESSION_HPP_
