#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
#include <memory>
#include <Experimental/Interpreter/DataValue/Value.hpp>
namespace NES::Interpreter {

class Any;

class Record;
using RecordPtr = std::shared_ptr<Record>;

class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;


class Expression {
  public:
    virtual Value<> execute(Record& record) = 0;
    virtual ~Expression() = default;
};

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
