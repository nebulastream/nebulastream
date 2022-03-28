#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
#include <memory>
namespace NES {

class Value;
using ValuePtr = std::unique_ptr<Value>;

class Record;
using RecordPtr = std::shared_ptr<Record>;

class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;


class Expression {
  public:
    virtual ValuePtr execute(RecordPtr record) = 0;
    virtual ~Expression() = default;
};

}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_EXPRESSION_HPP_
