#include <Experimental/Interpreter/DataValue/Value.hpp>
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Operations/AddOp.hpp>
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_WRITEFIELDEXPRESSION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_WRITEFIELDEXPRESSION_HPP_

namespace NES {
/*
class WriteFieldExpression : public Expression {
  private:
    uint64_t fieldIndex;
    ExpressionPtr subExpression;

  public:
    Value<AnyPtr> execute(RecordPtr record) override {
        Value<AnyPtr> newValue = subExpression->execute(record);
        record->write(fieldIndex, std::move(newValue));
        return Value<AnyPtr>(nullptr);
    }
};
*/
}// namespace NES

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_EXPRESSIONS_WRITEFIELDEXPRESSION_HPP_
