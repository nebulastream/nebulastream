#include <Experimental/Interpreter/Expressions/ConstantIntegerExpression.hpp>
namespace NES::ExecutionEngine::Experimental::Interpreter {

ConstantIntegerExpression::ConstantIntegerExpression(int64_t integerValue): integerValue(integerValue) {}

Value<> ConstantIntegerExpression::execute(Record&) {
    return Value<Integer>(integerValue);
}

}