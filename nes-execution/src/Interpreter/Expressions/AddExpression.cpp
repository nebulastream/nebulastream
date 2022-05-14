#include <Interpreter/Expressions/AddExpression.hpp>

namespace NES::Interpreter {

Value<> AddExpression::execute(Record& record) {
    Value leftValue = leftSubExpression->execute(record);
    Value rightValue = rightSubExpression->execute(record);
    return leftValue + rightValue;
}

}// namespace NES::Interpreter