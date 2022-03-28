#include <Interpreter/Expressions/EqualsExpression.hpp>

namespace NES::Interpreter {

EqualsExpression::EqualsExpression(ExpressionPtr leftSubExpression, ExpressionPtr rightSubExpression)
    : leftSubExpression(std::move(leftSubExpression)), rightSubExpression(rightSubExpression){};

Value<> EqualsExpression::execute(Record& record) {
    Value<> leftValue = leftSubExpression->execute(record);
    Value<> rightValue = rightSubExpression->execute(record);
    return leftValue == rightValue;
}

}// namespace NES::Interpreter