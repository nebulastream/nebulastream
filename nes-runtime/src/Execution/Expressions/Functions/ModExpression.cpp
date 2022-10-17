#include <Execution/Expressions/Functions/ModExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

ModExpression::ModExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                             const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

double calculateModOnDouble(double x, double y) { return std::fmod(x, y); }

Value<> ModExpression::execute(NES::Nautilus::Record& record) const {
    Value leftValue = leftSubExpression->execute(record);
    Value rightValue = rightSubExpression->execute(record);
    Value<> result =
        FunctionCall<>("calculateModOnDouble", calculateModOnDouble, leftValue.as<Float>(), rightValue.as<Float>());
    return result;
}

}// namespace NES::Runtime::Execution::Expressions