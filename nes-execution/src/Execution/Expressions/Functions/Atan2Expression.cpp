/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
#include <Exceptions/NotImplementedException.hpp>
#include <Execution/Expressions/Functions/Atan2Expression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

Atan2Expression::Atan2Expression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

/**
 * @brief This method calculates the atan between x and y.
 * This function is basically a wrapper for std::atan2 and enables us to use it in our execution engine framework.
 * @param x double
 * @param y double
 * @return double
 */
double calculateAtan2(double x, double y) { return std::atan2(x, y); }

Value<> Atan2Expression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value leftValue = leftSubExpression->execute(record);
    // Evaluate the right sub expression and retrieve the value.
    Value rightValue = rightSubExpression->execute(record);
    if (leftValue->isType<Float>() && rightValue->isType<Float>()) {
        return FunctionCall<>("calculateAtan2", calculateAtan2, leftValue.as<Float>(), rightValue.as<Float>());
    } else if (leftValue->isType<Double>() && rightValue->isType<Double>()) {
        return FunctionCall<>("calculateAtan2", calculateAtan2, leftValue.as<Double>(), rightValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Double or Float.");
    }
}
static ExecutableFunctionRegistry::Add<BinaryFunctionProvider<Atan2Expression>> atan2Function("atan");
}// namespace NES::Runtime::Execution::Expressions