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
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Execution/Expressions/Functions/SqrtExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

SqrtExpression::SqrtExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : subExpression(subExpression) {}

/**
 * @brief This method calculates the square root of x.
 * This function is basically a wrapper for std::sqrt and enables us to use it in our execution engine framework.
 * @param x double
 * @return double
 */
double calculateSqrt(double x) { return std::sqrt(x); }

Value<> SqrtExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the sub expression and retrieve the value.
    Value value = subExpression->execute(record);

    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // value.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateSqrt function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (value->isType<Int8>()) {
        // call the calculateSqrt function with the correct type
        return FunctionCall<>("calculateSqrt", calculateSqrt, value.as<Int8>());
    } else if (value->isType<Int16>()) {
        return FunctionCall<>("calculateSqrt", calculateSqrt, value.as<Int16>());
    } else if (value->isType<Int32>()) {
        return FunctionCall<>("calculateSqrt", calculateSqrt, value.as<Int32>());
    } else if (value->isType<Int64>()) {
        return FunctionCall<>("calculateSqrt", calculateSqrt, value.as<Int64>());
    } else if (value->isType<Float>()) {
        return FunctionCall<>("calculateSqrt", calculateSqrt, value.as<Float>());
    } else if (value->isType<Double>()) {
        return FunctionCall<>("calculateSqrt", calculateSqrt, value.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<SqrtExpression>> sqrtFunction("sqrt");
}// namespace NES::Runtime::Execution::Expressions