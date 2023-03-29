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
#include <Execution/Expressions/Functions/CosExpression.hpp>
#include <Execution/Expressions/Functions/ExecutableFunctionRegistry.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

CosExpression::CosExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression)
    : leftSubExpression(leftSubExpression) {}

/**
 * @brief This method calculates the cosinus of x.
 * This function is basically a wrapper for std::cos and enables us to use it in our execution engine framework.
 * @param x double
 * @return double
 */
double calculateCos(double x) { return std::cos(x); }

Value<> CosExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the  expression and retrieve the value.
    Value leftValue = leftSubExpression->execute(record);

    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // leftValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateMod function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (leftValue->isType<Int8>()) {
        // call the calculateCos function with the correct type
        return FunctionCall<>("calculateCos", calculateCos, leftValue.as<Int8>());
    } else if (leftValue->isType<Int16>()) {
        return FunctionCall<>("calculateCos", calculateCos, leftValue.as<Int16>());
    } else if (leftValue->isType<Int32>()) {
        return FunctionCall<>("calculateCos", calculateCos, leftValue.as<Int32>());
    } else if (leftValue->isType<Int64>()) {
        return FunctionCall<>("calculateCos", calculateCos, leftValue.as<Int64>());
    } else if (leftValue->isType<Float>()) {
        return FunctionCall<>("calculateCos", calculateCos, leftValue.as<Float>());
    } else if (leftValue->isType<Double>()) {
        return FunctionCall<>("calculateCos", calculateCos, leftValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<CosExpression>> cosFunction("cos");
}// namespace NES::Runtime::Execution::Expressions
