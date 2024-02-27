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
#include <Execution/Expressions/Functions/ModExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

ModExpression::ModExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                             const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), rightSubExpression(rightSubExpression) {}

/**
 * @brief This method calculates the modulus between x and y.
 * This function is basically a wrapper for std::fmod and enables us to use it in our execution engine framework.
 * @param x double
 * @param y double
 * @return double
 */
double calculateMod(double x, double y) { return std::fmod(x, y); }

Value<> ModExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value leftValue = leftSubExpression->execute(record);
    // Evaluate the right sub expression and retrieve the value.
    Value rightValue = rightSubExpression->execute(record);

    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // leftValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateMod function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (leftValue->isType<Int8>() && rightValue->isType<Int8>()) {
        // call the calculateMod function with the correct type
        return leftValue.as<Int8>() % rightValue.as<Int8>();
    } else if (leftValue->isType<Int16>() && rightValue->isType<Int16>()) {
        return leftValue.as<Int16>() % rightValue.as<Int16>();
    } else if (leftValue->isType<Int32>() && rightValue->isType<Int32>()) {
        return leftValue.as<Int32>() % rightValue.as<Int32>();
    } else if (leftValue->isType<Int64>() && rightValue->isType<Int64>()) {
        return leftValue.as<Int64>() % rightValue.as<Int64>();
    } else if (leftValue->isType<UInt8>() && rightValue->isType<UInt8>()) {
        return leftValue.as<UInt8>() % rightValue.as<UInt8>();
    } else if (leftValue->isType<UInt16>() && rightValue->isType<UInt16>()) {
        return leftValue.as<UInt16>() % rightValue.as<UInt16>();
    } else if (leftValue->isType<UInt32>() && rightValue->isType<UInt32>()) {
        return leftValue.as<UInt32>() % rightValue.as<UInt32>();
    } else if (leftValue->isType<UInt64>() && rightValue->isType<UInt64>()) {
        return leftValue.as<UInt64>() % rightValue.as<UInt64>();
    } else if (leftValue->isType<Float>() && rightValue->isType<Float>()) {
        // fp-mod implementation is currently not implemented in the MLIR Backend, so we use a proxy call to fmod here.
        return FunctionCall<>("calculateMod", calculateMod, leftValue.as<Float>(), rightValue.as<Float>());
    } else if (leftValue->isType<Double>() && rightValue->isType<Double>()) {
        return FunctionCall<>("calculateMod", calculateMod, leftValue.as<Double>(), rightValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<BinaryFunctionProvider<ModExpression>> modFunction("mod");
}// namespace NES::Runtime::Execution::Expressions