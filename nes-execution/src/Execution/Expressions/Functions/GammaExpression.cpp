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
#include <Execution/Expressions/Functions/GammaExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
namespace NES::Runtime::Execution::Expressions {

GammaExpression::GammaExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& subExpression)
    : SubExpression(subExpression) {}

/**
 * @brief This method calculates gamma(x).
 * @param x double
 * @return double
 */
double calculateGamma(double x) { return std::tgamma(x); }

Value<> GammaExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value leftValue = SubExpression->execute(record);

    if (leftValue->isType<Int8>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<Int8>());
    } else if (leftValue->isType<Int16>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<Int16>());
    } else if (leftValue->isType<Int32>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<Int32>());
    } else if (leftValue->isType<Int64>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<Int64>());
    } else if (leftValue->isType<UInt8>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<UInt8>());
    } else if (leftValue->isType<UInt16>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<UInt16>());
    } else if (leftValue->isType<UInt32>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<UInt32>());
    } else if (leftValue->isType<UInt64>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<UInt64>());
    } else if (leftValue->isType<Float>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<Float>());
    } else if (leftValue->isType<Double>()) {
        return FunctionCall<>("calculateGamma", calculateGamma, leftValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        throw Exceptions::NotImplementedException(
            "This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}
static ExecutableFunctionRegistry::Add<UnaryFunctionProvider<GammaExpression>> gammaFunction("gamma");
}// namespace NES::Runtime::Execution::Expressions