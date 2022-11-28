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

#include <Execution/Expressions/Functions/CeilExpression.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>

namespace NES::Runtime::Execution::Expressions {

CeilExpression::CeilExpression(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression)
    : leftSubExpression(leftSubExpression) {}

/**
* @brief This method calculates the modulus ceil of x .
* This function is basically a wrapper for std::ceil and enables us to use it in our execution engine framework.
* @param x double
* @return double
*/
double calculateCeil(double x) { return std::ceil(x); }

Value<> CeilExpression::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value leftValue = leftSubExpression->execute(record);

    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // leftValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateCeil function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (leftValue->isType<Int8>()) {
        // call the calculateMod function with the correct type
        return FunctionCall<>("calculateMod", calculateCeil, leftValue.as<Int8>());
    } else if (leftValue->isType<Int16>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<Int16>());
    } else if (leftValue->isType<Int32>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<Int32>());
    } else if (leftValue->isType<Int64>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<Int64>());
    }else if (leftValue->isType<UInt8>()) {
        return FunctionCall<>("calculateMod", calculateCeil, leftValue.as<UInt8>());
    } else if (leftValue->isType<UInt16>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<UInt16>());
    } else if (leftValue->isType<UInt32>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<UInt32>());
    } else if (leftValue->isType<UInt64>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<UInt64>());
    } else if (leftValue->isType<Float>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<Float>());
    } else if (leftValue->isType<Double>()) {
        return FunctionCall<>("calculateCeil", calculateCeil, leftValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}

}// namespace NES::Runtime::Execution::Expressions
