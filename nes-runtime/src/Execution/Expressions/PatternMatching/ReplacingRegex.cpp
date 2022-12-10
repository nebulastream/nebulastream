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
#include <Execution/Expressions/PatternMatching/ReplacingRegex.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <cmath>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

ReplacingRegex::ReplacingRegex(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                               const NES::Runtime::Execution::Expressions::ExpressionPtr& midSubExpression,
                             const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), midSubExpression(midSubExpression), rightSubExpression(rightSubExpression) {}

/**
 * @brief This Method replaces the first occurrence of regex with the replacement
 * This function is basically a wrapper for std::regex_replace and enables us to use it in our execution engine framework.
 * @param text std::string
 * @param regex std::regex
 * @param replacement std::string
 * @return std::string
 */
Value<Text> regex_replace(Value<Text> text, Value<Text> regex, Value<Text> replacement) { return std::regex_replace(text, regex, replacement) ; } // Ask: Alle 5 Parameter gewünscht?

Value<Text> ReplacingRegex::execute(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value<Text> leftValue = leftSubExpression->execute(record);
    // Evaluate the left sub expression and retrieve the value.
    Value<Text> midValue = midSubExpression->execute(record);
    // Evaluate the right sub expression and retrieve the value.
    Value<Text> rightValue = rightSubExpression->execute(record);

    // Ask: Fallunterscheidung notwendig? std::string oder TextValue
    if (leftValue->isEquals(NES::DataTypePtr Val ue<Text>) && midValue->isEquals(Value<Text>) && rightValue->isEquals(Value<Text>)) {
        leftValue->
    }
    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // leftValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateMod function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.
    if (leftValue->isType<Int8>() && rightValue->isType<Int8>()) {
        // call the calculateMax function with the correct type
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<Int8>(), rightValue.as<Int8>());
    } else if (leftValue->isType<Int16>() && rightValue->isType<Int16>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<Int16>(), rightValue.as<Int16>());
    } else if (leftValue->isType<Int32>() && rightValue->isType<Int32>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<Int32>(), rightValue.as<Int32>());
    } else if (leftValue->isType<Int64>() && rightValue->isType<Int64>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<Int64>(), rightValue.as<Int64>());
    } else if (leftValue->isType<UInt8>() && rightValue->isType<UInt8>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<UInt8>(), rightValue.as<UInt8>());
    } else if (leftValue->isType<UInt16>() && rightValue->isType<UInt16>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<UInt16>(), rightValue.as<UInt16>());
    } else if (leftValue->isType<UInt32>() && rightValue->isType<UInt32>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<UInt32>(), rightValue.as<UInt32>());
    } else if (leftValue->isType<UInt64>() && rightValue->isType<UInt64>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<UInt64>(), rightValue.as<UInt64>());
    } else if (leftValue->isType<Float>() && rightValue->isType<Float>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<Float>(), rightValue.as<Float>());
    } else if (leftValue->isType<Double>() && rightValue->isType<Double>()) {
        return FunctionCall<>("calculateMax", calculateMax, leftValue.as<Double>(), rightValue.as<Double>());
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on numeric input arguments that are either Integer or Float.");
    }
}

}// namespace NES::Runtime::Execution::Expressions