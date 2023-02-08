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
#include <Execution/Expressions/TextFunctions/PatternMatching/ExtractingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

ExtractingRegex::ExtractingRegex(const NES::Runtime::Execution::Expressions::ExpressionPtr& leftSubExpression,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& midSubExpression,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& rightSubExpression)
    : leftSubExpression(leftSubExpression), midSubExpression(midSubExpression), rightSubExpression(rightSubExpression) {}

/**
* @brief This Method returns a text sequence from the target text, which is matching the given regex pattern.
* If multiple text sequences, would match the given regex pattern the idx parameter can be used to select a match.
* @param text TextValue*
* @param regex TextValue*
* @param idx result index
* @return TextValue*
*/
TextValue* regexExtract(TextValue* text, TextValue* reg, int idx) {
    std::string strText = std::string(text->c_str(), text->length());
    std::regex tempRegex(std::string(reg->c_str(), reg->length()));
    std::smatch m;
    std::regex_search(strText, m, tempRegex);
    return TextValue::create(m[idx]);
}

Value<> ExtractingRegex::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> leftValue = leftSubExpression->execute(record);

    // Evaluate the mid sub expression and retrieve the value.
    Value<> midValue = midSubExpression->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value rightValue = rightSubExpression->execute(record);

    if (rightValue->isType<Int8>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<Int8>());
    } else if (rightValue->isType<Int16>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<Int16>());
    } else if (rightValue->isType<Int32>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<Int32>());
    } else if (rightValue->isType<Int64>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<Int64>());
    } else if (rightValue->isType<UInt8>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<UInt8>());
    } else if (rightValue->isType<UInt16>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<UInt16>());
    } else if (rightValue->isType<UInt32>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<UInt32>());
    } else if (rightValue->isType<UInt64>()) {
        return FunctionCall<>("regexExtract",
                              regexExtract,
                              leftValue.as<Text>()->getReference(),
                              midValue.as<Text>()->getReference(),
                              rightValue.as<UInt64>());
    } else {
        // If no type was applicable we throw an exception.
        NES_THROW_RUNTIME_ERROR("This expression is only defined on a numeric input argument that is of type Integer.");
    }
}

}// namespace NES::Runtime::Execution::Expressions