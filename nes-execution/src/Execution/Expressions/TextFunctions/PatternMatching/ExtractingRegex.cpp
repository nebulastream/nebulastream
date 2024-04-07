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
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

ExtractingRegex::ExtractingRegex(const NES::Runtime::Execution::Expressions::ExpressionPtr& textValue,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& regexpPattern,
                                 const NES::Runtime::Execution::Expressions::ExpressionPtr& idx)
    : textValue(textValue), regexpPattern(regexpPattern), idx(idx) {}

/**
* @brief This Method returns a text sequence from the target text, which is matching the given regex pattern.
* If multiple text sequences, would match the given regex pattern the idx parameter can be used to select a match.
* @param text TextValue* the text (string) to extract the regexpPattern from
* @param reg TextValue* the pattern to extract
* @param idx result index as Integer defines at what position in the textValue matching starts (only the first match is returned)
* @return TextValue*
*/
TextValue* regexExtract(TextValue* text, TextValue* reg, int idx) {
    std::string strText = std::string(text->c_str(), text->length());
    std::regex tempRegex(std::string(reg->c_str(), reg->length()));
    std::smatch result;
    std::regex_search(strText, result, tempRegex);
    return TextValue::create(result[idx]);
}

Value<> ExtractingRegex::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> text = textValue->execute(record);

    // Evaluate the mid-sub expression and retrieve the value.
    Value<> pattern = regexpPattern->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value pos = idx->execute(record);

    if (text->isType<Text>() && pattern->isType<Text>()) {

        if (pos->isType<Int8>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<Int8>());
        } else if (pos->isType<Int16>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<Int16>());
        } else if (pos->isType<Int32>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<Int32>());
        } else if (pos->isType<Int64>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<Int64>());
        } else if (pos->isType<UInt8>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<UInt8>());
        } else if (pos->isType<UInt16>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<UInt16>());
        } else if (pos->isType<UInt32>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<UInt32>());
        } else if (pos->isType<UInt64>()) {
            return FunctionCall<>("regexExtract",
                                  regexExtract,
                                  text.as<Text>()->getReference(),
                                  pattern.as<Text>()->getReference(),
                                  pos.as<UInt64>());
        } else {
            // If no type was applicable we throw an exception.
            NES_THROW_RUNTIME_ERROR("Idx only defined on a numeric input argument that is of type Integer.");
        }
    } else {
        NES_THROW_RUNTIME_ERROR("This expression is only defined for inputs of type Text and an index as Integer.");
    }
}

}// namespace NES::Runtime::Execution::Expressions