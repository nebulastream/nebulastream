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
#include <Execution/Expressions/TextFunctions/PatternMatching/ReplacingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

ReplacingRegex::ReplacingRegex(const NES::Runtime::Execution::Expressions::ExpressionPtr& textValue,
                               const NES::Runtime::Execution::Expressions::ExpressionPtr& regexpPattern,
                               const NES::Runtime::Execution::Expressions::ExpressionPtr& textReplacement)
    : textValue(textValue), regexpPattern(regexpPattern), textReplacement(textReplacement) {}

/**
* @brief This Method replaces the first occurrence of regex with the replacement
* This function is basically a wrapper for std::regex_replace and enables us to use it in our execution engine framework.
* @param text TextValue* the text (string) to extract the regexpPattern from
* @param regex TextValue* the pattern to match
* @param replacement TextValue* the text (string) to replacement a pattern match
* @return TextValue*
*/
TextValue* regexReplace(TextValue* text, TextValue* reg, TextValue* replacement) {
    std::string strText = std::string(text->c_str(), text->length());
    NES_DEBUG("Received the following source string {}", strText);
    std::regex tempRegex(std::string(reg->c_str(), reg->length()));
    NES_DEBUG("Received the following reg string {}", std::string(reg->c_str(), reg->length()));
    std::string strReplacement = std::string(replacement->c_str(), replacement->length());
    NES_DEBUG("Received the following replacement string {}", strReplacement);
    std::string strReplaced = std::regex_replace(strText, tempRegex, strReplacement);
    NES_DEBUG("Created the string {}", strReplaced)
    return TextValue::create(strReplaced);
}

Value<> ReplacingRegex::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> text = textValue->execute(record);

    // Evaluate the mid-sub expression and retrieve the value.
    Value<> pattern = regexpPattern->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> replacement = textReplacement->execute(record);

    if (text->isType<Text>() && pattern->isType<Text>() && replacement->isType<Text>()) {

        return FunctionCall<>("regexReplace",
                              regexReplace,
                              text.as<Text>()->getReference(),
                              pattern.as<Text>()->getReference(),
                              replacement.as<Text>()->getReference());
    } else {
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments of type Text.");
    }
}

}// namespace NES::Runtime::Execution::Expressions