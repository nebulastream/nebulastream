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
#include <Execution/Expressions/TextFunctions/PatternMatching/SearchingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

SearchingRegex::SearchingRegex(const NES::Runtime::Execution::Expressions::ExpressionPtr& textValue,
                               const NES::Runtime::Execution::Expressions::ExpressionPtr& regexpPattern)
    : textValue(textValue), regexpPattern(regexpPattern) {}

/**
 * @brief This Method returns whether some sub-sequence in the target sequence (the subject) matches the regular expression rgx (the pattern).
 * This function is basically a wrapper for std::regex_search and enables us to use it in our execution engine framework.
 * @param text TextValue* the text (string) to extract the regexpPattern from
 * @param regex TextValue* the pattern to match
 * @return bool if sub-sequence in the text matches the pattern
 */
bool regex_search(TextValue* text, TextValue* reg) {

    std::string strText = std::string(text->str(), text->length());
    std::regex tempRegex(std::string(reg->str(), reg->length()));
    std::smatch result;

    return std::regex_search(strText, result, tempRegex);
}

Value<> SearchingRegex::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> text = textValue->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> pattern = regexpPattern->execute(record);

    if (text->isType<Text>() && pattern->isType<Text>()) {

        return FunctionCall<>("regex_search", regex_search, text.as<Text>()->getReference(), pattern.as<Text>()->getReference());
    } else {
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments of type Text.");
    }
}

}// namespace NES::Runtime::Execution::Expressions