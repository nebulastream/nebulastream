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

#include <Execution/Expressions/TextFunctions/PatternMatching/MatchingRegex.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <regex>
#include <string>

namespace NES::Runtime::Execution::Expressions {

MatchingRegex::MatchingRegex(const NES::Runtime::Execution::Expressions::ExpressionPtr& textValue,
                             const NES::Runtime::Execution::Expressions::ExpressionPtr& regexpPattern,
                             const NES::Runtime::Execution::Expressions::ExpressionPtr& caseSensitive)
    : textValue(textValue), regexpPattern(regexpPattern), caseSensitive(caseSensitive) {}

/**
* @brief This method matches a given Regular Expression pattern in a given String. This Function only does full matches.
* @param txt TextValue the string
* @param regex TextValue the pattern to match
* @param caseSensitive Boolean to indicate case sensitive pattern matching
* @return Boolean if txt merely contains the pattern
*/
bool regexMatch(TextValue* txt, TextValue* regex, const Boolean& caseSensetive) {
    std::string target = std::string(txt->str(), txt->length());
    NES_DEBUG("Received the following source string {}", target);
    std::string strPattern = std::string(regex->str(), regex->length());
    NES_DEBUG("Received the following source string {}", strPattern);
    // LIKE and GLOB adoption requires syntax conversion functions
    // would make regex case in sensitive for ILIKE
    if (caseSensetive) {
        std::regex regexPattern(strPattern, std::regex::icase);
        return std::regex_match(target, regexPattern);
    } else {
        std::regex regexPattern(strPattern);
        return std::regex_match(target, regexPattern);
    }
}

Value<> MatchingRegex::execute(NES::Nautilus::Record& record) const {

    Value<> text = textValue->execute(record);
    Value<> pattern = regexpPattern->execute(record);
    Value<> caseSensitiveFlag = caseSensitive->execute(record);

    if (text->isType<Text>() && pattern->isType<Text>() && caseSensitiveFlag->isType<Boolean>()) {
        return FunctionCall<>("regexMatch",
                              regexMatch,
                              text.as<Text>()->getReference(),
                              pattern.as<Text>()->getReference(),
                              caseSensitiveFlag.as<Boolean>());
    } else {
        NES_DEBUG("{}", text->getType()->toString());
        std::stringstream patternType;
        patternType << pattern->getType();
        NES_DEBUG("{}", patternType.str());
        std::stringstream type;
        type << caseSensitiveFlag->getType();
        NES_DEBUG("{}", type.str());
        NES_THROW_RUNTIME_ERROR("This expression is only defined on input arguments that are Text and a Boolean for case "
                                "sensitive pattern matching.");
    }
}

}// namespace NES::Runtime::Execution::Expressions