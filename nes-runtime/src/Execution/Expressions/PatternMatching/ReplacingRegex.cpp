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
#include <Nautilus/Interface/DataTypes/Text/TextValue.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
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
 * @param text TextValue*
 * @param regex TextValue*
 * @param replacement TextValue*
 * @return TextValue*
 */
TextValue* regex_replace(TextValue* text, TextValue* reg, TextValue* replacement) {

    std::shared_ptr<char> p(text->str(), &free);
    std::string strText (p.get());

    std::shared_ptr<char> q(reg->str(), &free);
    std::string strRegex (q.get());
    std::regex tempRegex (strRegex);

    std::shared_ptr<char> r(replacement->str(), &free);
    std::string strReplacement (r.get());

    std::string strReplaced = std::regex_replace(strText, tempRegex, strReplacement);

    return TextValue::create(strReplaced);

}

Value<> ReplacingRegex::execute(NES::Nautilus::Record& record) const {

    // Evaluate the left sub expression and retrieve the value.
    Value<> leftValue = leftSubExpression->execute(record);

    // Evaluate the mid sub expression and retrieve the value.
    Value<> midValue = midSubExpression->execute(record);

    // Evaluate the right sub expression and retrieve the value.
    Value<> rightValue = rightSubExpression->execute(record);

    return FunctionCall<>("regex_replace", regex_replace, leftValue.as<Text>()->getReference(), midValue.as<Text>()->getReference(), rightValue.as<Text>()->getReference());

}

}// namespace NES::Runtime::Execution::Expressions