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
 * @param text std::string
 * @param regex std::regex
 * @param replacement std::string
 * @return std::string
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

} // Ask: Alle 5 Parameter gewünscht?

Value<Text> ReplacingRegex::executeT(NES::Nautilus::Record& record) const {
    // Evaluate the left sub expression and retrieve the value.
    Value<Text> leftValue = leftSubExpression->executeT(record);
    //Value<Text> leftValue = Value(templeftValue);
    // Evaluate the left sub expression and retrieve the value.
    Value<Text> midValue = midSubExpression->executeT(record);
    //Value<Text> midValue = Value(tempmidValue);
    // Evaluate the right sub expression and retrieve the value.
    Value<Text> rightValue = rightSubExpression->executeT(record);
    //Value<Text> rightValue = Value(temprightValue);


    return FunctionCall<>("regex_replace", regex_replace, leftValue->getReference(), midValue->getReference(), rightValue->getReference());
    //return FunctionCall<>("regex_replace", regex_replace, leftValue, midValue, rightValue);

    // Ask: Fallunterscheidung notwendig? std::string oder TextValue
//    if (leftValue.value->isText() && midValue.value->isText() && rightValue.value->isText()) {
//        return FunctionCall<>("regex_replace", regex_replace, leftValue.ref , midValue.ref, rightValue.ref);
//    }
    // As we don't know the exact type of value here, we have to check the type and then call the function.
    // leftValue.as<Int8>() makes an explicit cast from Value to Value<Int8>.
    // In all cases we can call the same calculateMod function as under the hood C++ can do an implicit cast from
    // primitive integer types to the double argument.
    // Later we will introduce implicit casts on this level to hide this casting boilerplate code.

}

}// namespace NES::Runtime::Execution::Expressions