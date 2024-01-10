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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_TEXTFUNCTIONS_PATTERNMATCHING_EXTRACTINGREGEX_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_TEXTFUNCTIONS_PATTERNMATCHING_EXTRACTINGREGEX_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
* @brief This method returns a text sequence from the target text, which is matching the given regex pattern.
* If multiple text sequences, would match the given regex pattern the idx parameter can be used to select a match.
* @param textValue as TextValue the text (string) to extract the regexpPattern from
* @param regexpPattern as TextValue the pattern to extract
* @param idx as Integer defines at what position in the textValue matching starts (only the first match is returned), use 0 for the first
*/
class ExtractingRegex : public Expression {
  public:
    ExtractingRegex(const ExpressionPtr& textValue, const ExpressionPtr& regexpPattern, const ExpressionPtr& idx);
    Value<> execute(Record& record) const override;

  private:
    const ExpressionPtr textValue;
    const ExpressionPtr regexpPattern;
    const ExpressionPtr idx;
};

}// namespace NES::Runtime::Execution::Expressions

#endif // NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_TEXTFUNCTIONS_PATTERNMATCHING_EXTRACTINGREGEX_HPP_
