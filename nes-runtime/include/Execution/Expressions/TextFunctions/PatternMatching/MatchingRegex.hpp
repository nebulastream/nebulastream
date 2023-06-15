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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_TEXTFUNCTIONS_PATTERNMATCHING_MATCHINGREGEX_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_TEXTFUNCTIONS_PATTERNMATCHING_MATCHINGREGEX_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Text/Text.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
* @brief This expression matches a given pattern in a given String and returns true in case of a full match. For partial matches please use SearchingRegex.
 * Set caseSensitive to true for case insensitive matching
* @param textValue as TextValue the text (string) to extract the regexpPattern from
* @param regexpPattern as TextValue the pattern to extract
* @param caseSensitive as Boolean indicates if matching should be done case sensitive
*/
class MatchingRegex : public Expression {
  public:
    MatchingRegex(const ExpressionPtr& textValue, const ExpressionPtr& regexpPattern, const ExpressionPtr& caseSensitive);
    Value<> execute(Record& record) const override;

  private:
    const ExpressionPtr textValue;
    const ExpressionPtr regexpPattern;
    const ExpressionPtr caseSensitive;
};
}// namespace NES::Runtime::Execution::Expressions

#endif// NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_TEXTFUNCTIONS_PATTERNMATCHING_MATCHINGREGEX_HPP_
