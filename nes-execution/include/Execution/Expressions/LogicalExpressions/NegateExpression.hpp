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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_LOGICALEXPRESSIONS_NEGATEEXPRESSION_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_LOGICALEXPRESSIONS_NEGATEEXPRESSION_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
namespace NES::Runtime::Execution::Expressions {

/**
 * @brief This expressions returns a Boolean values that is the negation of subExpression.
 */
class NegateExpression : public Expression {
  public:
    NegateExpression(ExpressionPtr subExpression);
    Value<> execute(Record& record) const override;

  private:
    const ExpressionPtr subExpression;
};
}// namespace NES::Runtime::Execution::Expressions

#endif // NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_LOGICALEXPRESSIONS_NEGATEEXPRESSION_HPP_
