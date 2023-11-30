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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_WRITEFIELDEXPRESSION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_WRITEFIELDEXPRESSION_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief This expression writes a particular Value to a specific field of an record.
 */
class WriteFieldExpression : public Expression {
  public:
    /**
     * @brief Creates a new WriteFieldExpression.
     * @param field identifier of a record field.
     * @param subExpression the input expression that generates a value that is written to the field.
     */
    WriteFieldExpression(Nautilus::Record::RecordFieldIdentifier field, const ExpressionPtr& subExpression);
    Value<> execute(Record& record) const override;

  private:
    const Nautilus::Record::RecordFieldIdentifier field;
    const ExpressionPtr subExpression;
};
}// namespace NES::Runtime::Execution::Expressions

#endif  // NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_WRITEFIELDEXPRESSION_HPP_
