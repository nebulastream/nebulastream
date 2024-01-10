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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_READFIELDEXPRESSION_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_READFIELDEXPRESSION_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief This expression reads a specific field from the input record and returns its value.
 */
class ReadFieldExpression : public Expression {
  public:
    /**
     * @brief Creates a new ReadFieldExpression.
     * @param field the field name that is read from the record.
     */
    ReadFieldExpression(Nautilus::Record::RecordFieldIdentifier field);
    Value<> execute(Record& record) const override;

  private:
    const Nautilus::Record::RecordFieldIdentifier field;
};
using ReadFieldExpressionPtr = std::shared_ptr<Runtime::Execution::Expressions::ReadFieldExpression>;

}// namespace NES::Runtime::Execution::Expressions

#endif // NES_EXECUTION_INCLUDE_EXECUTION_EXPRESSIONS_READFIELDEXPRESSION_HPP_
