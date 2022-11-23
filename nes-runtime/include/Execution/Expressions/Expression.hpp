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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_EXPRESSION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_EXPRESSION_HPP_
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <memory>

namespace NES::Nautilus {
class Record;
using RecordPtr = std::shared_ptr<Record>;
}// namespace NES::Nautilus

namespace NES::Runtime::Execution::Expressions {
using namespace Nautilus;
class Any;
class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;

/**
 * @brief Base class for all expressions.
 */
class Expression {
  public:
    /**
     * @brief Evaluates the expressions on a record.
     * @param record
     * @return Value<>
     */
    virtual Value<> execute(Record& record) const = 0;
    virtual ~Expression() = default;
};

}// namespace NES::Runtime::Execution::Expressions

#endif// NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_EXPRESSION_HPP_
