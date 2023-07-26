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
#ifndef NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_CONSTANTVALUEEXPRESSION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_CONSTANTVALUEEXPRESSION_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <type_traits>

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief This expression returns a specific constant values.
 */
template<typename T>
    requires std::is_integral_v<T> || std::is_floating_point_v<T>
class ConstantValueExpression : public Expression {
  public:
    explicit ConstantValueExpression(T value);
    Value<> execute(Record& record) const override;

  private:
    const T value;
};

using ConstantInt8ValueExpression = ConstantValueExpression<int8_t>;
using ConstantInt16ValueExpression = ConstantValueExpression<int16_t>;
using ConstantInt32ValueExpression = ConstantValueExpression<int32_t>;
using ConstantInt64ValueExpression = ConstantValueExpression<int64_t>;
using ConstantUInt8ValueExpression = ConstantValueExpression<uint8_t>;
using ConstantUInt16ValueExpression = ConstantValueExpression<uint16_t>;
using ConstantUInt32ValueExpression = ConstantValueExpression<uint32_t>;
using ConstantUInt64ValueExpression = ConstantValueExpression<uint64_t>;
using ConstantFloatValueExpression = ConstantValueExpression<float>;
using ConstantDoubleValueExpression = ConstantValueExpression<double>;
using ConstantBooleanValueExpression = ConstantValueExpression<bool>;

}// namespace NES::Runtime::Execution::Expressions

#endif// NES_RUNTIME_INCLUDE_EXECUTION_EXPRESSIONS_CONSTANTVALUEEXPRESSION_HPP_
