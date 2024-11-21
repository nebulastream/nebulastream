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

#ifndef NES_PLUGINS_meos_INCLUDE_EXECUTION_OPERATORS_meos_meos_HPP_
#define NES_PLUGINS_meos_INCLUDE_EXECUTION_OPERATORS_meos_meos_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>

namespace NES::Runtime::Execution::Expressions {

/**
 * @brief this is the nautilus implementation of infer model operator. This operator allows for inferring (currently only tensorflow) machine learning model over incoming data stream.
 */
class MeosExpression : public Expression {

  public:
    MeosExpression(const ExpressionPtr& left, const ExpressionPtr& middle, const ExpressionPtr& right);
    Value<> execute(Record& record) const override;

  private:
    const ExpressionPtr left;
    const ExpressionPtr middle;
    const ExpressionPtr right;
};

}// namespace NES::Runtime::Execution::Expressions
#endif// NES_PLUGINS_meos_INCLUDE_EXECUTION_OPERATORS_meos_meos_HPP_
