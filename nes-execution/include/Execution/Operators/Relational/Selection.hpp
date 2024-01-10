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
#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SELECTION_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SELECTION_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Selection operator that evaluates an boolean expression on each record.
 */
class Selection : public ExecutableOperator {
  public:
    /**
     * @brief Creates a selection operator with a expression.
     * @param expression boolean predicate expression
     */
    Selection(Runtime::Execution::Expressions::ExpressionPtr expression) : expression(expression){};
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr expression;
};

}// namespace NES::Runtime::Execution::Operators
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_SELECTION_HPP_
