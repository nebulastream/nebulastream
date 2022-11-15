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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_THRESHOLDWINDOW_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_THRESHOLDWINDOW_HPP_
#include "Execution/Expressions/Expression.hpp"
#include "Execution/Operators/ExecutableOperator.hpp"
#include <utility>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief Threshold window operator that compute aggregation of tuples satisfying the threshold.
 */
class ThresholdWindow : public ExecutableOperator {
  public:
    /**
     * @brief Creates a selection operator with a expression.
     * @param expression boolean predicate expression
     */
    explicit ThresholdWindow(Runtime::Execution::Expressions::ExpressionPtr expression)
        : expression(std::move(expression)), sum(0){};

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr expression;
    mutable int32_t sum; // move to global state

};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_THRESHOLDWINDOW_HPP_
