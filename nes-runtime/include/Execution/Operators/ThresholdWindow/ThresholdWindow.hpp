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
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
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
    explicit ThresholdWindow(Runtime::Execution::Expressions::ExpressionPtr predicateExpression,
                             Runtime::Execution::Expressions::ExpressionPtr aggregatedFieldAccessExpression,
                             uint64_t operatorHandlerIndex)
        : predicateExpression(std::move(predicateExpression)),
          aggregatedFieldAccessExpression(std::move(aggregatedFieldAccessExpression)),
          operatorHandlerIndex(operatorHandlerIndex){};

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr predicateExpression;
    const Runtime::Execution::Expressions::ExpressionPtr aggregatedFieldAccessExpression;
    uint64_t operatorHandlerIndex;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_THRESHOLDWINDOW_HPP_
