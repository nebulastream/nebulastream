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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <utility>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief Threshold window operator that compute aggregation of tuples satisfying the threshold.
 */
class NonKeyedThresholdWindow : public ExecutableOperator {
  public:
    /**
     * @brief Creates a threshold window operator.
     * @param predicateExpression boolean predicate expression which check if a tuple satisfy the threshold
     * @param aggregationResultFieldIdentifier a string indicating the name of field to store the aggregation result
     * @param minCount minimum number of tuple in a window to trigger aggregation in the threshold window
     * @param aggregationFunctions vector of aggregation functiosn to apply to the threshold window
     * @param operatorHandlerIndex index of the handler of this operator in the pipeline execution context
     */
    NonKeyedThresholdWindow(Runtime::Execution::Expressions::ExpressionPtr predicateExpression,
                            const std::vector<Nautilus::Record::RecordFieldIdentifier>& aggregationResultFieldIdentifiers,
                            uint64_t minCount,
                            const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                            uint64_t operatorHandlerIndex);

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr predicateExpression;
    const std::vector<Expressions::ExpressionPtr> aggregatedFieldAccessExpressions;
    const std::vector<Nautilus::Record::RecordFieldIdentifier> aggregationResultFieldIdentifiers;
    uint64_t minCount = 0;
    uint64_t operatorHandlerIndex;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_THRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_NONKEYEDTHRESHOLDWINDOW_HPP_
