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
#include <Execution/Aggregation/AggregationFunction.hpp>
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
     * @brief Creates a threshold window operator.
     * @param predicateExpression boolean predicate expression which check if a tuple satisfy the threshold
     * @param aggregatedFieldAccessExpression field access to the field that is aggregated
     * @param aggregationResultFieldIdentifier a string indicating the name of field to store the aggregation result
     * @param operatorHandlerIndex index of the handler of this operator in the pipeline execution context
     */
    ThresholdWindow(Runtime::Execution::Expressions::ExpressionPtr predicateExpression,
                    uint64_t minCount,
                    Runtime::Execution::Expressions::ExpressionPtr aggregatedFieldAccessExpression,
                    Nautilus::Record::RecordFieldIdentifier aggregationResultFieldIdentifier,
                    Execution::Aggregation::AggregationFunctionPtr aggregationFunction,
                    uint64_t operatorHandlerIndex)
        : predicateExpression(std::move(predicateExpression)),
          aggregatedFieldAccessExpression(std::move(aggregatedFieldAccessExpression)),
          aggregationResultFieldIdentifier(std::move(aggregationResultFieldIdentifier)), minCount(minCount),
          operatorHandlerIndex(operatorHandlerIndex), aggregationFunction(std::move(aggregationFunction)){};

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr predicateExpression;
    const Runtime::Execution::Expressions::ExpressionPtr aggregatedFieldAccessExpression;
    const Nautilus::Record::RecordFieldIdentifier aggregationResultFieldIdentifier;
    uint64_t minCount = 0;
    uint64_t operatorHandlerIndex;
    const std::shared_ptr<Aggregation::AggregationFunction> aggregationFunction;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_THRESHOLDWINDOW_HPP_
