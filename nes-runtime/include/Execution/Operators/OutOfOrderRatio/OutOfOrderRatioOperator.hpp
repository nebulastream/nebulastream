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

#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OUTOFORDERRATIOOPERATOR_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OUTOFORDERRATIOOPERATOR_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

/**
* @brief Operator that measures the ratio of out-of-order-tuples in the stream.
*/
class OutOfOrderRatioOperator : public ExecutableOperator {
  public:
    /**
     * @brief Constructor for the out-of-order ratio operator.
     * @param timestampFieldAccessExpression field access to the tuple field that holds the timestamp
     * @param operatorHandlerIndex index of the handler of this operator in the pipeline execution context
     */
    OutOfOrderRatioOperator(Runtime::Execution::Expressions::ExpressionPtr timestampFieldAccessExpression,
                            uint64_t operatorHandlerIndex)
        : timestampFieldAccessExpression(std::move(timestampFieldAccessExpression)),
          operatorHandlerIndex(operatorHandlerIndex){};

    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const Runtime::Execution::Expressions::ExpressionPtr timestampFieldAccessExpression;
    uint64_t operatorHandlerIndex;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_OUTOFORDERRATIOOPERATOR_HPP_