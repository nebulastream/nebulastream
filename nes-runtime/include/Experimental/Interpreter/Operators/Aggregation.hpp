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
#ifndef NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_HPP_
#define NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_HPP_
#include <Execution/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <vector>

namespace NES::Nautilus {
class AggregationFunction;
class AggregationState;

class GlobalAggregationState : public OperatorState {
  public:
    GlobalAggregationState() {}
    std::vector<std::unique_ptr<AggregationState>> threadLocalAggregationSlots;
};

class Aggregation : public ExecutableOperator {
  public:
    Aggregation(std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions);
    void setup(RuntimeExecutionContext& executionCtx) const override;
    void open(RuntimeExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(RuntimeExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(RuntimeExecutionContext& ctx, Record& record) const override;

  private:
    const std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions;
    mutable uint32_t tag;
};

}// namespace NES::Nautilus
#endif// NES_RUNTIME_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_HPP_
