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
#ifndef NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_AGGREGATION_HPP_
#define NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_AGGREGATION_HPP_
#include <Experimental/Interpreter/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>
#include <vector>

namespace NES::ExecutionEngine::Experimental::Interpreter {
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
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
  private:
    const std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions;
};

}// namespace NES::ExecutionEngine::Experimental::Interpreter
#endif//NES_NES_EXECUTION_INCLUDE_INTERPRETER_OPERATORS_AGGREGATION_HPP_
