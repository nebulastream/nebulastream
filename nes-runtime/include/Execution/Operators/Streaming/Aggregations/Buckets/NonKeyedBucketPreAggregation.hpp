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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_NONKEYEDSLICEPREAGGREGATION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_NONKEYEDSLICEPREAGGREGATION_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
namespace NES::Runtime::Execution::Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;

/**
* @brief NonKeyedBucketPreAggregation operator that performs the pre-aggregation step for a global window aggregation.
*/
class NonKeyedBucketPreAggregation : public ExecutableOperator {
  public:
    /**
    * @brief Creates a NonKeyedBucketPreAggregation operator
    */
    NonKeyedBucketPreAggregation(uint64_t operatorHandlerIndex,
                                 TimeFunctionPtr timeFunction,
                                 const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const TimeFunctionPtr timeFunction;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_NONKEYEDSLICEPREAGGREGATION_HPP_
