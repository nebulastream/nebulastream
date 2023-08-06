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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_KEYEDSLICEPREAGGREGATION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_KEYEDSLICEPREAGGREGATION_HPP_
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
namespace NES::Runtime::Execution::Operators {
class TimeFunction;
using TimeFunctionPtr = std::unique_ptr<TimeFunction>;

/**
* @brief NonKeyedBucketPreAggregation operator that performs the pre-aggregation step for a global window aggregation.
*/
class KeyedBucketPreAggregation : public ExecutableOperator {
  public:
    /**
    * @brief Creates a NonKeyedBucketPreAggregation operator
    */
    KeyedBucketPreAggregation(uint64_t operatorHandlerIndex,
                             TimeFunctionPtr timeFunction,
                             const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                             const std::vector<PhysicalTypePtr>& keyDataTypes,
                             const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                             const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const TimeFunctionPtr timeFunction;
    const std::vector<Expressions::ExpressionPtr> keyExpressions;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    uint64_t keySize;
    uint64_t valueSize;
};

}// namespace NES::Runtime::Execution::Operators

#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_KEYEDSLICEPREAGGREGATION_HPP_
