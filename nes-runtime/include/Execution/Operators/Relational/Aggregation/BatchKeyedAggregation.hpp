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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHKEYEDAGGREGATION_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHKEYEDAGGREGATION_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Batch operator for keyed aggregations.
 */
class BatchKeyedAggregation : public ExecutableOperator {
  public:
    /**
     * @brief Creates a keyed batch aggregation operator.
     * @param operatorHandlerIndex operator handler index.
     * @param keyExpressions expressions to derive the key values.
     * @param keyDataTypes types of the key values.
     * @param aggregationExpressions expressions to derive the aggregation values.
     * @param aggregationFunctions aggregation functions
     * @param hashFunction hash function
     */
    BatchKeyedAggregation(uint64_t operatorHandlerIndex,
                          const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                          const std::vector<PhysicalTypePtr>& keyDataTypes,
                          const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                          std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::vector<Expressions::ExpressionPtr> keyExpressions;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    uint64_t keySize;
    uint64_t valueSize;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_AGGREGATION_BATCHKEYEDAGGREGATION_HPP_
