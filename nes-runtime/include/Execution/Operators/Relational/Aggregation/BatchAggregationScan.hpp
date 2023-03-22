//
// Created by pgrulich on 24.02.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHAGGREGATION_SCAN_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHAGGREGATION_SCAN_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Batch Aggregation operator.
 */
class BatchAggregationScan : public Operator {
  public:
    /**
     * @brief Creates a batch aggregation operator with a expression.
     */
    BatchAggregationScan(uint64_t operatorHandlerIndex,
                         const std::vector<std::shared_ptr<Execution::Aggregation::AggregationFunction>>& aggregationFunctions,
                         const std::vector<std::string>& aggregationResultFields);
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    const std::vector<std::string> aggregationResultFields;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHAGGREGATION_HPP_
