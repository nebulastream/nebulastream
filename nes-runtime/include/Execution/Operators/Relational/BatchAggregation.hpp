//
// Created by pgrulich on 24.02.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHAGGREGATION_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHAGGREGATION_HPP_
#include <Execution/Aggregation/AggregationFunction.hpp>
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>

namespace NES::Runtime::Execution::Operators {

/**
 * @brief Batch Aggregation operator.
 */
class BatchAggregation : public ExecutableOperator {
  public:
    /**
     * @brief Creates a batch aggregation operator with a expression.
     */
    BatchAggregation(uint64_t operatorHandlerIndex,
                     const std::vector<Expressions::ExpressionPtr>& aggregationExpressions,
                     const std::vector<std::shared_ptr<Execution::Aggregation::AggregationFunction>>& aggregationFunctions,
                     const std::vector<std::string>& aggregationResultFields);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;
    void close(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void terminate(ExecutionContext& executionCtx) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::vector<Expressions::ExpressionPtr> aggregationExpressions;
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions;
    [[maybe_unused]] const std::vector<std::string>& aggregationResultFields;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_BATCHAGGREGATION_HPP_
