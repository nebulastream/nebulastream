//
// Created by pgrulich on 03.03.23.
//

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINBUILD_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINBUILD_HPP_

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
namespace NES::Runtime::Execution::Operators {

/**
 * @brief Batch Aggregation operator.
 */
class BatchJoinBuild : public ExecutableOperator {
  public:
    /**
     * @brief Creates a batch aggregation operator with a expression.
     */
    BatchJoinBuild(uint64_t operatorHandlerIndex,
                   const std::vector<Expressions::ExpressionPtr>& keyExpressions,
                   const std::vector<PhysicalTypePtr>& keyDataTypes,
                   const std::vector<Expressions::ExpressionPtr>& valueExpressions,
                   const std::vector<PhysicalTypePtr>& valueDataTypes,
                   std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction);
    void setup(ExecutionContext& executionCtx) const override;
    void open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const override;
    void execute(ExecutionContext& ctx, Record& record) const override;

  private:
    const uint64_t operatorHandlerIndex;
    const std::vector<Expressions::ExpressionPtr> keyExpressions;
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const std::vector<Expressions::ExpressionPtr> valueExpressions;
    const std::vector<PhysicalTypePtr> valueDataTypes;
    const std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction;
    uint64_t keySize;
    uint64_t valueSize;
};
}// namespace NES::Runtime::Execution::Operators
#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_RELATIONAL_JOIN_BATCHJOINBUILD_HPP_
