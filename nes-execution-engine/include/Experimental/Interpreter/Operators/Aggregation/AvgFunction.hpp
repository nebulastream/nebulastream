#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_AVGFUNCTION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_AVGFUNCTION_HPP_

#include <Experimental/Interpreter/Expressions/Expression.hpp>
#include <Experimental/Interpreter/Expressions/ReadFieldExpression.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/ExecutableOperator.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class GlobalAvgState : public AggregationState {
  public:
    uint64_t sum;
    uint64_t count;
};

class AvgState : public AggregationState {
  public:
    AvgState(Value<> sum, Value<> count) : sum(sum), count(count) {}
    Value<> sum;
    Value<> count;
};

class AvgFunction : public AggregationFunction {
  public:
    AvgFunction(ExpressionPtr expression, IR::Types::StampPtr stamp);
    std::unique_ptr<AggregationState> createGlobalState() override;
    std::unique_ptr<AggregationState> createState() override;
    void liftCombine(std::unique_ptr<AggregationState>& ctx, Record& recordBuffer) override;
    void combine(std::unique_ptr<AggregationState>& ctx1, std::unique_ptr<AggregationState>& ctx2) override;
    Value<Any> lower(std::unique_ptr<AggregationState>& ctx) override;
    std::unique_ptr<AggregationState> loadState(Value<MemRef>& ref) override;
    void storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) override;
    uint64_t getStateSize() const override;

  private:
    ExpressionPtr expression;
    IR::Types::StampPtr stamp;
};
}// namespace NES::ExecutionEngine::Experimental::Interpreter
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_EXPERIMENTAL_INTERPRETER_OPERATORS_AGGREGATION_AVGFUNCTION_HPP_
