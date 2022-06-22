#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {



std::unique_ptr<AggregationState> SumFunction::createState() { return std::make_unique<SumState>(); }

SumFunction::SumFunction(std::shared_ptr<ReadFieldExpression> readFieldExpression) : readFieldExpression(readFieldExpression) {}

void SumFunction::liftCombine(std::unique_ptr<AggregationState>& state, Record& record) {
    auto sumState = (SumState*) state.get();
    auto value = readFieldExpression->execute(record);
    sumState->currentSum = sumState->currentSum + value;
}

void SumFunction::combine(std::unique_ptr<AggregationState>& leftState, std::unique_ptr<AggregationState>& rightState) {
    auto leftSum = (SumState*) leftState.get();
    auto rightSum = (SumState*) rightState.get();
    leftSum->currentSum = leftSum->currentSum + rightSum->currentSum;
}

Value<Any> SumFunction::lower(std::unique_ptr<AggregationState>& state) {
    auto sumState = (SumState*) state.get();
    return sumState->currentSum;
}
std::unique_ptr<AggregationState> SumFunction::loadState(Value<MemRef>& ref) {
    auto value = load<Integer>(ref);
    return std::make_unique<SumState>(value);
}
void SumFunction::storeState(Value<MemRef>& ref, std::unique_ptr<AggregationState>& state) {
    auto sumState = (SumState*) state.get();
    store(ref, sumState->currentSum);
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter