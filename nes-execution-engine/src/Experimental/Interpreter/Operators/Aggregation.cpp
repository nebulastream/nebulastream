#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/AggregationFunction.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

class GlobalAggregationState : public OperatorState {
  public:
    GlobalAggregationState() {}
    std::vector<Value<MemRef>> threadLocalAggregationSlots;
};

class ThreadLocalAggregationState : public OperatorState {
  public:
    ThreadLocalAggregationState() {}
    std::vector<std::unique_ptr<AggregationState>> contexts;
};

Aggregation::Aggregation(std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions)
    : aggregationFunctions(aggregationFunctions) {}

void Aggregation::setup(ExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GlobalAggregationState>();
    auto state = aggregationFunctions[0]->createState();
    auto address = std::addressof(*state.get());
    auto value = (int64_t) address;
    auto val = Value<MemRef>(std::make_unique<MemRef>(value));
    val.ref = Trace::ValueRef(INT32_MAX, 10, IR::Operations::Operation::INT8PTR);

    globalState->threadLocalAggregationSlots.push_back(val);
    executionCtx.setGlobalOperatorState(this, std::move(globalState));
}

void Aggregation::open(ExecutionContext& executionCtx, RecordBuffer&) const {
    auto threadLocalState = std::make_unique<ThreadLocalAggregationState>();
    for (auto aggregationFunction : aggregationFunctions) {
        threadLocalState->contexts.push_back(aggregationFunction->createState());
    }
    executionCtx.setLocalOperatorState(this, std::move(threadLocalState));
}

void Aggregation::execute(ExecutionContext& executionCtx, Record& record) const {
    auto aggregationState = (ThreadLocalAggregationState*) executionCtx.getLocalState(this);
    for (uint64_t aggIndex = 0; aggIndex < aggregationFunctions.size(); aggIndex++) {
        aggregationFunctions[aggIndex]->liftCombine(aggregationState->contexts[aggIndex], record);
    }
}

void Aggregation::close(ExecutionContext& executionCtx, RecordBuffer&) const {
    auto localAggregationState = (ThreadLocalAggregationState*) executionCtx.getLocalState(this);
    auto globalAggregationState = (GlobalAggregationState*) executionCtx.getGlobalState(this);
    for (uint64_t aggIndex = 0; aggIndex < aggregationFunctions.size(); aggIndex++) {
        auto function = aggregationFunctions[aggIndex];
        auto& stateRef = globalAggregationState->threadLocalAggregationSlots[0];
        auto state = function->loadState(stateRef);
        function->combine(state, localAggregationState->contexts[aggIndex]);
        function->storeState(stateRef, state);
    }
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter