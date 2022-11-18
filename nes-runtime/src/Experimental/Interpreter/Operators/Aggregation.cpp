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
#include <Experimental/Interpreter/ExecutionContext.hpp>
#include <Experimental/Interpreter/Operators/Aggregation.hpp>
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus {

class ThreadLocalAggregationState : public OperatorState {
  public:
    ThreadLocalAggregationState() {}
    std::vector<std::unique_ptr<AggregationState>> contexts;
};

Aggregation::Aggregation(std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions)
    : aggregationFunctions(aggregationFunctions) {}

void Aggregation::setup(RuntimeExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GlobalAggregationState>();
    auto state = aggregationFunctions[0]->createGlobalState();
    globalState->threadLocalAggregationSlots.emplace_back(std::move(state));
    tag = executionCtx.getPipelineContext().registerGlobalOperatorState(this, std::move(globalState));
    Operator::setup(executionCtx);
}

void Aggregation::open(RuntimeExecutionContext& executionCtx, RecordBuffer&) const {
    auto threadLocalState = std::make_unique<ThreadLocalAggregationState>();
    for (auto aggregationFunction : aggregationFunctions) {
        threadLocalState->contexts.push_back(aggregationFunction->createState());
    }
    executionCtx.setLocalOperatorState(this, std::move(threadLocalState));
}

void Aggregation::execute(RuntimeExecutionContext& executionCtx, Record& record) const {
    auto aggregationState = (ThreadLocalAggregationState*) executionCtx.getLocalState(this);
    for (uint64_t aggIndex = 0; aggIndex < aggregationFunctions.size(); aggIndex++) {
        aggregationFunctions[aggIndex]->liftCombine(aggregationState->contexts[aggIndex], record);
    }
}

extern "C" void* getThreadLocalAggregationStateProxy(void* globalAggregationState, uint64_t threadId) {
    auto gas = (GlobalAggregationState*) globalAggregationState;
    return gas->threadLocalAggregationSlots[threadId].get();
}

void Aggregation::close(RuntimeExecutionContext& executionCtx, RecordBuffer&) const {
    auto localAggregationState = (ThreadLocalAggregationState*) executionCtx.getLocalState(this);

    auto pipelineContext = executionCtx.getPipelineContext();
    auto globalOperatorState = pipelineContext.getGlobalOperatorState(this);
    auto threadLocalAggregationState = FunctionCall<>("getThreadLocalAggregationState",
                                                      getThreadLocalAggregationStateProxy,
                                                      globalOperatorState,
                                                      executionCtx.getWorkerContext().getWorkerId());

    for (uint64_t aggIndex = 0; aggIndex < aggregationFunctions.size(); aggIndex++) {
        auto function = aggregationFunctions[aggIndex];
        auto state = function->loadState(threadLocalAggregationState);
        function->combine(state, localAggregationState->contexts[aggIndex]);
        function->storeState(threadLocalAggregationState, state);
    }
}

}// namespace NES::Nautilus