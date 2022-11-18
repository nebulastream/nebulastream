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
#include <Experimental/Interpreter/Operators/Aggregation/AggregationFunction.hpp>
#include <Experimental/Interpreter/Operators/Streaming/WindowAggregation.hpp>
#include <Experimental/Interpreter/Util/HashMap.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/Experimental/MurMurHash3.hpp>

namespace NES::Nautilus {

extern "C" void* getSliceState(void* state, int64_t ts) {
    auto groupedAggregationState = (GroupedWindowAggregationState*) state;
    return &groupedAggregationState->sliceStore->findSliceByTs(ts)->getState();
}

WindowAggregation::WindowAggregation(std::shared_ptr<Windowing::Experimental::KeyedThreadLocalSliceStore> sliceStore,
                                     Runtime::Execution::Expressions::ExpressionPtr tsExpression,
                                     std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyExpressions,
                                     std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions)
    : sliceStore(sliceStore), tsExpression(tsExpression), keyExpressions(keyExpressions),
      aggregationFunctions(aggregationFunctions) {}

void WindowAggregation::setup(RuntimeExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GroupedWindowAggregationState>(sliceStore);
    tag = executionCtx.getPipelineContext().registerGlobalOperatorState(this, std::move(globalState));
    Operator::setup(executionCtx);
}

void WindowAggregation::open(RuntimeExecutionContext&, RecordBuffer&) const {
    // executionCtx.getPipelineContext().getGlobalOperatorState(this);
}

void WindowAggregation::execute(RuntimeExecutionContext& executionCtx, Record& record) const {

    auto globalAggregationState = executionCtx.getPipelineContext().getGlobalOperatorState(this);
    auto ts = tsExpression->execute(record);
    auto hashmapRef = FunctionCall("getSliceState", getSliceState, globalAggregationState, ts.as<Int64>());
    auto hashMap = HashMap(hashmapRef, 8ul, keyTypes, valueTypes);

    std::vector<Value<>> keyValues;
    for (auto& keyExpression : keyExpressions) {
        auto keyValue = keyExpression->execute(record);
        keyValues.push_back(keyValue);
    }

    auto entry = hashMap.findOrCreate(keyValues);
    auto valuePtr = entry.getValuePtr();
    for (auto& aggregationFunction : aggregationFunctions) {
        auto state = aggregationFunction->loadState(valuePtr);
        aggregationFunction->liftCombine(state, record);
        aggregationFunction->storeState(valuePtr, state);
        valuePtr = valuePtr + aggregationFunction->getStateSize();
    }
}

void WindowAggregation::close(RuntimeExecutionContext&, RecordBuffer&) const {
    /* auto localAggregationState = (ThreadLocalAggregationState*) executionCtx.getLocalState(this);

    auto pipelineContext = executionCtx.getPipelineContext();
    auto globalOperatorState = pipelineContext.getGlobalOperatorState(this);

    auto function = aggregationFunctions[0];
    auto state = function->loadState(threadLocalAggregationState);
    function->combine(state, localAggregationState->contexts[0]);
    function->storeState(threadLocalAggregationState, state);
    */
}

}// namespace NES::Nautilus