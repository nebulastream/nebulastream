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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/ThresholdWindow/ThresholdWindow.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Util/Logger/Logger.hpp>
#include <Nautilus/Interface/DataTypes/Integer/Int.hpp>

namespace NES::Runtime::Execution::Operators {

extern "C" void addToSumAggregate(void* state, int64_t valueToAdd) {
    auto thresholdWindowAggregationState = (ThresholdWindowAggregationState*) state;
    thresholdWindowAggregationState->sum = thresholdWindowAggregationState->sum + valueToAdd;
}

extern "C" void setSumAggregate(void* state, int64_t valueToSet) {
    auto thresholdWindowAggregationState = (ThresholdWindowAggregationState*) state;
    thresholdWindowAggregationState->sum = valueToSet;
}

extern "C" int64_t getSumAggregate(void* state) {
    auto thresholdWindowAggregationState = (ThresholdWindowAggregationState*) state;
    auto sum = thresholdWindowAggregationState->sum.as<Nautilus::Int64>();
    return sum.getValue().getValue();
}

void ThresholdWindow::setup(ExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<ThresholdWindowAggregationState>((uint64_t) 0);
    executionCtx.setGlobalOperatorState(this, std::move(globalState));
    Operator::setup(executionCtx);
}

void NES::Runtime::Execution::Operators::ThresholdWindow::execute(ExecutionContext& ctx, Record& record) const {
    auto globalAggregationState = ctx.getGlobalState(this);

    // Evaluate the threshold condition
    auto val = predicateExpression->execute(record);
    if (val) {
        auto aggregatedValue = aggregatedFieldAccessExpression->execute(record);
        addToSumAggregate(globalAggregationState,  aggregatedValue.as<Int64>()->getRawInt());
    } else {
        auto sumAggregation = getSumAggregate(globalAggregationState);
        auto aggregationResult = Record({{"sum", sumAggregation}});
        child->execute(ctx, aggregationResult);
        setSumAggregate(globalAggregationState, 0);
    }
}

}// namespace NES::Runtime::Execution::Operators