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
#include <Experimental/Interpreter/Operators/GroupedAggregation.hpp>
#include <Experimental/Interpreter/Util/HashMap.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/Experimental/MurMurHash3.hpp>

namespace NES::Nautilus {

extern "C" void* getHashMapState(void* state) {
    auto groupedAggregationState = (GroupedAggregationState*) state;
    return groupedAggregationState->threadLocalAggregationSlots[0].get();
}

GroupedAggregation::GroupedAggregation(NES::Experimental::HashMapFactory factory,
                                       std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyExpressions,
                                       std::vector<std::shared_ptr<AggregationFunction>> aggregationFunctions)
    : factory(factory), keyExpressions(keyExpressions), aggregationFunctions(aggregationFunctions) {}

void GroupedAggregation::setup(RuntimeExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GroupedAggregationState>();

    globalState->threadLocalAggregationSlots.emplace_back(this->factory.createPtr());

    tag = executionCtx.getPipelineContext().registerGlobalOperatorState(this, std::move(globalState));
    Operator::setup(executionCtx);
}

void GroupedAggregation::open(RuntimeExecutionContext&, RecordBuffer&) const {}

void GroupedAggregation::execute(RuntimeExecutionContext& executionCtx, Record& record) const {

    auto globalAggregationState = executionCtx.getPipelineContext().getGlobalOperatorState(this);
    auto hashmapRef = FunctionCall("getHashMapState", getHashMapState, globalAggregationState);
    auto hashMap = HashMap(hashmapRef, factory.getKeySize(), keyTypes, valueTypes);

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

void GroupedAggregation::close(RuntimeExecutionContext&, RecordBuffer&) const {}

}// namespace NES::Nautilus