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
#include <Experimental/Interpreter/Operators/Join/JoinBuild.hpp>
#include <Experimental/Interpreter/Util/HashMap.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus {

extern "C" void* getJoinState(void* state) {
    auto joinState = (GlobalJoinState*) state;
    return joinState->hashTable.get();
}

JoinBuild::JoinBuild(std::shared_ptr<NES::Experimental::Hashmap> hashMap,
                     std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyExpressions,
                     std::vector<Runtime::Execution::Expressions::ExpressionPtr> valueExpressions)
    : hashMap(hashMap), keyExpressions(keyExpressions), valueExpressions(valueExpressions) {}

void JoinBuild::setup(RuntimeExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GlobalJoinState>();

    globalState->hashTable = this->hashMap;

    tag = executionCtx.getPipelineContext().registerGlobalOperatorState(this, std::move(globalState));
    Operator::setup(executionCtx);
}

void JoinBuild::open(RuntimeExecutionContext&, RecordBuffer&) const {}

void JoinBuild::execute(RuntimeExecutionContext& executionCtx, Record& record) const {
    auto globalAggregationState = executionCtx.getPipelineContext().getGlobalOperatorState(this);
    auto hashmapRef = FunctionCall("getJoinState", getJoinState, globalAggregationState);
    auto hashMapObject = HashMap(hashmapRef, 8ul, keyTypes, valueTypes);

    std::vector<Value<>> keyValues;
    for (auto& keyExpression : keyExpressions) {
        auto keyValue = keyExpression->execute(record);
        keyValues.push_back(keyValue);
    }
    auto hash = hashMapObject.calculateHash(keyValues);
    auto entry = hashMapObject.createEntry(keyValues, hash);

    auto valuePtr = entry.getValuePtr();
    for (auto& valueExpression : valueExpressions) {
        auto value = valueExpression->execute(record);
        valuePtr.store(value);
        valuePtr = valuePtr + (uint8_t) 8;
    }
}

void JoinBuild::close(RuntimeExecutionContext&, RecordBuffer&) const {}

}// namespace NES::Nautilus