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
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>

namespace NES::Nautilus {

JoinProbe::JoinProbe(std::shared_ptr<NES::Experimental::Hashmap> hashmap,
                     std::vector<Record::RecordFieldIdentifier> resultFields,
                     std::vector<Runtime::Execution::Expressions::ExpressionPtr> keyExpressions,
                     std::vector<Runtime::Execution::Expressions::ExpressionPtr> valueExpressions,
                     std::vector<IR::Types::StampPtr> hashmapKeyStamps,
                     std::vector<IR::Types::StampPtr> hashmapValueStamps)
    : hashMap(hashmap), resultFields(resultFields), keyExpressions(keyExpressions), valueExpressions(valueExpressions),
      keyTypes(hashmapKeyStamps), valueTypes(hashmapValueStamps) {}

void JoinProbe::setup(RuntimeExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GlobalJoinState>();
    globalState->hashTable = this->hashMap;
    tag = executionCtx.getPipelineContext().registerGlobalOperatorState(this, std::move(globalState));
    Operator::setup(executionCtx);
}

void JoinProbe::execute(RuntimeExecutionContext& ctx, Record& record) const {
    auto globalAggregationState = ctx.getPipelineContext().getGlobalOperatorState(this);
    auto hashmapRef = FunctionCall("getJoinState", getJoinState, globalAggregationState);
    auto hashMapObject = HashMap(hashmapRef, 8ul, keyTypes, valueTypes);

    std::vector<Value<>> keyValues;
    for (auto& keyExpression : keyExpressions) {
        auto keyValue = keyExpression->execute(record);
        keyValues.push_back(keyValue);
    }

    auto entry = hashMapObject.findOne(keyValues);

    if (!entry.isNull()) {

        // create result record
        Record joinResult;

        // add keys to result
        for (auto& keyValue : keyValues) {
            auto fieldName = resultFields[joinResult.numberOfFields()];
            joinResult.write(fieldName, keyValue);
        }

        // add result from build table
        auto valuePtr = entry.getValuePtr();
        for (auto& valueType : valueTypes) {
            Value<> leftValue = valuePtr.load<Int64>();
            auto fieldName = resultFields[joinResult.numberOfFields()];
            joinResult.write(fieldName, leftValue);
            valuePtr = valuePtr + (uint64_t) 8;
        }

        // add right values to the result
        for (auto& valueExpression : valueExpressions) {
            auto rightValue = valueExpression->execute(record);
            auto fieldName = resultFields[joinResult.numberOfFields()];
            joinResult.write(fieldName, rightValue);
        }
        child->execute(ctx, joinResult);
    }
}

}// namespace NES::Nautilus