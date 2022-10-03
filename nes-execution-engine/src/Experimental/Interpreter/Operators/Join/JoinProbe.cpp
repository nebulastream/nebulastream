#include <Nautilus/Interface/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>

using namespace NES::Nautilus;
namespace NES::Nautilus {

JoinProbe::JoinProbe(std::shared_ptr<NES::Experimental::Hashmap> hashmap,
                     std::vector<Record::RecordFieldIdentifier> resultFields,
                     std::vector<ExpressionPtr> keyExpressions,
                     std::vector<ExpressionPtr> valueExpressions,
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
            valuePtr = valuePtr + 8ul;
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