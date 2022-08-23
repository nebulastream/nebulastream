#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>

namespace NES::ExecutionEngine::Experimental::Interpreter {

JoinProbe::JoinProbe(std::shared_ptr<NES::Experimental::Hashmap> hashmap, std::vector<ExpressionPtr> keyExpressions)
    : hashMap(hashmap), keyExpressions(keyExpressions) {}

void JoinProbe::setup(RuntimeExecutionContext& executionCtx) const {
    auto globalState = std::make_unique<GlobalJoinState>();
    globalState->hashTable = this->hashMap;
    executionCtx.getPipelineContext().registerGlobalOperatorState(this, std::move(globalState));
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
        // todo load values from table values
        if (child != nullptr)
            child->execute(ctx, record);
    }
}

void JoinProbe::open(RuntimeExecutionContext&, RecordBuffer&) const {}

void JoinProbe::close(RuntimeExecutionContext&, RecordBuffer&) const {}

}// namespace NES::ExecutionEngine::Experimental::Interpreter