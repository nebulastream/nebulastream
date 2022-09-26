#include <Experimental/Interpreter/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinProbe.hpp>
#include <Experimental/Interpreter/Record.hpp>
using namespace NES::Nautilus;
namespace NES::ExecutionEngine::Experimental::Interpreter {

JoinProbe::JoinProbe(std::shared_ptr<NES::Experimental::Hashmap> hashmap,
                     std::vector<ExpressionPtr> keyExpressions,
                     std::vector<ExpressionPtr> valueExpressions,
                     std::vector<IR::Types::StampPtr> hashmapKeyStamps,
                     std::vector<IR::Types::StampPtr> hashmapValueStamps)
    : hashMap(hashmap), keyExpressions(keyExpressions), valueExpressions(valueExpressions), keyTypes(hashmapKeyStamps),
      valueTypes(hashmapValueStamps) {}

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
        // todo load values from table values
        std::vector<Value<>> joinResults;

        // add keys to result
        for (auto& keys : keyValues) {
            joinResults.push_back(keys);
        }

        // add left results
        auto valuePtr = entry.getValuePtr();
        for (auto& valueType : valueTypes) {
            Value<> leftValue = valuePtr.load<Int64>();
            joinResults.push_back(leftValue);
            valuePtr = valuePtr + 8ul;
        }

        // add right values to the result
        for (auto& valueExpression : valueExpressions) {
            auto rightValue = valueExpression->execute(record);
            joinResults.push_back(rightValue);
        }
        auto joinResult = Record(joinResults);
        child->execute(ctx, joinResult);
    }
}

}// namespace NES::ExecutionEngine::Experimental::Interpreter