#include <Nautilus/Interface/FunctionCall.hpp>
#include <Experimental/Interpreter/Operators/Join/JoinBuild.hpp>
#include <Experimental/Interpreter/Util/HashMap.hpp>
using namespace NES::Nautilus;
namespace NES::Nautilus {

extern "C" void* getJoinState(void* state) {
    auto joinState = (GlobalJoinState*) state;
    return joinState->hashTable.get();
}

JoinBuild::JoinBuild(std::shared_ptr<NES::Experimental::Hashmap> hashMap,
                     std::vector<ExpressionPtr> keyExpressions,
                     std::vector<ExpressionPtr> valueExpressions)
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
        valuePtr = valuePtr + 8ul;
    }
}

void JoinBuild::close(RuntimeExecutionContext&, RecordBuffer&) const {}

}// namespace NES::Nautilus