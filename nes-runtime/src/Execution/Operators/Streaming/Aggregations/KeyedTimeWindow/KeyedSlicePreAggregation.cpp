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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getSliceStoreProxy2(void* op, uint64_t workerId) {
    auto handler = static_cast<KeyedSlicePreAggregationHandler*>(op);
    return handler->getThreadLocalSliceStore(workerId);
}

void* findSliceStateByTsProxy2(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<KeyedThreadLocalSliceStore*>(ss);
    return sliceStore->findSliceByTs(ts)->getState().get();
}

void triggerThreadLocalStateProxy2(
    void* op,
                                  void* wctx,
                                  void* pctx,
                                  uint64_t workerId,
                                  uint64_t originId,
                                  uint64_t sequenceNumber,
                                  uint64_t watermarkTs) {
    auto handler = static_cast<KeyedSlicePreAggregationHandler*>(op);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->triggerThreadLocalState(*workerContext, *pipelineExecutionContext, workerId, originId, sequenceNumber, watermarkTs);
}

void setupWindowHandler2(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<KeyedSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size, 8);
}

class LocalSliceStoreState : public Operators::OperatorState {
  public:
    explicit LocalSliceStoreState(const Value<MemRef>& sliceStoreState) : sliceStoreState(sliceStoreState){};
    Nautilus::Interface::ChainedHashMapRef findSliceStateByTs(Value<UInt64>& timestampValue) {
        auto htPtr = Nautilus::FunctionCall("findSliceStateByTsProxy", findSliceStateByTsProxy2, sliceStoreState, timestampValue);
        return Interface::ChainedHashMapRef(htPtr, 8, 8);
    }
    const Value<MemRef> sliceStoreState;
};

KeyedSlicePreAggregation::KeyedSlicePreAggregation(
    uint64_t operatorHandlerIndex,
    Expressions::ExpressionPtr timestampExpression,
    const std::vector<Expressions::ExpressionPtr>& keyExpressions,
    const std::vector<Expressions::ExpressionPtr>& aggregationExpressions,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : operatorHandlerIndex(operatorHandlerIndex), timestampExpression(std::move(timestampExpression)),
      keyExpressions(keyExpressions), aggregationExpressions(aggregationExpressions), aggregationFunctions(aggregationFunctions), hashFunction(std::move(hashFunction)) {
    NES_ASSERT(aggregationFunctions.size() == aggregationExpressions.size(),
               "The number of aggregation expression and aggregation functions need to be equals");
}

void KeyedSlicePreAggregation::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> valueSize = (uint64_t) 0;
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }
    Nautilus::FunctionCall("setupWindowHandler",
                           setupWindowHandler2,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           valueSize);
}

void KeyedSlicePreAggregation::open(ExecutionContext& ctx, RecordBuffer&) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto sliceStore = Nautilus::FunctionCall("getSliceStoreProxy", getSliceStoreProxy2, globalOperatorHandler, ctx.getWorkerId());
    // 3. store the reference to the slice store in the local operator state.
    auto sliceStoreState = std::make_unique<LocalSliceStoreState>(sliceStore);
    ctx.setLocalOperatorState(this, std::move(sliceStoreState));
}

void KeyedSlicePreAggregation::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // For each input record, we derive its timestamp, we derive the correct slice from the slice store, and we manipulate the thread local aggregate.
    // 1. derive the current ts for the record.
    // Depending on the timestamp expression this is derived by a record field (event-time).
    auto timestampValue = timestampExpression->execute(record).as<UInt64>();

    // 2. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 3. load the reference to the slice store and find the correct slice.
    auto sliceStore = reinterpret_cast<LocalSliceStoreState*>(ctx.getLocalState(this));
    auto sliceState = sliceStore->findSliceStateByTs(timestampValue);

    // 4. calculate hash
    auto hash = hashFunction->calculate(keyValues);

    // 5. create entry in the slice hash map. If the entry is new set default values for aggregations.
    auto entry = sliceState.findOrCreate(hash, keyValues, [this](auto& entry) {
        // set aggregation values if a new entry was created
        auto valuePtr = entry.getValuePtr();
        for (const auto& aggFunction : aggregationFunctions) {
            aggFunction->reset(valuePtr);
            valuePtr = valuePtr + aggFunction->getSize();
        }
    });

    // 6. manipulate the current aggregate values
    auto valuePtr = entry.getValuePtr();
    for (size_t i = 0; i < aggregationFunctions.size(); ++i) {
        auto value = aggregationExpressions[i]->execute(record);
        aggregationFunctions[i]->lift(valuePtr, value);
        valuePtr = valuePtr + aggregationFunctions[i]->getSize();
    }
}
void KeyedSlicePreAggregation::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);

    // After we processed all records in the record buffer we call triggerThreadLocalStateProxy
    // with the current watermark ts to check if we can trigger a window.
    Nautilus::FunctionCall("triggerThreadLocalStateProxy",
                           triggerThreadLocalStateProxy2,
                           globalOperatorHandler,
                           executionCtx.getWorkerContext(),
                           executionCtx.getPipelineContext(),
                           executionCtx.getWorkerId(),
                           recordBuffer.getOriginId(),
                           recordBuffer.getSequenceNr(),
                           recordBuffer.getWatermarkTs());
}

}// namespace NES::Runtime::Execution::Operators