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

#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getKeyedSliceStoreProxy(void* op, WorkerThreadId workerThreadId) {
    auto handler = static_cast<KeyedSlicePreAggregationHandler*>(op);
    return handler->getThreadLocalSliceStore(workerThreadId);
}

void* findKeyedSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<KeyedThreadLocalSliceStore*>(ss);
    return sliceStore->findSliceByTs(ts)->getState().get();
}

void triggerKeyedThreadLocalWindow(void* op,
                                   void* wctx,
                                   void* pctx,
                                   uint64_t originId,
                                   uint64_t sequenceNumber,
                                   uint64_t chunkNumber,
                                   bool lastChunk,
                                   uint64_t watermarkTs) {
    auto handler = static_cast<KeyedSlicePreAggregationHandler*>(op);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->trigger(*workerContext,
                     *pipelineExecutionContext,
                     OriginId(originId),
                     {sequenceNumber, chunkNumber, lastChunk},
                     watermarkTs);
}

void setupWindowHandler2(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<KeyedSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

class LocalKeyedSliceStoreState : public Operators::OperatorState {
  public:
    explicit LocalKeyedSliceStoreState(const std::vector<PhysicalTypePtr>& keyDataTypes,
                                       uint64_t keySize,
                                       uint64_t valueSize,
                                       const Value<MemRef>& sliceStoreState)
        : keyDataTypes(keyDataTypes), keySize(keySize), valueSize(valueSize), sliceStoreState(sliceStoreState){};

    auto findSliceStateByTs(Value<UInt64>& timestampValue) {
        auto htPtr =
            Nautilus::FunctionCall("findKeyedSliceStateByTsProxy", findKeyedSliceStateByTsProxy, sliceStoreState, timestampValue);
        return Interface::ChainedHashMapRef(htPtr, keyDataTypes, keySize, valueSize);
    }
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const uint64_t keySize;
    const uint64_t valueSize;
    const Value<MemRef> sliceStoreState;
};

KeyedSlicePreAggregation::KeyedSlicePreAggregation(
    uint64_t operatorHandlerIndex,
    TimeFunctionPtr timeFunction,
    const std::vector<Expressions::ExpressionPtr>& keyExpressions,
    const std::vector<PhysicalTypePtr>& keyDataTypes,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    std::unique_ptr<Nautilus::Interface::HashFunction> hashFunction)
    : operatorHandlerIndex(operatorHandlerIndex), timeFunction(std::move(timeFunction)), keyExpressions(keyExpressions),
      keyDataTypes(keyDataTypes), aggregationFunctions(aggregationFunctions), hashFunction(std::move(hashFunction)), keySize(0),
      valueSize(0) {
    for (auto& keyType : keyDataTypes) {
        keySize = keySize + keyType->size();
    }
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }
}

void KeyedSlicePreAggregation::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupWindowHandler",
                           setupWindowHandler2,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           Value<UInt64>(keySize),
                           Value<UInt64>(valueSize));
}

void KeyedSlicePreAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto sliceStore = Nautilus::FunctionCall("getKeyedSliceStoreProxy",
                                             getKeyedSliceStoreProxy,
                                             globalOperatorHandler,
                                             ctx.getWorkerThreadId());
    // 3. store the reference to the slice store in the local operator state.
    auto sliceStoreState = std::make_unique<LocalKeyedSliceStoreState>(keyDataTypes, keySize, valueSize, sliceStore);
    ctx.setLocalOperatorState(this, std::move(sliceStoreState));
    // 4. initialize timestamp function
    timeFunction->open(ctx, rb);
}

void KeyedSlicePreAggregation::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // For each input record, we derive its timestamp, we derive the correct slice from the slice store, and we manipulate the thread local aggregate.
    // 1. derive the current ts for the record.
    auto timestampValue = timeFunction->getTs(ctx, record);

    // 2. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 3. load the reference to the slice store and find the correct slice.
    auto sliceStore = reinterpret_cast<LocalKeyedSliceStoreState*>(ctx.getLocalState(this));
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
    for (const auto& aggregationFunction : aggregationFunctions) {
        aggregationFunction->lift(valuePtr, record);
        valuePtr = valuePtr + aggregationFunction->getSize();
    }
}


void KeyedSlicePreAggregation::close(ExecutionContext& ctx, RecordBuffer&) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // After we processed all records in the record buffer we call triggerKeyedThreadLocalWindow
    // with the current watermark ts to check if we can trigger a window.
    Nautilus::FunctionCall("triggerKeyedThreadLocalWindow",
                           triggerKeyedThreadLocalWindow,
                           globalOperatorHandler,
                           ctx.getWorkerContext(),
                           ctx.getPipelineContext(),
                           ctx.getOriginId(),
                           ctx.getSequenceNumber(),
                           ctx.getChunkNumber(),
                           ctx.getLastChunk(),
                           ctx.getWatermarkTs());
}

}// namespace NES::Runtime::Execution::Operators
