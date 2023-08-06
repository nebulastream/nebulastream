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
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregation.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRef.hpp>
#include <Nautilus/Interface/DataTypes/Value.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* getKeyedBucketStore(void* op, uint64_t workerId) {
    auto handler = static_cast<KeyedBucketPreAggregationHandler*>(op);
    return handler->getThreadLocalBucketStore(workerId);
}

void* findKeyedBucketsByTs(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NonKeyedBucketStore*>(ss);
    return sliceStore->findBucketsByTs(ts);
}

uint64_t getKeyedBucketListSize(void* ptr) {
    auto buckets = static_cast<std::vector<KeyedSlice*>*>(ptr);
    return buckets->size();
}

void* getKeyedBucket(void* ptr, uint64_t index) {
    auto buckets = static_cast<std::vector<KeyedSlice*>*>(ptr);
    return buckets->operator[](index)->getState().get();
}

void triggerKeyedBucketsProxy(void* op,
                              void* wctx,
                              void* pctx,
                              uint64_t originId,
                              uint64_t sequenceNumber,
                              uint64_t watermarkTs) {
    auto handler = static_cast<KeyedBucketPreAggregationHandler*>(op);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->trigger(*workerContext, *pipelineExecutionContext, originId, sequenceNumber, watermarkTs);
}

void setupKeyedBucketWindowHandler(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
    auto handler = static_cast<KeyedBucketPreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
}

class LocalKeyedBucketStoreState : public Operators::OperatorState {
  public:
    explicit LocalKeyedBucketStoreState(const std::vector<PhysicalTypePtr>& keyDataTypes,
                                        uint64_t keySize,
                                        uint64_t valueSize,
                                        const Value<MemRef>& sliceStoreState)
        : keyDataTypes(keyDataTypes), keySize(keySize), valueSize(valueSize), sliceStoreState(sliceStoreState){};

    auto findSliceStateByTs(Value<UInt64>& timestampValue) {
        Value<MemRef> htPtr =
            Nautilus::FunctionCall("findKeyedBucketsByTs", findKeyedBucketsByTs, sliceStoreState, timestampValue);
        return Interface::ChainedHashMapRef(htPtr, keyDataTypes, keySize, valueSize);
    }
    const std::vector<PhysicalTypePtr> keyDataTypes;
    const uint64_t keySize;
    const uint64_t valueSize;
    const Value<MemRef> sliceStoreState;
};
KeyedBucketPreAggregation::KeyedBucketPreAggregation(
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

void KeyedBucketPreAggregation::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupKeyedBucketWindowHandler",
                           setupKeyedBucketWindowHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           Value<UInt64>(keySize),
                           Value<UInt64>(valueSize));
}

void KeyedBucketPreAggregation::open(ExecutionContext& ctx, RecordBuffer& rb) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    // 2. load the thread local slice store according to the worker id.
    auto bucketStore =
        Nautilus::FunctionCall("getKeyedBucketStore", getKeyedBucketStore, globalOperatorHandler, ctx.getWorkerId());
    // 3. store the reference to the slice store in the local operator state.
    auto state = std::make_unique<LocalKeyedBucketStoreState>(keyDataTypes, keySize, valueSize, bucketStore);
    ctx.setLocalOperatorState(this, std::move(state));
    // 4. initialize timestamp function
    timeFunction->open(ctx, rb);
}

void KeyedBucketPreAggregation::execute(NES::Runtime::Execution::ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    // For each input record, we derive its timestamp, we derive the correct slice from the slice store, and we manipulate the thread local aggregate.
    // 1. derive the current ts for the record.
    auto timestampValue = timeFunction->getTs(ctx, record);

    // 2. derive key values
    std::vector<Value<>> keyValues;
    for (const auto& exp : keyExpressions) {
        keyValues.emplace_back(exp->execute(record));
    }

    // 3. load the reference to the slice store and find the correct slice.
    auto state = reinterpret_cast<LocalKeyedBucketStoreState*>(ctx.getLocalState(this));

    auto buckets = Nautilus::FunctionCall("findKeyedBucketsByTs", findKeyedBucketsByTs, state->sliceStoreState, timestampValue);
    auto numberOfBuckets = Nautilus::FunctionCall("getKeyedBucketListSize", getKeyedBucketListSize, buckets);
    for (Value<UInt64> i = 0_u64; i < numberOfBuckets; i = i + 1_u64) {
        auto bucketState = Nautilus::FunctionCall("getKeyedBucket", getKeyedBucket, buckets, i);
        auto map = Interface::ChainedHashMapRef(bucketState, keyDataTypes, keySize, valueSize);
        // 4. calculate hash
        auto hash = hashFunction->calculate(keyValues);

        // 5. create entry in the slice hash map. If the entry is new set default values for aggregations.
        auto entry = map.findOrCreate(hash, keyValues, [this](auto& entry) {
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
}
void KeyedBucketPreAggregation::close(ExecutionContext& ctx, RecordBuffer&) const {
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);

    // After we processed all records in the record buffer we call triggerKeyedThreadLocalWindow
    // with the current watermark ts to check if we can trigger a window.
    Nautilus::FunctionCall("triggerKeyedBucketsProxy",
                           triggerKeyedBucketsProxy,
                           globalOperatorHandler,
                           ctx.getWorkerContext(),
                           ctx.getPipelineContext(),
                           ctx.getOriginId(),
                           ctx.getSequenceNumber(),
                           ctx.getWatermarkTs());
}

}// namespace NES::Runtime::Execution::Operators