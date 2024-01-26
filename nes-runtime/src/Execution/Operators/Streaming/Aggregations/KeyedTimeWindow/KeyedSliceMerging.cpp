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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/StdInt.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* createKeyedState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask<KeyedSlice>*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

void* getKeyedSliceState(void* gs) {
    auto globalSlice = static_cast<KeyedSlice*>(gs);
    return globalSlice->getState().get();
}

void* getKeyedSliceStateFromTask(void* smt, uint64_t index) {
    auto task = static_cast<SliceMergeTask<KeyedSlice>*>(smt);
    return task->slices[index].get()->getState().get();
}

uint64_t getKeyedNumberOfSlicesFromTask(void* smt) {
    auto task = static_cast<SliceMergeTask<KeyedSlice>*>(smt);
    return task->slices.size();
}

void freeKeyedSliceMergeTask(void* smt) {
    auto task = static_cast<SliceMergeTask<KeyedSlice>*>(smt);
    task->~SliceMergeTask();
}

void setupKeyedSliceMergingHandler(void* ss, void* ctx, uint64_t keySize, uint64_t valueSize) {
#ifndef UNIKERNEL_EXPORT
    auto handler = static_cast<KeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, keySize, valueSize);
#else
    NES_THROW_RUNTIME_ERROR("Not Implemented");
#endif
}

KeyedSliceMerging::KeyedSliceMerging(uint64_t operatorHandlerIndex,
                                     const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
                                     std::unique_ptr<SliceMergingAction> sliceMergingAction,
                                     const std::vector<PhysicalTypePtr>& keyDataTypes,
                                     const uint64_t keySize,
                                     const uint64_t valueSize)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      sliceMergingAction(std::move(sliceMergingAction)), keyDataTypes(keyDataTypes), keySize(keySize), valueSize(valueSize) {}

void KeyedSliceMerging::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Nautilus::FunctionCall("setupSliceMergingHandler",
                           setupKeyedSliceMergingHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           Value<UInt64>(keySize),
                           Value<UInt64>(valueSize));
    if (this->child != nullptr)
        this->child->setup(executionCtx);
}

void KeyedSliceMerging::open(ExecutionContext& ctx, RecordBuffer& buffer) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    if (this->child != nullptr)
        this->child->open(ctx, buffer);

    // 1. get the operator handler and extract the slice information that should be combined.
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    auto startSliceTs = getMember(sliceMergeTask, SliceMergeTask<KeyedSlice>, startSlice).load<UInt64>();
    auto endSliceTs = getMember(sliceMergeTask, SliceMergeTask<KeyedSlice>, endSlice).load<UInt64>();
    auto sequenceNumber = getMember(sliceMergeTask, SliceMergeTask<KeyedSlice>, sequenceNumber).load<UInt64>();

    // 2. initialize global slice state, which is represented by a chained hashtable
    auto globalSlice = Nautilus::FunctionCall("createKeyedState", createKeyedState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getKeyedSliceState", getKeyedSliceState, globalSlice);
    auto globalHashTable = Interface::ChainedHashMapRef(globalSliceState, keyDataTypes, keySize, valueSize);

    // 3. combine thread local slices and append them to the global slice store
    combineThreadLocalSlices(globalHashTable, sliceMergeTask);
    FunctionCall("freeKeyedSliceMergeTask", freeKeyedSliceMergeTask, sliceMergeTask);

    // 4. emit global slice when we have a tumbling window.
    sliceMergingAction->emitSlice(ctx, child, startSliceTs, endSliceTs, sequenceNumber, globalSlice);
}

void KeyedSliceMerging::combineThreadLocalSlices(Interface::ChainedHashMapRef& globalHashTable,
                                                 Value<MemRef>& sliceMergeTask) const {
    // combine all thread local partitions into the global slice hash map
    auto numberOfSlices =
        Nautilus::FunctionCall("getKeyedNumberOfSlicesFromTask", getKeyedNumberOfSlicesFromTask, sliceMergeTask);
    NES_DEBUG("combine {} slices", numberOfSlices->toString());

    for (Value<UInt64> i = 0_u64; i < numberOfSlices; i = i + 1_u64) {
        auto partitionState = Nautilus::FunctionCall("getKeyedSliceStateFromTask", getKeyedSliceStateFromTask, sliceMergeTask, i);
        auto partitionStateHashTable = Interface::ChainedHashMapRef(partitionState, keyDataTypes, keySize, valueSize);
        mergeHashTable(globalHashTable, partitionStateHashTable);
    }
}

void KeyedSliceMerging::mergeHashTable(Interface::ChainedHashMapRef& globalSliceHashMap,
                                       Interface::ChainedHashMapRef& threadLocalSliceHashMap) const {
    // inserts all entries from the thread local hash map into the global hash map.
    // 1. iterate over all entries in thread local hash map.
    for (const auto& threadLocalEntry : threadLocalSliceHashMap) {
        // 2. insert entry or update existing one with same key.
        globalSliceHashMap.insertEntryOrUpdate(threadLocalEntry, [&](auto& globalEntry) {
            // 2b. update aggregation if the entry was already existing in the global hash map
            auto key = threadLocalEntry.getKeyPtr();
            auto threadLocalValue = threadLocalEntry.getValuePtr();
            Value<MemRef> globalValue = globalEntry.getValuePtr();
            NES_TRACE("merge key {} th {} gb {}",
                      key.load<UInt64>()->toString(),
                      threadLocalValue.load<UInt64>()->toString(),
                      globalValue.load<UInt64>()->toString())
            // 2c. apply aggregation functions and combine the values
            for (const auto& function : aggregationFunctions) {
                function->combine(globalValue, threadLocalValue);
                threadLocalValue = threadLocalValue + function->getSize();
                NES_TRACE("result value {}", globalValue.load<UInt64>()->toString());
                globalValue = globalValue + function->getSize();
            }
        });
    }
}

}// namespace NES::Runtime::Execution::Operators