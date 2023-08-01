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
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators {

void* createGlobalState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

void* getGlobalSliceState(void* gs) {
    auto globalSlice = static_cast<NonKeyedSlice*>(gs);
    return globalSlice->getState()->ptr;
}

void* erasePartition(void* op, uint64_t ts) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(op);
    auto partition = handler->getSliceStaging().erasePartition(ts);
    // we give nautilus the ownership, thus deletePartition must be called.
    return partition.release();
}

uint64_t getSizeOfPartition(void* p) {
    auto partition = static_cast<NonKeyedSliceStaging::Partition*>(p);
    return partition->partialStates.size();
}

void* getPartitionState(void* p, uint64_t index) {
    auto partition = static_cast<NonKeyedSliceStaging::Partition*>(p);
    return partition->partialStates[index].get()->ptr;
}

void deletePartition(void* p) {
    auto partition = static_cast<NonKeyedSliceStaging::Partition*>(p);
    delete partition;
}

void deleteNonKeyedSlice(void* slice) {
    auto deleteNonKeyedSlice = static_cast<NonKeyedSlice*>(slice);
    delete deleteNonKeyedSlice;
}

void setupSliceMergingHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size);
}
void* getDefaultMergingState(void* ss) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(ss);
    return handler->getDefaultState()->ptr;
}


NonKeyedSliceMerging::NonKeyedSliceMerging(
    uint64_t operatorHandlerIndex,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    std::unique_ptr<SliceMergingAction> sliceMergingAction)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      sliceMergingAction(std::move(sliceMergingAction)) {}

void NonKeyedSliceMerging::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> entrySize = 0_u64;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    Nautilus::FunctionCall("setupSliceMergingHandler",
                           setupSliceMergingHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           entrySize);
    auto defaultState = Nautilus::FunctionCall("getDefaultMergingState", getDefaultMergingState, globalOperatorHandler);
    for (auto& function : aggregationFunctions) {
        function->reset(defaultState);
        defaultState = defaultState + function->getSize();
    }
    if (this->child != nullptr)
        this->child->setup(executionCtx);
}

void NonKeyedSliceMerging::open(ExecutionContext& ctx, RecordBuffer& buffer) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    if (this->child != nullptr)
        this->child->open(ctx, buffer);
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    auto startSliceTs = getMember(sliceMergeTask, SliceMergeTask, startSlice).load<UInt64>();
    auto endSliceTs = getMember(sliceMergeTask, SliceMergeTask, endSlice).load<UInt64>();
    auto sequenceNumber = getMember(sliceMergeTask, SliceMergeTask, sequenceNumber).load<UInt64>();
    // 2. load the thread local slice store according to the worker id.
    auto combinedSlice = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask, endSliceTs);

    // 3. emit the combined slice via an action
    sliceMergingAction->emitSlice(ctx, child, startSliceTs, endSliceTs, sequenceNumber, combinedSlice);
}

Value<MemRef> NonKeyedSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                             Value<MemRef>& sliceMergeTask,
                                                             Value<UInt64>& endSliceTs) const {
    auto globalSlice = Nautilus::FunctionCall("createGlobalState", createGlobalState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);
    auto partition = Nautilus::FunctionCall("erasePartition", erasePartition, globalOperatorHandler, endSliceTs.as<UInt64>());
    auto sizeOfPartitions = Nautilus::FunctionCall("getSizeOfPartition", getSizeOfPartition, partition);
    for (Value<UInt64> i = 0_u64; i < sizeOfPartitions; i = i + 1_u64) {
        auto partitionState = Nautilus::FunctionCall("getPartitionState", getPartitionState, partition, i);
        uint64_t stateOffset = 0;
        for (const auto& function : aggregationFunctions) {
            auto globalValuePtr = globalSliceState + stateOffset;
            auto partitionValuePtr = partitionState + stateOffset;
            function->combine(globalValuePtr.as<MemRef>(), partitionValuePtr.as<MemRef>());
            stateOffset = stateOffset + function->getSize();
        }
    }
    Nautilus::FunctionCall("deletePartition", deletePartition, partition);
    return globalSlice;
}

}// namespace NES::Runtime::Execution::Operators