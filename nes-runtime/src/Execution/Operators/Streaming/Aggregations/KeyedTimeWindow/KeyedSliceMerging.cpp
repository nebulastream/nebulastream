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
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMerging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <utility>

namespace NES::Runtime::Execution::Operators {

void* createGlobalState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

void* getGlobalSliceState(void* gs) {
    auto globalSlice = static_cast<KeyedSlice*>(gs);
    return globalSlice->getState().get();
}

void* erasePartition(void* op, uint64_t ts) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(op);
    auto partition = handler->getSliceStaging().erasePartition(ts);
    // we give nautilus the ownership, thus deletePartition must be called.
    return partition.release();
}

uint64_t getSizeOfPartition(void* p) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    return partition->partialStates.size();
}

void* getPartitionState(void* p, uint64_t index) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    return partition->partialStates[index].get();
}

void deletePartition(void* p) {
    auto partition = static_cast<KeyedSliceStaging::Partition*>(p);
    delete partition;
}

void setupSliceMergingHandler(void* ss, void* ctx, uint64_t size) {
    auto handler = static_cast<KeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size,8);
}

KeyedSliceMerging::KeyedSliceMerging(uint64_t operatorHandlerIndex,
                                     const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions) {}

void KeyedSliceMerging::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    Value<UInt64> valueSize = (uint64_t) 0;
    for (auto& function : aggregationFunctions) {
        valueSize = valueSize + function->getSize();
    }
    Nautilus::FunctionCall("setupSliceMergingHandler",
                           setupSliceMergingHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           valueSize);
    this->child->setup(executionCtx);
}

void KeyedSliceMerging::open(ExecutionContext& ctx, RecordBuffer& buffer) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    this->child->open(ctx, buffer);
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    Value<> startSliceTs = (sliceMergeTask + (uint64_t) offsetof(SliceMergeTask, startSlice)).as<MemRef>().load<UInt64>();
    Value<> endSliceTs = (sliceMergeTask + (uint64_t) offsetof(SliceMergeTask, endSlice)).as<MemRef>().load<UInt64>();
    // 2. load the thread local slice store according to the worker id.
    auto globalSliceState = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask, endSliceTs);
    // emit global slice when we have a tumbling window.
    emitWindow(ctx, startSliceTs, endSliceTs, globalSliceState);
}

Value<MemRef> KeyedSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                          Value<MemRef>& sliceMergeTask,
                                                          Value<>& endSliceTs) const {
    auto globalSlice = Nautilus::FunctionCall("createGlobalState", createGlobalState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);
    auto partition = Nautilus::FunctionCall("erasePartition", erasePartition, globalOperatorHandler, endSliceTs.as<UInt64>());
    auto sizeOfPartitions = Nautilus::FunctionCall("getSizeOfPartition", getSizeOfPartition, partition);
    for (Value<UInt64> i = (uint64_t) 0; i < sizeOfPartitions; i = i + (uint64_t) 1) {
        auto partitionState = Nautilus::FunctionCall("getPartitionState", getPartitionState, partition, i);
        for (const auto& function : aggregationFunctions) {
            function->combine(globalSliceState, partitionState);
            partitionState = partitionState + function->getSize();
            globalSliceState = globalSliceState + function->getSize();
        }
    }
    Nautilus::FunctionCall("deletePartition", deletePartition, partition);
    return globalSlice;
}

void KeyedSliceMerging::emitWindow(ExecutionContext& ctx,
                                   Value<>& windowStart,
                                   Value<>& windowEnd,
                                   Value<MemRef>& globalSlice) const {
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);

    Record resultWindow;
    resultWindow.write("start_ts", windowStart);
    resultWindow.write("end_ts", windowEnd);
    for (const auto& function : aggregationFunctions) {
        auto finalAggregationValue = function->lower(globalSliceState);
        resultWindow.write("test$sum", finalAggregationValue);
        globalSliceState = globalSliceState + function->getSize();
    }
    child->execute(ctx, resultWindow);
}

}// namespace NES::Runtime::Execution::Operators