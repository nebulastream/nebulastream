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

void appendToGlobalSliceStore(void* ss, void* slicePtr) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(ss);
    auto slice = std::unique_ptr<NonKeyedSlice>((NonKeyedSlice*) slicePtr);
    handler->appendToGlobalSliceStore(std::move(slice));
}

void triggerSlidingWindows(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(sh);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}

NonKeyedSliceMerging::NonKeyedSliceMerging(
    uint64_t operatorHandlerIndex,
    WindowType type,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    const std::string& startTsFieldName,
    const std::string& endTsFieldName,
    uint64_t resultOriginId)
    : operatorHandlerIndex(operatorHandlerIndex), type(type), aggregationFunctions(aggregationFunctions),
      startTsFieldName(startTsFieldName), endTsFieldName(endTsFieldName), resultOriginId(resultOriginId) {}

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
    Value<> startSliceTs = getMember(sliceMergeTask, SliceMergeTask, startSlice).load<UInt64>();
    Value<> endSliceTs = getMember(sliceMergeTask, SliceMergeTask, endSlice).load<UInt64>();
    Value<> sequenceNumber = getMember(sliceMergeTask, SliceMergeTask, sequenceNumber).load<UInt64>();
    // 2. load the thread local slice store according to the worker id.
    auto globalSliceState = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask, endSliceTs);

    if (type == TumblingWindow) {
        // emit global slice when we have a tumbling window.
        emitWindow(ctx, startSliceTs, endSliceTs, sequenceNumber, globalSliceState);
    } else if (type == SlidingWindow) {
        FunctionCall("appendToGlobalSliceStore", appendToGlobalSliceStore, globalOperatorHandler, globalSliceState);
        FunctionCall("triggerSlidingWindows",
                     triggerSlidingWindows,
                     globalOperatorHandler,
                     ctx.getWorkerContext(),
                     ctx.getPipelineContext(),
                     sequenceNumber.as<UInt64>(),
                     endSliceTs.as<UInt64>());
    }
}

Value<MemRef> NonKeyedSliceMerging::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                             Value<MemRef>& sliceMergeTask,
                                                             Value<>& endSliceTs) const {
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

void NonKeyedSliceMerging::emitWindow(ExecutionContext& ctx,
                                      Value<>& windowStart,
                                      Value<>& windowEnd,
                                      Value<>& sequenceNumber,
                                      Value<MemRef>& globalSlice) const {
    NES_DEBUG("Emit window: {}-{}-{}",
              windowStart.as<UInt64>()->toString(),
              windowEnd.as<UInt64>()->toString(),
              sequenceNumber.as<UInt64>()->toString());
    ctx.setWatermarkTs(windowEnd.as<UInt64>());
    ctx.setOrigin(resultOriginId);
    ctx.setSequenceNumber(sequenceNumber.as<UInt64>());
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);
    Record resultWindow;
    resultWindow.write(startTsFieldName, windowStart);
    resultWindow.write(endTsFieldName, windowEnd);
    uint64_t stateOffset = 0;
    for (const auto& function : aggregationFunctions) {
        auto valuePtr = globalSliceState + stateOffset;
        function->lower(valuePtr.as<MemRef>(), resultWindow);
        stateOffset = stateOffset + function->getSize();
    }
    child->execute(ctx, resultWindow);
    Nautilus::FunctionCall("deleteNonKeyedSlice", deleteNonKeyedSlice, globalSlice);
}

}// namespace NES::Runtime::Execution::Operators