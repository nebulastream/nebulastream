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
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlidingWindowSink.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Nautilus/Interface/DataTypes/MemRefUtils.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators {

void* createGlobalState(void* op, void* sliceMergeTaskPtr);
void* getGlobalSliceState(void* gs);
void deleteNonKeyedSlice(void* slice);
void setupSliceMergingHandler(void* ss, void* ctx, uint64_t size);
void* getDefaultMergingState(void* ss);

void* getSliceState(void* smt, uint64_t index) {
    auto window = static_cast<Window<NonKeyedSlice>*>(smt);
    return window->slices[index].get()->getState()->ptr;
}

uint64_t getNumberOfSlices(void* smt) {
    auto window = static_cast<Window<NonKeyedSlice>*>(smt);
    return window->slices.size();
}

void freeWindow(void* smt) {
    auto window = static_cast<Window<NonKeyedSlice>*>(smt);
    window->~Window();
}

NonKeyedSlidingWindowSink::NonKeyedSlidingWindowSink(
    uint64_t operatorHandlerIndex,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    const std::string& startTsFieldName,
    const std::string& endTsFieldName,
    uint64_t resultOriginId)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions), startTsFieldName(startTsFieldName),
      endTsFieldName(endTsFieldName), resultOriginId(resultOriginId) {}

void NonKeyedSlidingWindowSink::setup(ExecutionContext& executionCtx) const {
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
    this->child->setup(executionCtx);
}

void NonKeyedSlidingWindowSink::open(ExecutionContext& ctx, RecordBuffer& buffer) const {
    // Open is called once per pipeline invocation and enables us to initialize some local state, which exists inside pipeline invocation.
    // We use this here, to load the thread local slice store and store the pointer/memref to it in the execution context as the local slice store state.
    this->child->open(ctx, buffer);
    // 1. get the operator handler
    auto globalOperatorHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    auto sliceMergeTask = buffer.getBuffer();
    Value<> startSliceTs = getMember(sliceMergeTask, Window<NonKeyedSlice>, startTs).load<UInt64>();
    Value<> endSliceTs = getMember(sliceMergeTask, Window<NonKeyedSlice>, endTs).load<UInt64>();
    Value<> sequenceNumber = getMember(sliceMergeTask, Window<NonKeyedSlice>, sequenceNumber).load<UInt64>();
    // 2. load the thread local slice store according to the worker id.
    auto globalSlice = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask);
    Nautilus::FunctionCall("freeWindow", freeWindow, sliceMergeTask);
    emitWindow(ctx, startSliceTs, endSliceTs, sequenceNumber, globalSlice);
}

Value<MemRef> NonKeyedSlidingWindowSink::combineThreadLocalSlices(Value<MemRef>& globalOperatorHandler,
                                                                  Value<MemRef>& sliceMergeTask) const {
    auto globalSlice = Nautilus::FunctionCall("createGlobalState", createGlobalState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = Nautilus::FunctionCall("getGlobalSliceState", getGlobalSliceState, globalSlice);
    auto sizeOfPartitions = Nautilus::FunctionCall("getNumberOfSlices", getNumberOfSlices, sliceMergeTask);
    for (Value<UInt64> i = 0_u64; i < sizeOfPartitions; i = i + 1_u64) {
        auto sliceState = Nautilus::FunctionCall("getSliceState", getSliceState, sliceMergeTask, i);
        uint64_t stateOffset = 0;
        for (const auto& function : aggregationFunctions) {
            auto globalValuePtr = globalSliceState + stateOffset;
            auto partitionValuePtr = sliceState + stateOffset;
            function->combine(globalValuePtr.as<MemRef>(), partitionValuePtr.as<MemRef>());
            stateOffset = stateOffset + function->getSize();
        }
    }
    return globalSlice;
}

void NonKeyedSlidingWindowSink::emitWindow(ExecutionContext& ctx,
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
}

}// namespace NES::Runtime::Execution::Operators