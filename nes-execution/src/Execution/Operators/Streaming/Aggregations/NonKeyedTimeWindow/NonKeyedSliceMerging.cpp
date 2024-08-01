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
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/RecordBuffer.hpp>
#include <Util/StdInt.hpp>

namespace NES::Runtime::Execution::Operators {

void* createGlobalState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(op);
    auto sliceMergeTask = static_cast<SliceMergeTask<NonKeyedSlice>*>(sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
}

int8_t* getGlobalSliceState(void* gs) {
    auto globalSlice = static_cast<NonKeyedSlice*>(gs);
    return globalSlice->getState()->ptr;
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
int8_t* getDefaultMergingState(void* ss) {
    auto handler = static_cast<NonKeyedSliceMergingHandler*>(ss);
    return handler->getDefaultState()->ptr;
}

int8_t* getNonKeyedSliceState(void* smt, uint64_t index) {
    auto task = static_cast<SliceMergeTask<NonKeyedSlice>*>(smt);
    return task->slices[index].get()->getState()->ptr;
}

uint64_t getNonKeyedNumberOfSlices(void* smt) {
    auto task = static_cast<SliceMergeTask<NonKeyedSlice>*>(smt);
    return task->slices.size();
}

void freeNonKeyedSliceMergeTask(void* smt) {
    auto task = static_cast<SliceMergeTask<NonKeyedSlice>*>(smt);
    task->~SliceMergeTask();
}

NonKeyedSliceMerging::NonKeyedSliceMerging(
    uint64_t operatorHandlerIndex,
    const std::vector<std::shared_ptr<Aggregation::AggregationFunction>>& aggregationFunctions,
    std::unique_ptr<SliceMergingAction> sliceMergingAction)
    : operatorHandlerIndex(operatorHandlerIndex), aggregationFunctions(aggregationFunctions),
      sliceMergingAction(std::move(sliceMergingAction)) {}

void NonKeyedSliceMerging::setup(ExecutionContext& executionCtx) const {
    auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    UInt64 entrySize = 0_u64;
    for (auto& function : aggregationFunctions) {
        entrySize = entrySize + function->getSize();
    }
    nautilus::invoke(setupSliceMergingHandler,
                           globalOperatorHandler,
                           executionCtx.getPipelineContext(),
                           entrySize);
    auto defaultState = nautilus::invoke(getDefaultMergingState, globalOperatorHandler);
    for (auto& function : aggregationFunctions) {
        function->reset(defaultState);
        defaultState = defaultState + nautilus::val<uint64_t>(function->getSize());
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
    auto startSliceTs = getMemberAsExecDataType(sliceMergeTask, SliceMergeTask<NonKeyedSlice>, startSlice, uint64_t);
    auto endSliceTs = getMemberAsExecDataType(sliceMergeTask, SliceMergeTask<NonKeyedSlice>, endSlice, uint64_t);
    auto sequenceNumber = getMemberAsExecDataType(sliceMergeTask, SliceMergeTask<NonKeyedSlice>, sequenceNumber, uint64_t);
    auto chunkNumber = getMemberAsExecDataType(sliceMergeTask, SliceMergeTask<NonKeyedSlice>, chunkNumber, uint64_t);
    auto lastChunk = getMemberAsExecDataType(sliceMergeTask, SliceMergeTask<NonKeyedSlice>, lastChunk, bool);
    // 2. load the thread local slice store according to the worker id.
    auto combinedSlice = combineThreadLocalSlices(globalOperatorHandler, sliceMergeTask);
    nautilus::invoke(freeNonKeyedSliceMergeTask, sliceMergeTask);

    // 3. emit the combined slice via an action
    sliceMergingAction->emitSlice(ctx, child, startSliceTs, endSliceTs, sequenceNumber, chunkNumber, lastChunk, combinedSlice);
}

VoidRef NonKeyedSliceMerging::combineThreadLocalSlices(MemRef& globalOperatorHandler,
                                                             MemRef& sliceMergeTask) const {
    auto globalSlice = nautilus::invoke(createGlobalState, globalOperatorHandler, sliceMergeTask);
    auto globalSliceState = nautilus::invoke(getGlobalSliceState, globalSlice);
    auto numberOfSlices = nautilus::invoke(getNonKeyedNumberOfSlices, sliceMergeTask);
    for (UInt64 i = 0_u64; i < numberOfSlices; i = i + 1_u64) {
        auto srcSliceState = nautilus::invoke(getNonKeyedSliceState, sliceMergeTask, i);
        UInt64 stateOffset = 0;
        for (const auto& function : aggregationFunctions) {
            auto globalValuePtr = globalSliceState + stateOffset;
            auto partitionValuePtr = srcSliceState + stateOffset;
            function->combine(globalValuePtr, partitionValuePtr);
            stateOffset = stateOffset + function->getSize();
        }
    }
    return globalSlice;
}

}// namespace NES::Runtime::Execution::Operators
