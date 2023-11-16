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
#include "fmt.hpp"
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Runtime/Execution/UnikernelPipelineExecutionContext.hpp>

namespace fmt {
template<>
struct formatter<std::source_location> : formatter<std::string> {
    auto format(const std::source_location& loc, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "{}:{} {}", loc.file_name(), loc.line(), loc.function_name());
    }
};
}// namespace fmt

extern "C" void* getSliceStoreProxy(void* op, uint64_t workerId) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(op);
    NES_DEBUG("getSliceStoreProxy");
    return handler->getThreadLocalSliceStore(workerId);
}

extern "C" void* findSliceStateByTsProxy(void* ss, uint64_t ts) {
    auto sliceStore = static_cast<NES::Runtime::Execution::Operators::NonKeyedThreadLocalSliceStore*>(ss);
    NES_DEBUG("findSliceStateByTsProxy ts: {}", ts);
    auto slice = sliceStore->findSliceByTs(ts).get();
    NES_DEBUG("Slice: {}-{}", slice->getStart(), slice->getEnd());
    auto state = slice->getState().get();
    NES_DEBUG("State: {}", *state);
    return state->ptr;
}
extern "C" void* getGlobalOperatorHandlerProxy(void* pc, uint64_t index) {
    NES_DEBUG("PC: {} index: {}", pc, index);
    auto pipelineCtx = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(pc);
    NES_DEBUG("getGlobalOperatorHandlerProxy");

    NES_DEBUG("PC: {}", (void*) pipelineCtx);
    return pipelineCtx->getOperatorHandler(index);
}

extern "C" void triggerThreadLocalStateProxy(void* op,
                                             void* wctx,
                                             void* pctx,
                                             uint64_t,
                                             uint64_t originId,
                                             uint64_t sequenceNumber,
                                             uint64_t watermarkTs) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(op);
    auto workerContext = static_cast<NES::Runtime::WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<NES::Runtime::Execution::PipelineExecutionContext*>(pctx);
    handler->trigger(*workerContext, *pipelineExecutionContext, originId, sequenceNumber, watermarkTs);
}

extern "C" void* createGlobalState(void* op, void* sliceMergeTaskPtr) {
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler*>(op);
    auto sliceMergeTask =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(
            sliceMergeTaskPtr);
    auto globalState = handler->createGlobalSlice(sliceMergeTask);
    NES_DEBUG("createGlobalState({}, {}, {})", *handler, *sliceMergeTask, *globalState);
    // we give nautilus the ownership, thus deletePartition must be called.
    return globalState.release();
};

extern "C" void setupSliceMergingHandler(void* ss, void* ctx, uint64_t size) {
    NES_DEBUG("setupSliceMergingHandler()");
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    handler->setup(*pipelineExecutionContext, size);
}
extern "C" void* getDefaultMergingState(void* ss) {
    NES_DEBUG("getDefaultMergingState()");
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSliceMergingHandler*>(ss);
    return handler->getDefaultState()->ptr;
}
extern "C" void setupWindowHandler(void* ss, void* ctx, uint64_t size) {
    NES_DEBUG("setupWindowHandler()");
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(ss);
    auto pipelineExecutionContext = static_cast<NES::Unikernel::UnikernelPipelineExecutionContext*>(ctx);
    NES_DEBUG("setupWindowHandler()");
    handler->setup(*pipelineExecutionContext, size);
}
extern "C" void* getDefaultState(void* ss) {
    NES_DEBUG("getDefaultState()");
    auto handler = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler*>(ss);
    return handler->getDefaultState()->ptr;
}

extern "C" void* getGlobalSliceState(void* gs) {
    NES_DEBUG("getGlobalSliceState()");
    auto globalSlice = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlice*>(gs);
    NES_DEBUG("getGlobalSliceState({})", *globalSlice);
    NES_DEBUG("globalSliceState Size: {}",
              static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(
                  globalSlice->getState()->ptr)
                  ->slices.size());
    return globalSlice->getState()->ptr;
};

extern "C" uint64_t getNonKeyedNumberOfSlices(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(smt);
    NES_DEBUG("getNonKeyedNumberOfSlices({})", *task);
    return task->slices.size();
};
extern "C" void* getNonKeyedSliceState(void* smt, uint64_t index) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(smt);
    NES_DEBUG("getNonKeyedSliceState({})", *task);
    return task->slices[index].get()->getState()->ptr;
}
extern "C" void freeNonKeyedSliceMergeTask(void* smt) {
    auto task =
        static_cast<NES::Runtime::Execution::Operators::SliceMergeTask<NES::Runtime::Execution::Operators::NonKeyedSlice>*>(smt);
    NES_DEBUG("freeNonKeyedSliceMergeTask({})", *task);
    task->~SliceMergeTask();
}
extern "C" void deleteNonKeyedSlice(void* slice) {
    auto deleteNonKeyedSlice = static_cast<NES::Runtime::Execution::Operators::NonKeyedSlice*>(slice);
    NES_DEBUG("deleteNonKeyedSlice({})", *deleteNonKeyedSlice);
    delete deleteNonKeyedSlice;
}