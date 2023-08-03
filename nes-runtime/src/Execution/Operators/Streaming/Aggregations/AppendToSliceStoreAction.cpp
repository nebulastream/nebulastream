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
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

template<class Slice>
void appendToGlobalSliceStore(void* ss, void* slicePtr) {
    auto handler = static_cast<AppendToSliceStoreHandler<Slice>*>(ss);
    auto slice = std::unique_ptr<Slice>((Slice*) slicePtr);
    handler->appendToGlobalSliceStore(std::move(slice));
}
template<class Slice>
void triggerSlidingWindows(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<AppendToSliceStoreHandler<Slice>*>(sh);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}

template<class Slice>
AppendToSliceStoreHandler<Slice>::AppendToSliceStoreHandler(uint64_t windowSize, uint64_t windowSlide) {
    std::vector<OriginId> ids = {0};
    watermarkProcessor = std::make_unique<MultiOriginWatermarkProcessor>(ids);
    sliceStore = std::make_unique<SlidingWindowSliceStore<Slice>>(windowSize, windowSlide);
}

template<class Slice>
void AppendToSliceStoreHandler<Slice>::appendToGlobalSliceStore(std::unique_ptr<Slice> slice) {
    sliceStore->insertSlice(std::move(slice));
}

template<class Slice>
void AppendToSliceStoreHandler<Slice>::triggerSlidingWindows(Runtime::WorkerContext& wctx,
                                                             Runtime::Execution::PipelineExecutionContext& ctx,
                                                             uint64_t sequenceNumber,
                                                             uint64_t slideEnd) {

    NES_ASSERT(sliceStore != 0, "slice store is not initialized");
    // the watermark update is an atomic process and returns the last and the current watermark.
    auto currentWatermark = watermarkProcessor->updateWatermark(slideEnd, sequenceNumber, 0);

    // the watermark has changed get the lock to trigger
    std::lock_guard<std::mutex> lock(triggerMutex);
    // update currentWatermark, such that other threads to have to acquire the lock
    NES_TRACE("Trigger sliding windows between {}-{}", lastTriggerWatermark, currentWatermark);

    auto windows = sliceStore->collectWindows(lastTriggerWatermark, currentWatermark);
    // collect all slices that end <= watermark from all thread local slice stores.
    auto bufferProvider = wctx.getBufferProvider();
    for (const auto& [windowStart, windowEnd] : windows) {
        auto slicesForWindow = sliceStore->collectSlicesForWindow(windowStart, windowEnd);
        NES_TRACE("Deploy window ({}-{}) merge task for {} slices  ", windowStart, windowEnd, slicesForWindow.size());
        auto buffer = bufferProvider->getBufferBlocking();
        auto task = allocateWithin<SliceMergeTask<Slice>>(buffer);
        task->startSlice = windowStart;
        task->endSlice = windowEnd;
        task->slices = slicesForWindow;
        task->sequenceNumber = resultSequenceNumber++;
        ctx.dispatchBuffer(buffer);
    }
    // remove all slices from the slice store that are not necessary anymore.
    sliceStore->removeSlices(currentWatermark);
    lastTriggerWatermark = currentWatermark;
};

template<class Slice>
void AppendToSliceStoreHandler<Slice>::stop(NES::Runtime::QueryTerminationType queryTerminationType,
                                            NES::Runtime::Execution::PipelineExecutionContextPtr ctx) {
    NES_DEBUG("stop AppendToSliceStoreHandler: {}", queryTerminationType);
    if (queryTerminationType == QueryTerminationType::Graceful) {
        // the watermark has changed get the lock to trigger
        std::lock_guard<std::mutex> lock(triggerMutex);
        auto windows = sliceStore->collectAllWindows(lastTriggerWatermark);
        // collect all slices that end <= watermark from all thread local slice stores.
        auto bufferProvider = ctx->getBufferManager();
        for (const auto& [windowStart, windowEnd] : windows) {
            auto slicesForWindow = sliceStore->collectSlicesForWindow(windowStart, windowEnd);
            NES_TRACE("Deploy window ({}-{}) merge task for {} slices  ", windowStart, windowEnd, slicesForWindow.size());
            auto buffer = bufferProvider->getBufferBlocking();
            auto task = allocateWithin<SliceMergeTask<Slice>>(buffer);
            task->startSlice = windowStart;
            task->endSlice = windowEnd;
            task->slices = slicesForWindow;
            task->sequenceNumber = resultSequenceNumber++;
            ctx->dispatchBuffer(buffer);
        }
    }
}

template<class Slice>
AppendToSliceStoreAction<Slice>::AppendToSliceStoreAction(const uint64_t operatorHandlerIndex)
    : operatorHandlerIndex(operatorHandlerIndex) {}

template<class Slice>
void AppendToSliceStoreAction<Slice>::emitSlice(ExecutionContext& ctx,
                                                ExecuteOperatorPtr&,
                                                Value<UInt64>&,
                                                Value<UInt64>& sliceEnd,
                                                Value<UInt64>& sequenceNumber,
                                                Value<MemRef>& combinedSlice) const {

    auto actionHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    FunctionCall("appendToGlobalSliceStore", appendToGlobalSliceStore<Slice>, actionHandler, combinedSlice);
    FunctionCall("triggerSlidingWindows",
                 triggerSlidingWindows<Slice>,
                 actionHandler,
                 ctx.getWorkerContext(),
                 ctx.getPipelineContext(),
                 sequenceNumber,
                 sliceEnd);
}

// Instantiate types
template class AppendToSliceStoreHandler<NonKeyedSlice>;
template class AppendToSliceStoreHandler<KeyedSlice>;
template class AppendToSliceStoreAction<NonKeyedSlice>;
template class AppendToSliceStoreAction<KeyedSlice>;
}// namespace NES::Runtime::Execution::Operators