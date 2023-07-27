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

#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceMergingHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>

namespace NES::Runtime::Execution::Operators {

NonKeyedSliceMergingHandler::NonKeyedSliceMergingHandler(std::shared_ptr<NonKeyedSliceStaging> globalSliceStaging,
                                                         std::unique_ptr<SlidingWindowSliceStore<NonKeyedSlice>> globalSliceStore)
    : sliceStaging(globalSliceStaging), globalSliceStore(std::move(globalSliceStore)) {
    std::vector<OriginId> ids = {0};
    watermarkProcessor = std::make_unique<MultiOriginWatermarkProcessor>(ids);
}

NonKeyedSliceMergingHandler::NonKeyedSliceMergingHandler(std::shared_ptr<NonKeyedSliceStaging> globalSliceStaging)
    : sliceStaging(globalSliceStaging), globalSliceStore(nullptr) {}

void NonKeyedSliceMergingHandler::setup(Runtime::Execution::PipelineExecutionContext&, uint64_t entrySize) {
    this->entrySize = entrySize;
    defaultState = std::make_unique<State>(entrySize);
}

void NonKeyedSliceMergingHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start NonKeyedSliceMergingHandler");
}

void NonKeyedSliceMergingHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                       Runtime::Execution::PipelineExecutionContextPtr) {
    NES_DEBUG("stop NonKeyedSliceMergingHandler: {}", queryTerminationType);
}

GlobalSlicePtr NonKeyedSliceMergingHandler::createGlobalSlice(SliceMergeTask* sliceMergeTask) {
    return std::make_unique<NonKeyedSlice>(entrySize, sliceMergeTask->startSlice, sliceMergeTask->endSlice, defaultState);
}

const State* NonKeyedSliceMergingHandler::getDefaultState() const { return defaultState.get(); }

NonKeyedSliceMergingHandler::~NonKeyedSliceMergingHandler() { NES_DEBUG("Destruct NonKeyedSliceMergingHandler"); }

void NonKeyedSliceMergingHandler::appendToGlobalSliceStore(NonKeyedSlicePtr slice) {
    NES_ASSERT(globalSliceStore != 0, "slice store is not initialized");
    globalSliceStore->insertSlice(std::move(slice));
}

void NonKeyedSliceMergingHandler::triggerSlidingWindows(Runtime::WorkerContext& wctx,
                                                        Runtime::Execution::PipelineExecutionContext& ctx,
                                                        uint64_t sequenceNumber,
                                                        uint64_t slideEnd) {
    NES_ASSERT(globalSliceStore != 0, "slice store is not initialized");
    // the watermark update is an atomic process and returns the last and the current watermark.
    auto currentWatermark = watermarkProcessor->updateWatermark(slideEnd, sequenceNumber, 0);

    // the watermark has changed get the lock to trigger
    std::lock_guard<std::mutex> lock(triggerMutex);
    // update currentWatermark, such that other threads to have to acquire the lock
    NES_DEBUG("Trigger sliding windows between {}-{}", lastTriggerWatermark, currentWatermark);

    auto windows = globalSliceStore->collectWindows(lastTriggerWatermark, currentWatermark);
    // collect all slices that end <= watermark from all thread local slice stores.
    auto bufferProvider = wctx.getBufferProvider();
    for (const auto& [windowStart, windowEnd] : windows) {
        auto slicesForWindow = globalSliceStore->collectSlicesForWindow(windowStart, windowEnd);
        NES_DEBUG("Deploy window ({}-{}) merge task for {} slices  ", windowStart, windowEnd, slicesForWindow.size());
        auto buffer = bufferProvider->getBufferBlocking();
        auto task = buffer.getBuffer<Window<NonKeyedSlice>>();
        task->startTs = windowStart;
        task->endTs = windowEnd;
        task->slices = slicesForWindow;
        task->sequenceNumber = resultSequenceNumber++;
        buffer.setNumberOfTuples(1);
        ctx.dispatchBuffer(buffer);
    }
    // remove all slices from the slice store that are not necessary anymore.
    globalSliceStore->removeSlices(currentWatermark);
    lastTriggerWatermark = currentWatermark;
};

}// namespace NES::Runtime::Execution::Operators