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

#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <tuple>
#include <utility>

namespace NES::Runtime::Execution::Operators {

KeyedSlicePreAggregationHandler::KeyedSlicePreAggregationHandler(uint64_t windowSize,
                                                                 uint64_t windowSlide,
                                                                 const std::vector<OriginId>& origins,
                                                                 std::weak_ptr<KeyedSliceStaging> weakSliceStagingPtr)
    : windowSize(windowSize), windowSlide(windowSlide), weakSliceStaging(std::move(weakSliceStagingPtr)),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)) {}
class NewTag;
KeyedThreadLocalSliceStore* KeyedSlicePreAggregationHandler::getThreadLocalSliceStore(uint64_t workerId) {

    auto index = workerId % threadLocalSliceStores.size();
    return threadLocalSliceStores[index].get();
}

void KeyedSlicePreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                            uint64_t keySize,
                                            uint64_t valueSize) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<KeyedThreadLocalSliceStore>(keySize, valueSize, windowSize, windowSlide);
        threadLocalSliceStores.emplace_back(std::move(threadLocal));
    }
}

void KeyedSlicePreAggregationHandler::triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                                              Runtime::Execution::PipelineExecutionContext& ctx,
                                                              uint64_t,
                                                              OriginId originId,
                                                              uint64_t sequenceNumber,
                                                              uint64_t watermarkTs) {
    // the watermark update is an atomic process and returns the last and the current watermark.
    auto currentWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

    // todo try cas
    if (lastTriggerWatermark == currentWatermark) {
        // if the current watermark has not changed, we don't have to trigger any windows and return.
        return;
    }

    // get the lock to trigger
    std::lock_guard<std::mutex> lock(triggerMutex);
    // update currentWatermark, such that other threads to have to acquire the lock
    auto oldWatermark = lastTriggerWatermark.load();
    lastTriggerWatermark.exchange(currentWatermark);

    NES_TRACE("Trigger slices between {}-{}", oldWatermark, lastTriggerWatermark);
    auto sliceStaging = this->weakSliceStaging.lock();
    if (!sliceStaging) {
        NES_FATAL_ERROR("SliceStaging is invalid, this should only happen after a hard stop. Drop all in flight data.");
        return;
    }

    std::set<std::tuple<uint64_t, uint64_t>> sliceMetaData;
    for (auto& threadLocalSliceStore : threadLocalSliceStores) {
        auto slices = threadLocalSliceStore->extractSlicesUntilTs(currentWatermark);
        for (auto& slice : slices) {
              sliceMetaData.emplace(slice->getStart(), slice->getEnd());
              NES_TRACE("Assign thread local slices {}-{} with n records {}",
                     slice->getStart(),
                     slice->getEnd(), slice->getState()->getCurrentSize());
            sliceStaging->addToSlice(slice->getEnd(), std::move(slice->getState()));
        }
    }

    for (const auto& [sliceStart, sliceEnd] : sliceMetaData) {
        NES_TRACE("Deploy merge task for slice {}-{} ", sliceStart, sliceEnd);
        auto buffer = wctx.allocateTupleBuffer();
        auto task = buffer.getBuffer<SliceMergeTask>();
        task->startSlice = sliceStart;
        task->endSlice = sliceEnd;
        buffer.setNumberOfTuples(1);
        ctx.dispatchBuffer(buffer);
    }
}

void KeyedSlicePreAggregationHandler::start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
    NES_DEBUG("start GlobalSlicePreAggregationHandler");
}

void KeyedSlicePreAggregationHandler::stop(Runtime::QueryTerminationType queryTerminationType,
                                           Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) {
    NES_DEBUG("shutdown GlobalSlicePreAggregationHandler: {}", queryTerminationType);

    // get the lock to trigger -> this should actually not be necessary, as stop can not be called concurrently to the processing.
    std::lock_guard<std::mutex> lock(triggerMutex);

    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        auto sliceStaging = this->weakSliceStaging.lock();
        NES_ASSERT(sliceStaging, "SliceStaging is invalid, this should only happen after a soft stop.");

        std::set<std::tuple<uint64_t, uint64_t>> sliceMetaData;

        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            // we can directly access the slices as no other worker can concurrently change them
            for (auto& slice : threadLocalSliceStore->getSlices()) {
                auto& sliceState = slice->getState();
                // each worker adds its local state to the staging area
                sliceStaging->addToSlice(slice->getEnd(), std::move(sliceState));
                sliceMetaData.emplace(slice->getStart(), slice->getEnd());
            }
        }

        for (const auto& [sliceStart, sliceEnd] : sliceMetaData) {
            NES_TRACE("Deploy merge task for slice {}-{} ", sliceStart, sliceEnd);
            auto buffer = pipelineExecutionContext->getBufferManager()->getBufferBlocking();
            auto task = buffer.getBuffer<SliceMergeTask>();
            task->startSlice = sliceStart;
            task->endSlice = sliceEnd;
            buffer.setNumberOfTuples(1);
            pipelineExecutionContext->dispatchBuffer(buffer);
        }
    }
}
KeyedSlicePreAggregationHandler::~KeyedSlicePreAggregationHandler() { NES_DEBUG("~GlobalSlicePreAggregationHandler"); }

void KeyedSlicePreAggregationHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    // this->threadLocalSliceStores.clear();
}

}// namespace NES::Runtime::Execution::Operators