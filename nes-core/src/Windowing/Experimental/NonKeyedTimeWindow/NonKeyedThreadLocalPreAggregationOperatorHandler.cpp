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

#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Operators/LogicalOperators/Windows/Measures/TimeMeasure.hpp>
#include <Operators/LogicalOperators/Windows/Types/TimeBasedWindowType.hpp>
#include <Operators/LogicalOperators/Windows/Types/WindowType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/Experimental/HashMap.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedSliceStaging.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/Experimental/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>

namespace NES::Windowing::Experimental {

NonKeyedThreadLocalPreAggregationOperatorHandler::NonKeyedThreadLocalPreAggregationOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    const std::vector<OriginId> origins,
    std::weak_ptr<NonKeyedSliceStaging> weakSliceStagingPtr)
    : weakSliceStaging(weakSliceStagingPtr), windowDefinition(windowDefinition) {
    watermarkProcessor = NES::Experimental::LockFreeMultiOriginWatermarkProcessor::create(origins);
    auto windowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    windowSize = windowType->getSize().getTime();
    windowSlide = windowType->getSlide().getTime();
}

NonKeyedThreadLocalSliceStore& NonKeyedThreadLocalPreAggregationOperatorHandler::getThreadLocalSliceStore(uint64_t workerId) {
    if (threadLocalSliceStores.size() <= workerId) {
        throw WindowProcessingException("ThreadLocalSliceStore for " + std::to_string(workerId) + " is not initialized.");
    }
    return *threadLocalSliceStores[workerId];
}

void NonKeyedThreadLocalPreAggregationOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                                             uint64_t entrySize) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocal = std::make_unique<NonKeyedThreadLocalSliceStore>(entrySize, windowSize, windowSlide);
        threadLocalSliceStores.push_back(std::move(threadLocal));
    }
}

void NonKeyedThreadLocalPreAggregationOperatorHandler::triggerThreadLocalState(Runtime::WorkerContext& wctx,
                                                                               Runtime::Execution::PipelineExecutionContext& ctx,
                                                                               uint64_t workerId,
                                                                               OriginId originId,
                                                                               uint64_t sequenceNumber,
                                                                               uint64_t watermarkTs) {

    auto& threadLocalSliceStore = getThreadLocalSliceStore(workerId);

    // the watermark update is an atomic process and returns the last and the current watermark.
    auto newGlobalWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

    auto sliceStaging = this->weakSliceStaging.lock();
    if (!sliceStaging) {
        NES_FATAL_ERROR("SliceStaging is invalid, this should only happen after a hard stop. Drop all in flight data.");
        return;
    }

    // check if the current max watermark is larger than the thread local watermark
    if (newGlobalWatermark > threadLocalSliceStore.getLastWatermark()) {
        for (auto& slice : threadLocalSliceStore.getSlices()) {
            if (slice->getEnd() > newGlobalWatermark) {
                break;
            }
            auto& sliceState = slice->getState();
            // each worker adds its local state to the staging area
            auto [addedPartitionsToSlice, numberOfBuffers] = sliceStaging->addToSlice(slice->getEnd(), std::move(sliceState));
            if (addedPartitionsToSlice == threadLocalSliceStores.size()) {
                if (numberOfBuffers != 0) {
                    NES_DEBUG("Deploy merge task for slice {} with {} buffers.", slice->getEnd(), numberOfBuffers);
                    auto buffer = wctx.allocateTupleBuffer();
                    auto task = buffer.getBuffer<SliceMergeTask>();
                    task->startSlice = slice->getStart();
                    task->endSlice = slice->getEnd();
                    buffer.setNumberOfTuples(1);
                    ctx.dispatchBuffer(buffer);
                } else {
                    NES_DEBUG("Slice {} is empty. Don't deploy merge task.", slice->getEnd());
                }
            }
        }
        threadLocalSliceStore.removeSlicesUntilTs(newGlobalWatermark);
        threadLocalSliceStore.setLastWatermark(newGlobalWatermark);
    }
}

void NonKeyedThreadLocalPreAggregationOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                             Runtime::StateManagerPtr,
                                                             uint32_t) {
    NES_DEBUG("start NonKeyedThreadLocalPreAggregationOperatorHandler");
}

void NonKeyedThreadLocalPreAggregationOperatorHandler::stop(
    Runtime::QueryTerminationType queryTerminationType,
    Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) {
    NES_DEBUG("shutdown NonKeyedThreadLocalPreAggregationOperatorHandler: {}", queryTerminationType);

    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        auto sliceStaging = this->weakSliceStaging.lock();
        NES_ASSERT(sliceStaging, "SliceStaging is invalid, this should only happen after a soft stop.");
        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            for (auto& slice : threadLocalSliceStore->getSlices()) {
                auto& sliceState = slice->getState();
                // each worker adds its local state to the staging area
                auto [addedPartitionsToSlice, numberOfBuffers] = sliceStaging->addToSlice(slice->getEnd(), std::move(sliceState));
                if (addedPartitionsToSlice == threadLocalSliceStores.size()) {
                    if (numberOfBuffers != 0) {
                        NES_DEBUG("Deploy merge task for slice ({}-{}) with {} buffers.",
                                  slice->getStart(),
                                  slice->getEnd(),
                                  numberOfBuffers);
                        auto buffer = pipelineExecutionContext->getBufferManager()->getBufferBlocking();
                        auto task = buffer.getBuffer<SliceMergeTask>();
                        task->startSlice = slice->getStart();
                        task->endSlice = slice->getEnd();
                        buffer.setNumberOfTuples(1);
                        pipelineExecutionContext->dispatchBuffer(buffer);
                    } else {
                        NES_DEBUG("Slice {} is empty. Don't deploy merge task.", slice->getEnd());
                    }
                }
            }
        }
    }
}

NonKeyedThreadLocalPreAggregationOperatorHandler::~NonKeyedThreadLocalPreAggregationOperatorHandler() {
    NES_DEBUG("~NonKeyedThreadLocalPreAggregationOperatorHandler");
}

Windowing::LogicalWindowDefinitionPtr NonKeyedThreadLocalPreAggregationOperatorHandler::getWindowDefinition() {
    return windowDefinition;
}

void NonKeyedThreadLocalPreAggregationOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    this->threadLocalSliceStores.clear();
}

}// namespace NES::Windowing::Experimental