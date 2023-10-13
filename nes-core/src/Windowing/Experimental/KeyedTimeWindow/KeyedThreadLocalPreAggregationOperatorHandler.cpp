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

#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/NonBlockingMonotonicSeqQueue.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedSliceStaging.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalPreAggregationOperatorHandler.hpp>
#include <Windowing/Experimental/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Windowing/Experimental/LockFreeMultiOriginWatermarkProcessor.hpp>
#include <Windowing/Experimental/WindowProcessingTasks.hpp>
#include <Operators/LogicalOperators/Windows/LogicalWindowDefinition.hpp>
#include <Windowing/WindowMeasures/TimeMeasure.hpp>
#include <Windowing/WindowTypes/TimeBasedWindowType.hpp>

namespace NES::Windowing::Experimental {

KeyedThreadLocalPreAggregationOperatorHandler::KeyedThreadLocalPreAggregationOperatorHandler(
    const Windowing::LogicalWindowDefinitionPtr& windowDefinition,
    const std::vector<OriginId> origins,
    std::weak_ptr<KeyedSliceStaging> weakSliceStagingPtr)
    : weakSliceStaging(weakSliceStagingPtr), windowDefinition(windowDefinition) {
    watermarkProcessor = NES::Experimental::LockFreeMultiOriginWatermarkProcessor::create(origins);
    auto windowType = Windowing::WindowType::asTimeBasedWindowType(windowDefinition->getWindowType());
    windowSize = windowType->getSize().getTime();
    windowSlide = windowType->getSlide().getTime();
}

KeyedThreadLocalSliceStore& KeyedThreadLocalPreAggregationOperatorHandler::getThreadLocalSliceStore(uint64_t workerId) {
    if (threadLocalSliceStores.size() <= workerId) {
        throw WindowProcessingException("ThreadLocalSliceStore for " + std::to_string(workerId) + " is not initialized.");
    }
    return *threadLocalSliceStores[workerId];
}

void KeyedThreadLocalPreAggregationOperatorHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx,
                                                          NES::Experimental::HashMapFactoryPtr hashmapFactory) {
    for (uint64_t i = 0; i < ctx.getNumberOfWorkerThreads(); i++) {
        auto threadLocalSliceStore = std::make_unique<KeyedThreadLocalSliceStore>(hashmapFactory, windowSize, windowSlide);
        threadLocalSliceStores.emplace_back(std::move(threadLocalSliceStore));
    }
    this->factory = hashmapFactory;
}

void KeyedThreadLocalPreAggregationOperatorHandler::triggerThreadLocalState(Runtime::WorkerContext& wctx,
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
            auto [addedPartitionsToSlice, numberOfBuffers] =
                sliceStaging->addToSlice(slice->getEnd(), sliceState.extractEntries());
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

void KeyedThreadLocalPreAggregationOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr,
                                                          Runtime::StateManagerPtr,
                                                          uint32_t) {
    NES_DEBUG("start ThreadLocalPreAggregationWindowHandler");
}

void KeyedThreadLocalPreAggregationOperatorHandler::stop(
    Runtime::QueryTerminationType queryTerminationType,
    Runtime::Execution::PipelineExecutionContextPtr pipelineExecutionContext) {
    NES_DEBUG("stop ThreadLocalPreAggregationWindowHandler: {}", queryTerminationType);
    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        auto sliceStaging = this->weakSliceStaging.lock();
        NES_ASSERT(sliceStaging, "Slice staging is invalid. This should not happen for graceful stop.");
        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            for (auto& slice : threadLocalSliceStore->getSlices()) {
                auto& sliceState = slice->getState();
                // each worker adds its local state to the staging area
                auto [addedPartitionsToSlice, numberOfBuffers] =
                    sliceStaging->addToSlice(slice->getEnd(), sliceState.extractEntries());
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

KeyedThreadLocalPreAggregationOperatorHandler::~KeyedThreadLocalPreAggregationOperatorHandler() {
    NES_DEBUG("~KeyedThreadLocalPreAggregationOperatorHandler");
}

Windowing::LogicalWindowDefinitionPtr KeyedThreadLocalPreAggregationOperatorHandler::getWindowDefinition() {
    return windowDefinition;
}

void KeyedThreadLocalPreAggregationOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {
    this->threadLocalSliceStores.clear();
}

}// namespace NES::Windowing::Experimental