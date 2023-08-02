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

#ifndef NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
#define NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
#include <Common/Identifiers.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Util/VirtualEnableSharedFromThis.hpp>
#include <set>
#include <vector>

namespace NES::Runtime::Execution::Operators {
class MultiOriginWatermarkProcessor;

/**
 * @brief The AbstractSlicePreAggregationHandler provides an abstract operator handler to perform slice-based pre-aggregation
 * of time-based tumbling and sliding windows.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer trigger is called, which checks if the thread-local slice store should be triggered.
 * This is decided by the current watermark timestamp.
 */
template<class SliceType, typename SliceStore>
class AbstractSlicePreAggregationHandler : public Runtime::Execution::OperatorHandler {
  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     * @param windowDefinition logical window definition
     * @param origins the set of origins, which can produce data for the window operator
     * @param weakSliceStagingPtr access to the slice staging.
     */
    AbstractSlicePreAggregationHandler(uint64_t windowSize, uint64_t windowSlide, const std::vector<OriginId>& origins)
        : windowSize(windowSize), windowSlide(windowSlide),
          watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)){};

    SliceStore* getThreadLocalSliceStore(uint64_t workerId) {
        auto index = workerId % threadLocalSliceStores.size();
        return threadLocalSliceStores[index].get();
    }

    /**
     * @brief This method triggers the thread local state and appends all slices,
     * which end before the current global watermark to the slice staging area.
     * @param wctx WorkerContext
     * @param ctx PipelineExecutionContext
     * @param workerId
     * @param originId
     * @param sequenceNumber
     * @param watermarkTs
     */
    void trigger(Runtime::WorkerContext& wctx,
                 Runtime::Execution::PipelineExecutionContext& ctx,
                 OriginId originId,
                 uint64_t sequenceNumber,
                 uint64_t watermarkTs) {
        // the watermark update is an atomic process and returns the last and the current watermark.
        NES_DEBUG("{} Trigger {}-{}-{}", windowSize, originId, sequenceNumber, watermarkTs);
        auto currentWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

        if (lastTriggerWatermark == currentWatermark) {
            // if the current watermark has not changed, we don't have to trigger any windows and return.
            return;
        }

        // the watermark has changed get the lock to trigger
        std::lock_guard<std::mutex> lock(triggerMutex);
        // update currentWatermark, such that other threads to have to acquire the lock
        NES_DEBUG("{} Trigger slices between {}-{}", windowSize, lastTriggerWatermark, currentWatermark);
        lastTriggerWatermark = currentWatermark;

        // collect all slices that end <= watermark from all thread local slice stores.
        std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>> collectedSlices;
        for (auto& threadLocalSliceStore : threadLocalSliceStores) {
            for (const auto& slice : threadLocalSliceStore->getSlices()) {
                NES_DEBUG("Slices in slice store {}-{}", slice->getStart(), slice->getEnd());
            }
            auto slices = threadLocalSliceStore->extractSlicesUntilTs(lastTriggerWatermark);
            for (const auto& slice : slices) {
                NES_DEBUG("Assign thread local slices {}-{}", slice->getStart(), slice->getEnd());
                auto sliceData = std::make_tuple(slice->getStart(), slice->getEnd());
                if (!collectedSlices.contains(sliceData)) {
                    collectedSlices.emplace(std::make_tuple(slice->getStart(), slice->getEnd()),
                                            std::vector<std::shared_ptr<SliceType>>());
                }
                collectedSlices.find(sliceData)->second.emplace_back(slice);
            }
            threadLocalSliceStore->setLastWatermark(lastTriggerWatermark);
        }
        dispatchSliceMergingTasks(ctx, wctx.getBufferProvider(), collectedSlices);
    };

    void dispatchSliceMergingTasks(
        Runtime::Execution::PipelineExecutionContext& ctx,
        std::shared_ptr<AbstractBufferProvider> bufferProvider,
        std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>>& collectedSlices) {
        // for all slices that have been collected, emit a merge task to combine this slices.
        // note: the sliceMetaData set is ordered implicitly by the slice start time as the std::set
        // is an associative container that contains a sorted set of unique objects of type Key.
        // Thus, we emit slice deployment tasks in increasing order.
        for (const auto& [metaData, slices] : collectedSlices) {
            auto buffer = bufferProvider->getBufferBlocking();
            auto task = buffer.getBuffer<SliceMergeTask<SliceType>>();
            task->startSlice = std::get<0>(metaData);
            task->endSlice = std::get<1>(metaData);
            task->sequenceNumber = resultSequenceNumber++;
            task->slices = slices;
            NES_DEBUG("{} Deploy merge task for slice {}-{} ", windowSize, task->startSlice, task->endSlice);
            // task->slices = slices[sliceEnd];
            // the buffer contains one slice tasks, so we have to set the number of tuples to 1.
            buffer.setNumberOfTuples(1);
            ctx.dispatchBuffer(buffer);
        }
    }

    void start(Runtime::Execution::PipelineExecutionContextPtr, Runtime::StateManagerPtr, uint32_t) {
        NES_DEBUG("start AbstractSlicePreAggregationHandler");
    }

    void stop(Runtime::QueryTerminationType queryTerminationType, Runtime::Execution::PipelineExecutionContextPtr ctx) {
        NES_DEBUG("shutdown AbstractSlicePreAggregationHandler: {}", queryTerminationType);

        // get the lock to trigger -> this should actually not be necessary, as stop can not be called concurrently to the processing.
        std::lock_guard<std::mutex> lock(triggerMutex);

        if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
            // collect all remaining slices from all thread local slice stores.
            std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>> collectedSlices;
            for (auto& threadLocalSliceStore : threadLocalSliceStores) {
                // we can directly access the slices as no other worker can concurrently change them
                for (auto& slice : threadLocalSliceStore->getSlices()) {
                    auto& sliceState = slice->getState();
                    // each worker adds its local state to the staging area
                    auto sliceData = std::make_tuple(slice->getStart(), slice->getEnd());
                    if (!collectedSlices.contains(sliceData)) {
                        collectedSlices.emplace(std::make_tuple(slice->getStart(), slice->getEnd()),
                                                std::vector<std::shared_ptr<SliceType>>());
                    }
                    collectedSlices.find(sliceData)->second.emplace_back(std::move(slice));
                }
            }
            dispatchSliceMergingTasks(*ctx.get(), ctx->getBufferManager(), collectedSlices);
        }
    }

  protected:
    const uint64_t windowSize;
    const uint64_t windowSlide;
    std::vector<std::unique_ptr<SliceStore>> threadLocalSliceStores;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic<uint64_t> lastTriggerWatermark = 0;
    std::atomic<uint64_t> resultSequenceNumber = 1;
    std::mutex triggerMutex;
};
}// namespace NES::Runtime::Execution::Operators

#endif//NES_NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
