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

#include <Execution/Operators/Streaming/Aggregations/Buckets/AbstractBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

template<class SliceType, typename SliceStore>
AbstractBucketPreAggregationHandler<SliceType, SliceStore>::AbstractBucketPreAggregationHandler(
    uint64_t windowSize,
    uint64_t windowSlide,
    const std::vector<OriginId>& origins)
    : windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)){};

template<class SliceType, typename SliceStore>
SliceStore* AbstractBucketPreAggregationHandler<SliceType, SliceStore>::getThreadLocalBucketStore(uint64_t workerId) {
    auto index = workerId % threadLocalBuckets.size();
    return threadLocalBuckets[index].get();
}
template<class SliceType, typename SliceStore>
void AbstractBucketPreAggregationHandler<SliceType, SliceStore>::trigger(WorkerContext& wctx,
                                                                         PipelineExecutionContext& ctx,
                                                                         OriginId originId,
                                                                         uint64_t sequenceNumber,
                                                                         uint64_t watermarkTs) {
    // the watermark update is an atomic process and returns the last and the current watermark.
    NES_TRACE("{} Trigger {}-{}-{}", windowSize, originId, sequenceNumber, watermarkTs);
    auto currentWatermark = watermarkProcessor->updateWatermark(watermarkTs, sequenceNumber, originId);

    if (lastTriggerWatermark == currentWatermark) {
        // if the current watermark has not changed, we don't have to trigger any windows and return.
        return;
    }

    // the watermark has changed get the lock to trigger
    std::lock_guard<std::mutex> lock(triggerMutex);
    // update currentWatermark, such that other threads to have to acquire the lock
    NES_TRACE("{} Trigger slices between {}-{}", windowSize, lastTriggerWatermark, currentWatermark);
    lastTriggerWatermark = currentWatermark;

    // collect all buckets that end <= watermark from all thread local bucket stores.
    std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>> collectedBuckets;
    for (auto& bucketStore : threadLocalBuckets) {
        auto buckets = bucketStore->extractBucketsUntilTs(lastTriggerWatermark);
        for (const auto& bucket : buckets) {
            NES_TRACE("Assign thread local bucket {}-{}", bucket->getStart(), bucket->getEnd());
            auto metaData = std::make_tuple(bucket->getStart(), bucket->getEnd());
            if (!collectedBuckets.contains(metaData)) {
                collectedBuckets.emplace(std::make_tuple(bucket->getStart(), bucket->getEnd()),
                                         std::vector<std::shared_ptr<SliceType>>());
            }
            collectedBuckets.find(metaData)->second.emplace_back(bucket);
        }
        bucketStore->setLastWatermark(lastTriggerWatermark);
    }
    dispatchSliceMergingTasks(ctx, wctx.getBufferProvider(), collectedBuckets);
};
template<class SliceType, typename SliceStore>
void AbstractBucketPreAggregationHandler<SliceType, SliceStore>::dispatchSliceMergingTasks(
    PipelineExecutionContext& ctx,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>>& collectedSlices) {
    // for all thread local buckets that have been collected, emit a merge task to combine this buckets.
    // note: the sliceMetaData set is ordered implicitly by the slice start time as the std::map
    // is an associative container that contains a sorted set of unique objects of type Key.
    // Thus, we emit slice deployment tasks in increasing order.
    for (const auto& [metaData, slices] : collectedSlices) {
        auto buffer = bufferProvider->getBufferBlocking();
        // allocate a slice merge task withing the buffer.
        auto task = allocateWithin<SliceMergeTask<SliceType>>(buffer);
        task->startSlice = std::get<0>(metaData);
        task->endSlice = std::get<1>(metaData);
        task->sequenceNumber = resultSequenceNumber++;
        task->slices = slices;
        NES_DEBUG("{} Deploy merge task for bucket {}-{} ", windowSize, task->startSlice, task->endSlice);
        ctx.dispatchBuffer(buffer);
    }
}
template<class SliceType, typename SliceStore>
void AbstractBucketPreAggregationHandler<SliceType, SliceStore>::start(PipelineExecutionContextPtr, uint32_t) {
    NES_DEBUG("start AbstractBucketPreAggregationHandler");
}
template<class SliceType, typename SliceStore>
void AbstractBucketPreAggregationHandler<SliceType, SliceStore>::stop(QueryTerminationType queryTerminationType,
                                                                      PipelineExecutionContextPtr ctx) {
    NES_DEBUG("shutdown AbstractBucketPreAggregationHandler: {}", queryTerminationType);

    // get the lock to trigger -> this should actually not be necessary, as stop can not be called concurrently to the processing.
    std::lock_guard<std::mutex> lock(triggerMutex);
    if (queryTerminationType == Runtime::QueryTerminationType::Graceful) {
        // collect all remaining slices from all thread local slice stores.
        std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>> collectedSlices;
        for (auto& threadLocalSliceStore : threadLocalBuckets) {
            // we can directly access the buckets as no other worker can concurrently change them
            for (auto& ref : threadLocalSliceStore->getSlices()) {
                auto& slice = ref.second;
                auto sliceData = std::make_tuple(slice->getStart(), slice->getStart() + windowSize);
                if (!collectedSlices.contains(sliceData)) {
                    collectedSlices.emplace(std::make_tuple(slice->getStart(), slice->getStart() + windowSize),
                                            std::vector<std::shared_ptr<SliceType>>());
                }
                collectedSlices.find(sliceData)->second.emplace_back(std::move(slice));
            }
        }
        dispatchSliceMergingTasks(*ctx.get(), ctx->getBufferManager(), collectedSlices);
    } else if (queryTerminationType == Runtime::QueryTerminationType::Drain) {
        NES_NOT_IMPLEMENTED();
    }
}
template<class SliceType, typename SliceStore>
AbstractBucketPreAggregationHandler<SliceType, SliceStore>::~AbstractBucketPreAggregationHandler() {
    NES_DEBUG("~AbstractBucketPreAggregationHandler");
}

// Instantiate types
template class AbstractBucketPreAggregationHandler<NonKeyedSlice, NonKeyedBucketStore>;
template class AbstractBucketPreAggregationHandler<KeyedSlice, KeyedBucketStore>;

}// namespace NES::Runtime::Execution::Operators