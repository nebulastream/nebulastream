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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_ABSTRACTBUCKETPREAGGREGATIONHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_ABSTRACTBUCKETPREAGGREGATIONHANDLER_HPP_
#include <Execution/Operators/Streaming/Aggregations/AbstractSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/BucketStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/NonKeyedBucketStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Identifiers/Identifiers.hpp>
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
class State;
/**
 * @brief The AbstractBucketPreAggregationHandler provides an operator handler to perform bucket-based pre-aggregation
 * of tumbling and sliding windows.
 * This operator handler, maintains a bucket store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer trigger is called, which checks if the thread-local store should be triggered.
 * This is decided by the current watermark timestamp.
 */
template<class SliceType, typename SliceStore>
class AbstractBucketPreAggregationHandler : public Runtime::Execution::OperatorHandler {
  public:
    /**
     * @brief Creates the operator handler .
     * @param windowSize
     * @param windowSlide
     * @param origins the set of origins, which can produce data for the window operator
     */
    AbstractBucketPreAggregationHandler(uint64_t windowSize, uint64_t windowSlide, const std::vector<OriginId>& origins);

    SliceStore* getThreadLocalBucketStore(uint64_t workerId);
    void
    trigger(WorkerContext& wctx, PipelineExecutionContext& ctx, OriginId originId, SequenceData sequenceData, uint64_t watermarkTs);
    void
    dispatchSliceMergingTasks(PipelineExecutionContext& ctx,
                              std::shared_ptr<AbstractBufferProvider> bufferProvider,
                              std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>>& collectedSlices);
    ~AbstractBucketPreAggregationHandler();

    void start(PipelineExecutionContextPtr, uint32_t);
    void stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr ctx);

  protected:
    const uint64_t windowSize;
    const uint64_t windowSlide;
    std::vector<std::unique_ptr<SliceStore>> threadLocalBuckets;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic<uint64_t> lastTriggerWatermark = 0;
    std::atomic<uint64_t> resultSequenceNumber = 1;
    std::mutex triggerMutex;
    std::unique_ptr<State> defaultState;
};
}// namespace NES::Runtime::Execution::Operators
#endif // NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_BUCKETS_ABSTRACTBUCKETPREAGGREGATIONHANDLER_HPP_
