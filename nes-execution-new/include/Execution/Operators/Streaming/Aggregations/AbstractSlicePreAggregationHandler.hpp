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

#ifndef NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
#define NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Sequencing/SequenceData.hpp>
#include <map>
#include <tuple>
#include <vector>

namespace NES::Runtime {
class AbstractBufferProvider;
}

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
     * @brief Creates the slice pre aggregation operator handler
     * @param windowSize size of the window
     * @param windowSlide slide of the window
     * @param origins the set of origins, which can produce data for the window operator
     */
    AbstractSlicePreAggregationHandler(uint64_t windowSize, uint64_t windowSlide, const std::vector<OriginId>& origins);

    /**
     * @brief Returns a thread local slice store for a specific worker
     * @param workerThreadId
     * @return SliceStore*
     */
    SliceStore* getThreadLocalSliceStore(WorkerThreadId workerThreadId);

    /**
     * @brief This method triggers the thread local state and appends all slices,
     * which end before the current global watermark to the slice staging area.
     * @param wctx WorkerContext
     * @param ctx PipelineExecutionContext
     * @param originId
     * @param sequenceData
     * @param watermarkTs
     */
    void trigger(WorkerContext& wctx,
                 PipelineExecutionContext& ctx,
                 OriginId originId,
                 SequenceData sequenceData,
                 uint64_t watermarkTs);

    void start(PipelineExecutionContextPtr, uint32_t);
    void stop(QueryTerminationType queryTerminationType, PipelineExecutionContextPtr ctx);
    ~AbstractSlicePreAggregationHandler();

  protected:
    const uint64_t windowSize;
    const uint64_t windowSlide;
    std::vector<std::unique_ptr<SliceStore>> threadLocalSliceStores;
    std::unique_ptr<MultiOriginWatermarkProcessor> watermarkProcessor;
    std::atomic<uint64_t> lastTriggerWatermark = 0;
    std::atomic<uint64_t> resultSequenceNumber = 1;
    std::mutex triggerMutex;

  private:
    void
    dispatchSliceMergingTasks(PipelineExecutionContext& ctx,
                              std::shared_ptr<AbstractBufferProvider> bufferProvider,
                              std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>>& collectedSlices);
};
}// namespace NES::Runtime::Execution::Operators

#endif// NES_EXECUTION_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_ABSTRACTSLICEPREAGGREGATIONHANDLER_HPP_
