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

#ifndef NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDBUCKETPREAGGREGATIONHANDLER_HPP_
#define NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDBUCKETPREAGGREGATIONHANDLER_HPP_

#include <Identifiers.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/AbstractBucketPreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/Buckets/KeyedBucketStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <vector>

namespace NES::Runtime::Execution::Operators {

class MultiOriginWatermarkProcessor;
class State;
/**
 * @brief The KeyedSlicePreAggregationHandler provides an operator handler to perform slice-based pre-aggregation of keyed tumbling windows.
 * @note sliding windows will be added later.
 * This operator handler, maintains a slice store for each worker thread and provides them for the aggregation.
 * For each processed tuple buffer triggerThreadLocalState is called, which checks if the thread-local slice store should be triggered.
 * This is decided by the current watermark timestamp.
 */
class KeyedBucketPreAggregationHandler : public AbstractBucketPreAggregationHandler<KeyedSlice, KeyedBucketStore> {
  public:
    /**
     * @brief Creates the operator handler with a specific window definition, a set of origins, and access to the slice staging object.
     * @param windowDefinition logical window definition
     * @param origins the set of origins, which can produce data for the window operator
     * @param weakSliceStagingPtr access to the slice staging.
     */
    KeyedBucketPreAggregationHandler(uint64_t windowSize, uint64_t windowSlide, const std::vector<OriginId>& origins);

    /**
     * @brief Initializes the thread local state for the window operator
     * @param ctx PipelineExecutionContext
     * @param entrySize Size of the aggregated values in memory
     */
    void setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t keySize, uint64_t valueSize);

    ~KeyedBucketPreAggregationHandler() override;
};
}// namespace NES::Runtime::Execution::Operators
#endif// NES_RUNTIME_INCLUDE_EXECUTION_OPERATORS_STREAMING_AGGREGATIONS_KEYEDTIMEWINDOW_KEYEDBUCKETPREAGGREGATIONHANDLER_HPP_
