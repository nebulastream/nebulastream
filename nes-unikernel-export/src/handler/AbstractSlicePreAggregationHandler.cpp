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

#include <Execution/Operators/Streaming/Aggregations/AbstractSlicePreAggregationHandler.hpp>


#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedThreadLocalSliceStore.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
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

template<class SliceType, typename SliceStore>
AbstractSlicePreAggregationHandler<SliceType, SliceStore>::AbstractSlicePreAggregationHandler(
    uint64_t windowSize,
    uint64_t windowSlide,
    const std::vector<OriginId>& origins)
    : windowSize(windowSize), windowSlide(windowSlide),
      watermarkProcessor(std::make_unique<MultiOriginWatermarkProcessor>(origins)){};

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::dispatchSliceMergingTasks(
    PipelineExecutionContext& ctx,
    std::shared_ptr<AbstractBufferProvider> bufferProvider,
    std::map<std::tuple<uint64_t, uint64_t>, std::vector<std::shared_ptr<SliceType>>>& collectedSlices) {
NES_THROW_RUNTIME_ERROR("Not Implemented");
}

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::trigger(WorkerContext& wctx,
                                                                        PipelineExecutionContext& ctx,
                                                                        OriginId originId,
                                                                        uint64_t sequenceNumber,
                                                                        uint64_t watermarkTs) {
NES_THROW_RUNTIME_ERROR("Not Implemented");
};

template<class SliceType, typename SliceStore>
SliceStore* AbstractSlicePreAggregationHandler<SliceType, SliceStore>::getThreadLocalSliceStore(uint64_t workerId) {
NES_THROW_RUNTIME_ERROR("Not Implemented");
}

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::start(PipelineExecutionContextPtr, uint32_t) {
NES_THROW_RUNTIME_ERROR("Not Implemented");
}

template<class SliceType, typename SliceStore>
void AbstractSlicePreAggregationHandler<SliceType, SliceStore>::stop(QueryTerminationType queryTerminationType,
                                                                     PipelineExecutionContextPtr ctx) {
NES_THROW_RUNTIME_ERROR("Not Implemented");
}

template<class SliceType, typename SliceStore>
AbstractSlicePreAggregationHandler<SliceType, SliceStore>::~AbstractSlicePreAggregationHandler() {}

// Instantiate types
template class AbstractSlicePreAggregationHandler<NonKeyedSlice, NonKeyedThreadLocalSliceStore>;

template class AbstractSlicePreAggregationHandler<KeyedSlice, KeyedThreadLocalSliceStore>;


}// namespace NES::Runtime::Execution::Operators