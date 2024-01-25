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
#include "OperatorHandlerTracer.hpp"
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/WindowProcessingTasks.hpp>
#include <Execution/Operators/Streaming/MultiOriginWatermarkProcessor.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

template<class Slice>
void appendToGlobalSliceStore(void* ss, void* slicePtr) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}
template<class Slice>
void triggerSlidingWindows(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

template<class Slice>
AppendToSliceStoreHandler<Slice>::AppendToSliceStoreHandler(uint64_t windowSize, uint64_t windowSlide) {
    static_assert(std::is_same_v<Slice, NonKeyedSlice> || std::is_same_v<Slice, KeyedSlice>,
                  "AppendToSliceStoreHandler only Supports: KeyedSlice and NonKeyedSlice");
    if constexpr (std::is_same_v<Slice, NonKeyedSlice>) {
        TRACE_OPERATOR_HANDLER(
            "NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<Runtime::Execution::Operators::NonKeyedSlice>",
            "Execution/Operators/Streaming/Aggregations/AppendToSliceStoreHandler.hpp",
            windowSize,
            windowSlide);
    } else if constexpr (std::is_same_v<Slice, KeyedSlice>) {
        TRACE_OPERATOR_HANDLER(
            "NES::Runtime::Execution::Operators::AppendToSliceStoreHandler<Runtime::Execution::Operators::KeyedSlice>",
            "Execution/Operators/Streaming/Aggregations/AppendToSliceStoreHandler.hpp",
            windowSize,
            windowSlide);
    }

    std::vector<OriginId> ids = {0};
}

template<class Slice>
void AppendToSliceStoreHandler<Slice>::appendToGlobalSliceStore(std::unique_ptr<Slice> slice) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

template<class Slice>
void AppendToSliceStoreHandler<Slice>::triggerSlidingWindows(Runtime::WorkerContext& wctx,
                                                             Runtime::Execution::PipelineExecutionContext& ctx,
                                                             uint64_t sequenceNumber,
                                                             uint64_t slideEnd) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
};

template<class Slice>
void AppendToSliceStoreHandler<Slice>::stop(NES::Runtime::QueryTerminationType queryTerminationType,
                                            NES::Runtime::Execution::PipelineExecutionContextPtr ctx) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

// Instantiate types
template class AppendToSliceStoreHandler<NonKeyedSlice>;
template class AppendToSliceStoreHandler<KeyedSlice>;
}// namespace NES::Runtime::Execution::Operators