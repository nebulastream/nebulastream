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
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreAction.hpp>
#include <Execution/Operators/Streaming/Aggregations/AppendToSliceStoreHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/KeyedTimeWindow/KeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/LocalBufferPool.hpp>
#include <Runtime/WorkerContext.hpp>

namespace NES::Runtime::Execution::Operators {

template<class Slice>
void appendToGlobalSliceStore(void* ss, void* slicePtr) {
    auto handler = static_cast<AppendToSliceStoreHandler<Slice>*>(ss);
    auto slice = std::unique_ptr<Slice>((Slice*) slicePtr);
    handler->appendToGlobalSliceStore(std::move(slice));
}
template<class Slice>
void triggerSlidingWindows(void* sh, void* wctx, void* pctx, uint64_t sequenceNumber, uint64_t sliceEnd) {
    auto handler = static_cast<AppendToSliceStoreHandler<Slice>*>(sh);
    auto workerContext = static_cast<WorkerContext*>(wctx);
    auto pipelineExecutionContext = static_cast<PipelineExecutionContext*>(pctx);
    handler->triggerSlidingWindows(*workerContext, *pipelineExecutionContext, sequenceNumber, sliceEnd);
}

template<class Slice>
AppendToSliceStoreAction<Slice>::AppendToSliceStoreAction(const uint64_t operatorHandlerIndex)
    : operatorHandlerIndex(operatorHandlerIndex) {}

template<class Slice>
void AppendToSliceStoreAction<Slice>::emitSlice(ExecutionContext& ctx,
                                                ExecuteOperatorPtr&,
                                                Value<UInt64>&,
                                                Value<UInt64>& sliceEnd,
                                                Value<UInt64>& sequenceNumber,
                                                Value<MemRef>& combinedSlice) const {

    auto actionHandler = ctx.getGlobalOperatorHandler(operatorHandlerIndex);
    if constexpr (std::same_as<Slice, NonKeyedSlice>) {
        FunctionCall("appendToGlobalSliceStoreNonKeyed", appendToGlobalSliceStore<Slice>, actionHandler, combinedSlice);
        FunctionCall("triggerSlidingWindowsNonKeyed",
                     triggerSlidingWindows<Slice>,
                     actionHandler,
                     ctx.getWorkerContext(),
                     ctx.getPipelineContext(),
                     sequenceNumber,
                     sliceEnd);
    } else if constexpr (std::same_as<Slice, KeyedSlice>) {
        FunctionCall("appendToGlobalSliceStoreKeyed", appendToGlobalSliceStore<Slice>, actionHandler, combinedSlice);
        FunctionCall("triggerSlidingWindowsKeyed",
                     triggerSlidingWindows<Slice>,
                     actionHandler,
                     ctx.getWorkerContext(),
                     ctx.getPipelineContext(),
                     sequenceNumber,
                     sliceEnd);
    }
}

// Instantiate types
template class AppendToSliceStoreAction<NonKeyedSlice>;
template class AppendToSliceStoreAction<KeyedSlice>;
}// namespace NES::Runtime::Execution::Operators