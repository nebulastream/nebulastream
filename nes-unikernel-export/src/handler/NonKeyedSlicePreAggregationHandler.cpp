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
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlice.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedSlicePreAggregationHandler.hpp>
#include <Execution/Operators/Streaming/Aggregations/NonKeyedTimeWindow/NonKeyedThreadLocalSliceStore.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>

namespace NES::Runtime::Execution::Operators {

NonKeyedSlice::~NonKeyedSlice() = default;
NonKeyedSlicePreAggregationHandler::NonKeyedSlicePreAggregationHandler(uint64_t windowSize,
                                                                       uint64_t windowSlide,
                                                                       const std::vector<OriginId>& origins)
    : AbstractSlicePreAggregationHandler<NonKeyedSlice, NonKeyedThreadLocalSliceStore>(windowSize, windowSlide, origins) {
    TRACE_OPERATOR_HANDLER("NES::Runtime::Execution::Operators::NonKeyedSlicePreAggregationHandler",
                           "handler/NonKeyedSlicePreAggregationHandler.hpp",
                           windowSize,
                           windowSlide,
                           origins);
}

void NonKeyedSlicePreAggregationHandler::setup(Runtime::Execution::PipelineExecutionContext& ctx, uint64_t entrySize) {
    NES_THROW_RUNTIME_ERROR("Not Implemented");
}

NonKeyedSlicePreAggregationHandler::~NonKeyedSlicePreAggregationHandler() { NES_DEBUG("~NonKeyedSlicePreAggregationHandler"); }

const State* NonKeyedSlicePreAggregationHandler::getDefaultState() const { NES_THROW_RUNTIME_ERROR("Not Implemented"); }

}// namespace NES::Runtime::Execution::Operators